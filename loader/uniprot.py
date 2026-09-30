#!/usr/bin/env python3
#
# web.uniprot_mappings and web.hupo_psi_id: which UniProt entry each protein
# entity in the archive corresponds to.  Read by the website's summary pages
# and by BMRB-API's /mappings and /protein/uniprot endpoints.
#
# Ported from BMRB-API's reloaders/uniprot, for the same reason webapi.sql and
# the timedomain scan were: the reload swaps `web` in wholesale, and anything
# not built into web_new before the swap is dropped with the old schema.  The
# API reloader wrote `web.` and `macromolecules.` by name, so it could only run
# against the live schemas -- and it had not run in years anyway (its lookups
# had died; see below).  The first single-server release dropped both objects
# and the summary pages failed with `relation "web.uniprot_mappings" does not
# exist`.  hupo_psi_id in particular could not live anywhere else: it is a
# materialized view over macromolecules tables, and whatever is built on the
# old tables goes when the swap drops them.
#
# Three kinds of row, by link_type:
#
#   author  the depositor's own UniProt reference (Entity_db_link,
#           Author_supplied = yes)
#   pdb     no author reference, so the entity's PDB chain is looked up in
#           SIFTS
#   blast   BMRB's own sequence-search annotations (Author_supplied = no,
#           Database_code = SP), taken as they are
#
# The author and pdb IDs are checked against UniProt and replaced by the
# entry's primary accession.
#
# What changed from the API version, and why:
#
#   PDB -> UniProt   RCSB's describeMol REST API, retired in 2020; every lookup
#                    since then quietly found nothing.  Now SIFTS'
#                    pdb_chain_uniprot.tsv.gz: one download per release
#                    instead of a request per PDB ID, and current each week.
#   entry name       The legacy uniprot.org query API, retired in 2022; its
#                    reply crashed the reloader outright.  Now rest.uniprot.org.
#   validation       Kept the *last* <accession> in the entry, a secondary
#                    one: P0CG48 came back as Q9UPK7.  Now the primary.
#   caching          The API cached every answer in CSV files forever, which
#                    is how the wrong accessions above survived.  Nothing is
#                    cached across runs now; the distinct IDs are checked
#                    fresh each release, a few at a time.
#
# If SIFTS or UniProt cannot be reached, the previous release's mappings are
# carried forward rather than failing the whole macromolecule load, since the
# rescue DAG would rerun all of it.  That is loud on stderr, and it is not a
# fallback for anything else: a bug still fails the stage.
#

import concurrent.futures
import gzip
import os
import re
import sys
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
from loader import db
from loader import shadow

SIFTS_URL = "https://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/tsv/pdb_chain_uniprot.tsv.gz"
UNIPROT = "https://rest.uniprot.org/uniprotkb"

# UniProtKB accession format, from https://www.uniprot.org/help/accession_numbers
ACCESSION = re.compile(r"[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}")

# concurrent UniProt requests; well inside its rate limit, and 429s are retried
WORKERS = 8

_local = threading.local()


def _get(url, **kwargs):
    """GET with retries on rate limiting and 5xx.  One session per thread."""

    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.mount("https://", HTTPAdapter(max_retries=Retry(
            total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",))))
    return _local.session.get(url, timeout=60, **kwargs)


#######################################
# lookups


def sifts_chains(pdb_ids, verbose=False):
    """{(PDB ID, author chain): accession} for the given PDB IDs, plus
    {(PDB ID, None): accession} where the entry has only one accession -- the
    answer when the chain is not known.  (describeMol's rule was "one
    polymer"; SIFTS lists only the chains that map to UniProt, so this is "one
    protein" -- which also covers a single protein bound to DNA or RNA, where
    the old rule gave nothing.)"""

    want = set(p.lower() for p in pdb_ids)
    # Read whole (6 MB) rather than streamed from response.raw: a dropped
    # connection then surfaces as a requests exception, which is what sends
    # load() to its fallback, instead of a urllib3 one that would not.
    response = _get(SIFTS_URL)
    response.raise_for_status()
    try:
        text = gzip.decompress(response.content).decode()
    except (OSError, EOFError) as e:
        raise requests.RequestException("SIFTS download is not a complete gzip file: %s" % (e,))

    chains = {}
    per_entry = {}
    for line in text.splitlines():
        if line.startswith(("#", "PDB\t")):
            continue
        (pdb, chain, accession) = line.split("\t", 3)[:3]
        if pdb not in want:
            continue
        # a chimeric chain maps to more than one accession; keep the first,
        # as describeMol's first accession was kept
        chains.setdefault((pdb.upper(), chain), accession)
        per_entry.setdefault(pdb.upper(), set()).add(accession)

    for (pdb, accessions) in per_entry.items():
        if len(accessions) == 1:
            chains[(pdb, None)] = next(iter(accessions))
    if verbose:
        sys.stdout.write("uniprot: SIFTS maps %d chains of %d PDB IDs asked about\n"
                         % (len(chains), len(want),))
    return chains


def accession_for_name(name):
    """The accession for a UniProt entry name (P4R3A_HUMAN -> Q6IN85), or None."""

    # active:true because inactive entries keep the name they had: id:B2MG_HUMAN
    # alone also matches the retired P01884, and which came first varied.
    response = _get(UNIPROT + "/search", params={
        "query": "id:%s AND active:true" % (name,), "fields": "accession",
        "format": "tsv", "size": 1})
    response.raise_for_status()
    lines = response.text.splitlines()
    if len(lines) > 1:
        return lines[1].strip()

    # Unreviewed entry names are <accession>_<species>, and UniProt has renamed
    # some species codes since depositions quoted them (A5VHK8_LACRD is now
    # A5VHK8_LIMRD).  The accession is still in there; validation checks it.
    prefix = name.split("_")[0]
    return prefix if ACCESSION.fullmatch(prefix) else None


def primary_accession(accession):
    """(primary accession or None, why not) for an accession as deposited."""

    response = _get("%s/%s.json" % (UNIPROT, accession,), params={"fields": "accession"})
    if response.status_code in (400, 404):
        # 400 is UniProt's answer to a string that is not accession-shaped
        return (None, "not found")
    response.raise_for_status()
    entry = response.json()

    if entry.get("entryType") != "Inactive":
        return (entry.get("primaryAccession"), None)

    # Merged into one entry: follow it.  Demerged into several, or deleted:
    # there is no single right answer.
    reason = entry.get("inactiveReason", {})
    targets = reason.get("mergeDemergeTo") or []
    if reason.get("inactiveReasonType") == "MERGED" and len(targets) == 1:
        return (targets[0], None)
    return (None, "inactive, %s" % (str(reason.get("inactiveReasonType", "")).lower(),))


def _resolve_names(value):
    """An author reference that may be an entry name, or several separated by
    spaces; the first that resolves wins, as in the API reloader."""

    value = value.upper()
    if "_" not in value:
        return value
    for item in value.split(" "):
        if "_" not in item:
            return item
        found = accession_for_name(item)
        if found:
            return found
    return None


def _parallel(func, items):
    """{item: func(item)} for each distinct item, WORKERS at a time."""

    items = sorted(set(items))
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        return dict(zip(items, pool.map(func, items)))


#######################################
# the load


# Protein entities with their author UniProt reference if there is one, and
# their PDB ID and chain if not.  Schema names are filled in for the shadow.
_ENTITIES = """
SELECT ent."Entry_ID",
       ent."ID",
       upper(coalesce(ea."PDB_chain_ID", ent."Polymer_strand_ID")),
       upper(pdb_id),
       CASE WHEN dbl."Accession_code" IS NOT NULL THEN 'author' END,
       dbl."Accession_code",
       "Polymer_seq_one_letter_code",
       ent."Details"
FROM {m}."Entity" AS ent
         LEFT JOIN {w}.pdb_link AS pdb ON pdb.bmrb_id = ent."Entry_ID"
         LEFT JOIN {m}."Entity_db_link" AS dbl
                   ON dbl."Entry_ID" = ent."Entry_ID" AND ent."ID" = dbl."Entity_ID"
         LEFT JOIN {m}."Entity_assembly" AS ea
                   ON ea."Entry_ID" = ent."Entry_ID" AND ea."Entity_ID" = ent."ID"
WHERE "Polymer_seq_one_letter_code" IS NOT NULL
  AND "Polymer_seq_one_letter_code" != ''
  AND ent."Polymer_type" = 'polypeptide(L)'
  AND (dbl."Accession_code" IS NULL
    OR (dbl."Author_supplied" = 'yes' AND
        lower(dbl."Database_code") IN ('unp', 'uniprot', 'sp')))
ORDER BY ent."Entry_ID"::int, ent."ID"::int
"""

_COLUMNS = "bmrb_id, entity_id, pdb_chain, pdb_id, link_type, uniprot_id, protein_sequence, details"

_CREATE = """
DROP TABLE IF EXISTS {w}.uniprot_mappings CASCADE;
CREATE TABLE {w}.uniprot_mappings
(
    id               serial primary key,
    bmrb_id          text,
    entity_id        int,
    pdb_chain        text,
    pdb_id           text,
    link_type        text,
    uniprot_id       text,
    protein_sequence text,
    details          text
);
"""

# BMRB's sequence-search annotations, as they are, then the clean-up the API
# reloader did -- including its two hand-fixed deposition typos.
_BLAST_AND_CLEAN = """
INSERT INTO {w}.uniprot_mappings ({cols})
    (SELECT dbl."Entry_ID",
            dbl."Entity_ID"::int,
            null,
            pdb_id,
            'blast',
            REPLACE(dbl."Accession_code", '.', '-'),
            ent."Polymer_seq_one_letter_code",
            ent."Details"
     FROM {m}."Entity_db_link" AS dbl
              LEFT JOIN {m}."Entity" AS ent
                        ON dbl."Entry_ID" = ent."Entry_ID" AND ent."ID" = dbl."Entity_ID"
              LEFT JOIN {m}."Entity_assembly" AS ea
                        ON ea."Entry_ID" = dbl."Entry_ID" AND ea."Entity_ID" = dbl."Entity_ID"
              LEFT JOIN {w}.pdb_link AS pdb ON pdb.bmrb_id = ent."Entry_ID"
     WHERE dbl."Author_supplied" = 'no'
       AND dbl."Database_code" = 'SP');

DELETE FROM {w}.uniprot_mappings WHERE uniprot_id = '' OR uniprot_id IS NULL;
UPDATE {w}.uniprot_mappings SET uniprot_id = REPLACE(uniprot_id, '.', '-') WHERE uniprot_id LIKE '%.%';
UPDATE {w}.uniprot_mappings SET uniprot_id = 'P9WKD3' WHERE uniprot_id = 'P9WKD3[43 - 307]';
UPDATE {w}.uniprot_mappings SET uniprot_id = 'P05386' WHERE uniprot_id = 'P05386,P05387';
DELETE FROM {w}.uniprot_mappings WHERE uniprot_id = 'TmpAcc';
"""

_DEDUPE_AND_VIEW = """
DELETE FROM {w}.uniprot_mappings
WHERE id IN (SELECT id
             FROM (SELECT id,
                          ROW_NUMBER() OVER (PARTITION BY bmrb_id, pdb_id, entity_id, link_type, uniprot_id
                                             ORDER BY id) AS rnum
                   FROM {w}.uniprot_mappings) t
             WHERE t.rnum > 1);
CREATE UNIQUE INDEX ON {w}.uniprot_mappings (bmrb_id, pdb_id, entity_id, link_type, uniprot_id);

CREATE MATERIALIZED VIEW {w}.hupo_psi_id AS
(
SELECT uni.id,
       uni.uniprot_id,
       'bmrb:' || bmrb_id                   AS source,
       entity."Polymer_seq_one_letter_code" AS "regionSequenceExperimental",
       'ECO:0001238'                        AS "experimentType",
       CASE
           WHEN cit."PubMed_ID" IS NOT NULL THEN 'pubmed:' || cit."PubMed_ID"
           WHEN cit."DOI" IS NOT NULL THEN 'doi:' || cit."DOI"
           END                              AS "experimentReference",
       (SELECT "Date"
        FROM {m}."Release"
        WHERE "Entry_ID" = entity."Entry_ID"
        ORDER BY "Release_number"::int DESC
        LIMIT 1)                            AS "lastModified",
       uni.link_type                        AS "regionDefinitionSource"
FROM {w}.uniprot_mappings AS uni
         LEFT JOIN {m}."Entity" AS entity
                   ON entity."Entry_ID" = bmrb_id AND entity."ID"::int = entity_id
         LEFT JOIN {m}."Entry" AS entry ON entry."ID" = bmrb_id
         LEFT JOIN {m}."Citation" AS cit ON cit."Entry_ID" = entry."ID"
    AND cit."Class" = 'entry citation'
ORDER BY uniprot_id);
"""


# Deposited values the API reloader patched by hand, in SQL (see
# _BLAST_AND_CLEAN).  That ran after validation, so on an author row the
# unpatched value had already been rejected and the fix only ever reached
# blast rows; applied here as well, it reaches both.
_HAND_FIXES = {"P9WKD3[43 - 307]": "P9WKD3", "P05386,P05387": "P05386"}


def _mapped(rows, verbose=False):
    """The author/pdb rows with their UniProt IDs looked up and validated.
    Raises requests.RequestException if a service cannot be reached."""

    # PDB chains, for the entities with no author reference
    need_pdb = [r for r in rows if not r[5] and r[3]]
    chains = sifts_chains({r[3] for r in need_pdb}, verbose=verbose) if need_pdb else {}
    for r in need_pdb:
        (full_chain, pdb) = (r[2] or "", r[3])
        # the full author chain ID first; then, as the API reloader always did,
        # its first character; then the entry as a whole
        found = chains.get((pdb, full_chain)) or chains.get((pdb, full_chain[:1] or None)) \
            or chains.get((pdb, None))
        if found:
            r[4] = "pdb"
            r[5] = found

    for r in rows:
        if r[5]:
            r[5] = r[5].strip().replace(".", "-")
            r[5] = _HAND_FIXES.get(r[5], r[5])

    # entry names (FOO_HUMAN) to accessions
    names = _parallel(_resolve_names, [r[5] for r in rows if r[5] and "_" in r[5]])
    for r in rows:
        if r[5] and r[5] in names:
            r[5] = names[r[5]]

    # every ID to its entry's primary accession
    checked = _parallel(primary_accession, [r[5] for r in rows if r[5]])
    rejected = {}
    for r in rows:
        if r[5]:
            (primary, why) = checked[r[5]]
            if primary is None:
                rejected.setdefault(why, set()).add(r[5])
            r[5] = primary

    for (why, ids) in sorted(rejected.items()):
        sys.stderr.write("uniprot: %d IDs dropped (%s)%s\n"
                         % (len(ids), why, (": " + " ".join(sorted(ids))) if verbose else "",))
    return rows


def load(config, verbose=False):
    """Build uniprot_mappings and hupo_psi_id in the web shadow schema, from
    the macromolecule shadow schema.  Needs web.pdb_link loaded first."""

    if verbose:
        sys.stdout.write("uniprot.load()\n")

    names = {"w": shadow.target(config, "web"), "m": shadow.target(config, "macromolecules"),
             "cols": _COLUMNS}
    live = db.quote(config.get("web", "schema"))

    with db.connection(db.dsn(config, "web")) as conn:
        with conn.cursor() as curs:
            curs.execute(_ENTITIES.format(**names))
            rows = [list(r) for r in curs.fetchall()]
            curs.execute(_CREATE.format(**names))

            try:
                rows = _mapped(rows, verbose=verbose)
            except requests.RequestException as e:
                # an outage, not a bug: keep last release's mappings
                curs.execute("select to_regclass(%s)", ("%s.uniprot_mappings" % (live,),))
                if curs.fetchone()[0] is None:
                    raise
                sys.stderr.write(
                    "*** uniprot: SIFTS or UniProt could not be reached (%s)\n"
                    "    carrying forward the previous release's uniprot_mappings\n" % (e,))
                curs.execute("insert into {w}.uniprot_mappings ({cols})"
                             " select {cols} from {live}.uniprot_mappings order by id"
                             .format(live=live, **names))
            else:
                sql = "insert into {w}.uniprot_mappings ({cols})" \
                      " values (%s, %s, %s, %s, %s, %s, %s, %s)".format(**names)
                curs.executemany(sql, rows)
                curs.execute(_BLAST_AND_CLEAN.format(**names))

            curs.execute(_DEDUPE_AND_VIEW.format(**names))
            curs.execute("select link_type, count(*) from {w}.uniprot_mappings"
                         " group by link_type order by 1".format(**names))
            counts = curs.fetchall()
        conn.commit()

    sys.stdout.write("uniprot: %s\n" % (", ".join("%s %d" % (t, n) for (t, n) in counts) or "no mappings",))
