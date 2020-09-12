--
-- "web" schema: auxiliary tables supporting various website scripts
--

drop schema if exists web cascade;
create schema web;
grant usage on schema web to public;
grant select on all tables in schema web to public;
alter default privileges in schema web grant select on tables to public;

set search_path = web, pg_catalog;

create table dep2accno (
    depno text not null,
    accno text not null
);

create table procque (
    accno text not null,
    received date not null,
    onhold text not null,
    status text,
    released date
);

create sequence pulsefilelist_id_seq;

create table pulsefilelist (
    id integer not null default nextval('pulsefilelist_id_seq'),
    accnum integer,
    mfg text,
    exptype text,
    dimension text,
    file text,
    name text
);

alter sequence pulsefilelist_id_seq owned by pulsefilelist.id;

create table termlist (
    term text,
    expansion text,
    description text,
    isprimary text
);

-- related_entries: database_name, database_accession_code, relationship
--
-- assembly_db_link: author_supplied, database_code, accession_code,
--    entry_mol_code, entry_mol_name, entry_experimental_method, entry_structure_resolution,
--    entry_relationship_type, entry_details
--
-- entity_db_link: author_supplied, database_code, accession_code,
--    entry_mol_code, entry_mol_name, entry_experimental_method, entry_structure_resolution,
--    entry_relationship_type, entry_details,
--    [...BLAST fields...]
--
-- chem_comp_db_link: author_supplied, database_code, accession_code, accession_code_type,
--    entry_mol_code, entry_mol_name,
--    entry_relationship_type, entry_details
--
-- pdb_link: bmrb_id, pdb_id
--
-- uniprot_mappings: id, bmrb_id, entity_id, pdb_chain_id, pdb_id,
--     link_type: (author, blast, pdb),
--     uniprot_id, protein_sequence,details
--
-- web.related_entries (?)
--    bmrb_id
--    entity_id
--    link_type
--    db_name
--    db_id
--    db_subid
--    protein_sequence
--    exptl_method
--    quality_factor (?)
--    details
--
-- for now however just add pdb_link
--
create table pdb_link (
    bmrb_id text not null,
    pdb_id text not null
);
