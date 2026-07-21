# Data remediation needed in the entry archive

Defects in the deposited NMR-STAR files — not in dbloader — found while
replacing `starobj`/`sas` with pynmrstar (see [`PORT_NOTES.md`](PORT_NOTES.md)).
The loader currently loads them as deposited; once the source files are fixed,
nothing in the loader needs to change.

---

## 1. `$` framecode in `_Entity_assembly.Entity_assembly_name` — 203 entries

### What is wrong

`_Entity_assembly.Entity_assembly_name` is a free-text name
(`VARCHAR(127)`, `sfpointerflg = 'N'` in the dictionary). In 203 entries it
instead holds a **saveframe pointer** — the same `$framecode` as the adjacent
`_Entity_assembly.Entity_label`, which *is* a pointer (`sfpointerflg = 'Y'`).

`bmse010013.str`, lines 150–167 — the two marked columns are the same value:

```
    loop_
       _Entity_assembly.ID
       _Entity_assembly.Entity_assembly_name        <-- free text, should not have $
       _Entity_assembly.Entity_ID
       _Entity_assembly.Entity_label                <-- pointer, correctly has $
       ...

      1   $lignin_cw_compound_22   1   $lignin_cw_compound_22   yes   native  ...
          ^^^^^^^^^^^^^^^^^^^^^^^      ^^^^^^^^^^^^^^^^^^^^^^^
```

### The fix

In each affected file, in the `assembly` saveframe's `_Entity_assembly` loop,
on the row with `_Entity_assembly.ID = 1`: **remove the leading `$` from the
`_Entity_assembly.Entity_assembly_name` value.** Leave `_Entity_assembly.Entity_label`
alone — the `$` belongs there.

```
-      1   $lignin_cw_compound_22   1   $lignin_cw_compound_22   yes   native  ...
+      1   lignin_cw_compound_22    1   $lignin_cw_compound_22   yes   native  ...
```

Whether the name should be the bare framecode (`lignin_cw_compound_22`) or a
real human-readable compound name is a curation question this note does not
answer; stripping the `$` is the minimum that makes the field type-correct.

### Why it matters now

`sas` stripped `$` **lexically** — from any bare `$token`, whatever tag it was
in — so the old loader silently turned these into `lignin_cw_compound_22` and
the defect never reached the database. The rewrite strips `$` only from tags
the dictionary marks as pointers, so these now load **with the `$` intact**, as
deposited. That is deliberate: the loader stopped hiding a data error. It also
means `macromolecules`/`metabolomics`.`Entity_assembly`.`Entity_assembly_name`
will contain 203 values starting with `$` until the source files are fixed.

### Scope

- **203 rows in 203 entries** — one row each, all `Entity_assembly.ID = 1`.
- **All metabolomics**, none in the macromolecule archive.
- All in the `assembly` saveframe.
- In **all 203** the value equals the row's own `Entity_label`.
- Almost all are the `lignin_cw_compound_*` series.
- Verified over the complete archives (14,772 macromolecule + 3,629
  metabolomics entries) by `tests/pointer_tags.py`.

### How to re-check after fixing

```sh
venv/bin/python tests/pointer_tags.py
```

Expected once remediated: *121 tags carry $-values, 121 flagged
sfpointerflg='Y', 0 not*. Today it reports 120 flagged and 1 not.

Or, against a loaded database:

```sql
select count(*) from metabolomics."Entity_assembly"
 where "Entity_assembly_name" like '$%';     -- 203 today, 0 when fixed
```

## Affected entries

All 203 are metabolomics `bmse` entries, in the range bmse010013–bmse010457.
In every one the row is `Entity_assembly.ID = 1` of the `assembly` saveframe,
and the value is character-for-character the row's own `Entity_label`.

```
bmse010013  bmse010017  bmse010018  bmse010020  bmse010021  bmse010034
bmse010035  bmse010036  bmse010037  bmse010038  bmse010039  bmse010040
bmse010056  bmse010062  bmse010063  bmse010067  bmse010068  bmse010069
bmse010070  bmse010072  bmse010078  bmse010079  bmse010086  bmse010092
bmse010118  bmse010119  bmse010120  bmse010122  bmse010123  bmse010129
bmse010131  bmse010132  bmse010137  bmse010139  bmse010140  bmse010141
bmse010144  bmse010145  bmse010163  bmse010164  bmse010166  bmse010167
bmse010168  bmse010169  bmse010170  bmse010172  bmse010174  bmse010175
bmse010177  bmse010184  bmse010185  bmse010187  bmse010188  bmse010189
bmse010191  bmse010192  bmse010193  bmse010194  bmse010195  bmse010196
bmse010197  bmse010198  bmse010199  bmse010200  bmse010201  bmse010202
bmse010213  bmse010214  bmse010215  bmse010216  bmse010234  bmse010235
bmse010236  bmse010237  bmse010238  bmse010239  bmse010240  bmse010241
bmse010242  bmse010243  bmse010244  bmse010245  bmse010246  bmse010247
bmse010249  bmse010250  bmse010252  bmse010253  bmse010258  bmse010259
bmse010260  bmse010261  bmse010262  bmse010263  bmse010264  bmse010265
bmse010266  bmse010267  bmse010268  bmse010269  bmse010270  bmse010271
bmse010272  bmse010273  bmse010274  bmse010275  bmse010276  bmse010277
bmse010278  bmse010279  bmse010280  bmse010281  bmse010282  bmse010283
bmse010284  bmse010288  bmse010289  bmse010290  bmse010292  bmse010293
bmse010294  bmse010295  bmse010296  bmse010298  bmse010299  bmse010300
bmse010301  bmse010302  bmse010303  bmse010304  bmse010305  bmse010306
bmse010307  bmse010308  bmse010309  bmse010310  bmse010311  bmse010312
bmse010313  bmse010314  bmse010315  bmse010316  bmse010317  bmse010319
bmse010320  bmse010321  bmse010323  bmse010324  bmse010325  bmse010326
bmse010327  bmse010328  bmse010329  bmse010330  bmse010331  bmse010332
bmse010333  bmse010334  bmse010335  bmse010336  bmse010337  bmse010338
bmse010339  bmse010340  bmse010341  bmse010342  bmse010343  bmse010344
bmse010345  bmse010346  bmse010347  bmse010348  bmse010349  bmse010350
bmse010351  bmse010352  bmse010353  bmse010354  bmse010355  bmse010356
bmse010357  bmse010358  bmse010359  bmse010360  bmse010361  bmse010362
bmse010363  bmse010364  bmse010365  bmse010366  bmse010367  bmse010369
bmse010370  bmse010371  bmse010372  bmse010373  bmse010374  bmse010375
bmse010381  bmse010382  bmse010383  bmse010384  bmse010457
```
