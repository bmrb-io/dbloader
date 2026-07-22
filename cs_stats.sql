-- Build-time tuning: the aggregate scans below are parallel seq scans of
-- Atom_chem_shift, and PostgreSQL's default cap of 2 workers per gather
-- leaves cores idle on the build host (which serves nothing during the
-- reload).  Raising it to 6 roughly halves each scan; whole-script effect at
-- archive scale was 441 s -> 418 s.  A host with fewer workers clamps this
-- down safely, so it is a plain floor, not a requirement.
set max_parallel_workers_per_gather = 6;

-- To generate data files:
--
-- run this file in psql
--
--
-- entries with aromatic and/or paramagnetic ligand
--
create temporary table cs_stat_exclude_all (id text);

insert into cs_stat_exclude_all select distinct c."Entry_ID" from macromolecules."Chem_comp" c 
    join macromolecules."Chem_comp_bond" b on b."Comp_ID"=c."ID" and b."Entry_ID"=c."Entry_ID" 
    where (c."Aromatic"='yes' or b."Aromatic_flag"='yes' or b."Value_order"='AROM')
    union select distinct "Entry_ID" from macromolecules."Entity" where "Paramagnetic"='yes';

--
-- the easy one
--
-- RNA full set
--
drop table if exists web.cs_stat_rna_full;
select comp_id,atom_id,count(val) as count,min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std
  into table web.cs_stat_rna_full
  from
    (select "Comp_ID" as comp_id,"Atom_ID" as atom_id,cast("Val" as numeric) as val from
    macromolecules."Atom_chem_shift" where "Comp_ID" in ('A','C','G','U')) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

-- add outlier count

alter table web.cs_stat_rna_full
  add column num_outliers integer;

-- Counting outliers used to be a correlated subquery evaluated once per
-- statistics row -- a lookup into Atom_chem_shift for every (comp_id,
-- atom_id), in six statements of this shape.  Grouping instead reads the
-- table once per statement: 475 s -> 441 s at archive scale.
--
-- The join is LEFT so a statistics row with no outliers still produces a
-- group, and the count is over a join column rather than `count(*)` so that
-- the unmatched row counts 0 -- which is what the old `count(*)` over an
-- empty subquery returned.  That also covers avg/std being NULL (a
-- single-observation group): the comparisons are then NULL, nothing matches,
-- and the answer is 0 either way.

update web.cs_stat_rna_full t set num_outliers = o.n
  from (select s.comp_id, s.atom_id, count(a."Comp_ID") as n
          from web.cs_stat_rna_full s
          left join macromolecules."Atom_chem_shift" a
            on a."Comp_ID" = s.comp_id
           and a."Atom_ID" = s.atom_id
           and (cast(a."Val" as float) > s.avg + 3 * s.std
             or cast(a."Val" as float) < s.avg - 3 * s.std)
         group by s.comp_id, s.atom_id) o
 where t.comp_id = o.comp_id and t.atom_id = o.atom_id;

--
-- RNA exclusion list
--
create temporary table cs_stat_exclude_rna (id text);

-- The +/-8 sigma band used to be fetched by two correlated scalar subqueries
-- per row of Atom_chem_shift -- two subplan executions for every shift in the
-- archive, against a table of a few hundred rows.  As a join the planner
-- hashes that table once.  Measured at archive scale, the three inserts of
-- this shape were most of the script: 747 s -> 475 s.
--
-- The join is inner, matching the old semantics: a (comp_id, atom_id) with no
-- statistics row gave NULL bounds, `not (x between NULL and NULL)` is NULL,
-- and the row was not excluded -- which is what dropping it does.  The
-- statistics are grouped by (comp_id, atom_id), so there is exactly one match
-- and the join cannot duplicate a row.

insert into cs_stat_exclude_rna select distinct a."Entry_ID" from macromolecules."Atom_chem_shift" a
  join web.cs_stat_rna_full s on s.comp_id=a."Comp_ID" and s.atom_id=a."Atom_ID"
  where a."Comp_ID" in ('A','C','G','U')
  and not (cast(a."Val" as numeric) between s.avg - 8 * s.std and s.avg + 8 * s.std);

--
-- RNA restricted set
--
drop table if exists web.cs_stat_rna_filt;
select comp_id,atom_id,count(val) as count,min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std 
  into table web.cs_stat_rna_filt 
  from 
    (select "Comp_ID" as comp_id,"Atom_ID" as atom_id,cast("Val" as numeric) as val from 
    macromolecules."Atom_chem_shift" where "Comp_ID" in ('A','C','G','U') and "Entry_ID" not in 
    (select distinct id from cs_stat_exclude_all union select distinct id from cs_stat_exclude_rna)) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

-- add outlier count

alter table web.cs_stat_rna_filt
  add column num_outliers integer;

-- NB, unchanged from the correlated form and preserved deliberately: there is
-- no exclusion-list filter here, so the outlier count on a _filt table is
-- taken over every entry, not over the filtered set the rest of the row
-- describes.

update web.cs_stat_rna_filt t set num_outliers = o.n
  from (select s.comp_id, s.atom_id, count(a."Comp_ID") as n
          from web.cs_stat_rna_filt s
          left join macromolecules."Atom_chem_shift" a
            on a."Comp_ID" = s.comp_id
           and a."Atom_ID" = s.atom_id
           and (cast(a."Val" as float) > s.avg + 3 * s.std
             or cast(a."Val" as float) < s.avg - 3 * s.std)
         group by s.comp_id, s.atom_id) o
 where t.comp_id = o.comp_id and t.atom_id = o.atom_id;

--
--
-- DNA full set
--
select comp_id,atom_id,count(val) as count,min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std
  into temporary table cs_stat_dna_full_raw 
  from
    (select "Comp_ID" as comp_id,"Atom_ID" as atom_id,cast("Val" as numeric) as val from
    macromolecules."Atom_chem_shift" where "Comp_ID" in ('DA','DC','DG','DT')) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

--
-- DNA exclusion list
--
create temporary table cs_stat_exclude_dna (id text);

insert into cs_stat_exclude_dna select distinct a."Entry_ID" from macromolecules."Atom_chem_shift" a
  join cs_stat_dna_full_raw s on s.comp_id=a."Comp_ID" and s.atom_id=a."Atom_ID"
  where a."Comp_ID" in ('DA','DC','DG','DT')
  and not (cast(a."Val" as numeric) between s.avg - 8 * s.std and s.avg + 8 * s.std);

--
-- DNA restricted set for validator
--
select distinct comp_id,atom_id,count(val), min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std
  into temporary table cs_stat_dna_filt_raw
  from
    (select "Comp_ID" as comp_id,"Atom_ID" as atom_id,cast("Val" as numeric) as val 
    from macromolecules."Atom_chem_shift" 
    where "Comp_ID" in ('DA','DC','DG','DT') and "Entry_ID" not in
    (select distinct id from cs_stat_exclude_all union select distinct id from cs_stat_exclude_dna)) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

--
-- re-do DNA full set with methyls collapsed 
-- (collapsing methyls after calculating statistics produces multiple rows due to round-off error)
--
drop table if exists web.cs_stat_dna_full;
select distinct comp_id,atom_id,
  case when comp_id='DT' and atom_id='M7' then count(val)/3 else count(val) end as count,
  min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std
  into table web.cs_stat_dna_full
  from
    (select "Comp_ID" as comp_id,
    case when "Comp_ID"='DT' and "Atom_ID" similar to 'H7[123]' then 'M7' else "Atom_ID" end as atom_id,
    cast("Val" as numeric) as val
    from macromolecules."Atom_chem_shift" 
    where "Comp_ID" in ('DA','DC','DG','DT')) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

-- add outlier count

alter table web.cs_stat_dna_full
  add column num_outliers integer;

update web.cs_stat_dna_full t set num_outliers = o.n
  from (select s.comp_id, s.atom_id, count(a."Comp_ID") as n
          from web.cs_stat_dna_full s
          left join macromolecules."Atom_chem_shift" a
            on a."Comp_ID" = s.comp_id
           and a."Atom_ID" = (case when s.atom_id='M7' then 'H71' else s.atom_id end)
           and (cast(a."Val" as float) > s.avg + 3 * s.std
             or cast(a."Val" as float) < s.avg - 3 * s.std)
         group by s.comp_id, s.atom_id) o
 where t.comp_id = o.comp_id and t.atom_id = o.atom_id;

--
-- DNA restricted set with methyls collapsed
--
drop table if exists web.cs_stat_dna_filt;
select distinct comp_id,atom_id,case when comp_id='DT' and atom_id='M7' then count(val)/3 else count(val) end as count,
  min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std
  into table web.cs_stat_dna_filt
  from
    (select "Comp_ID" as comp_id,
    case when "Comp_ID"='DT' and "Atom_ID" similar to 'H7[123]' then 'M7' else "Atom_ID" end as atom_id,
    cast("Val" as numeric) as val 
    from macromolecules."Atom_chem_shift" 
    where "Comp_ID" in ('DA','DC','DG','DT') and "Entry_ID" not in
    (select distinct id from cs_stat_exclude_all union select distinct id from cs_stat_exclude_dna)) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

-- add outlier count

alter table web.cs_stat_dna_filt
  add column num_outliers integer;

update web.cs_stat_dna_filt t set num_outliers = o.n
  from (select s.comp_id, s.atom_id, count(a."Comp_ID") as n
          from web.cs_stat_dna_filt s
          left join macromolecules."Atom_chem_shift" a
            on a."Comp_ID" = s.comp_id
           and a."Atom_ID" = (case when s.atom_id='M7' then 'H71' else s.atom_id end)
           and (cast(a."Val" as float) > s.avg + 3 * s.std
             or cast(a."Val" as float) < s.avg - 3 * s.std)
         group by s.comp_id, s.atom_id) o
 where t.comp_id = o.comp_id and t.atom_id = o.atom_id;

--
-- peptide full set
--
select comp_id,atom_id,count(val) as count,min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std 
  into temporary table cs_stat_aa_full_raw 
  from
    (select "Comp_ID" as comp_id,"Atom_ID" as atom_id,cast("Val" as numeric) as val from macromolecules."Atom_chem_shift" 
    where "Comp_ID" in ('ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY','HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL')) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

--
-- peptide exclusion list
--
create temporary table cs_stat_exclude_aa (id text);

insert into cs_stat_exclude_aa select distinct a."Entry_ID" from macromolecules."Atom_chem_shift" a
  join cs_stat_aa_full_raw s on s.comp_id=a."Comp_ID" and s.atom_id=a."Atom_ID"
  where a."Comp_ID" in ('ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY','HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL')
  and not (cast(a."Val" as numeric) between s.avg - 8 * s.std and s.avg + 8 * s.std);

-- The thirteen hard limits below stay thirteen statements on purpose.  OR-ing
-- them into a single scan is the obvious tidy-up and it is slower: measured at
-- archive scale it cost 14 s (761 s against a 747 s baseline).  Each statement
-- as written is a narrow (Comp_ID, Atom_ID) equality that the index built in
-- loader/indexes.py turns into a cheap index scan; the merged form is one
-- sequential scan of the whole table instead.

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift"
  where "Comp_ID" in ('ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY','HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL')
  and "Atom_ID"='H' and not (cast("Val" as numeric) between -2.5 and 22.0);

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='HIS' and "Atom_ID"='HD1' and cast("Val" as numeric) < 2.0;

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='HIS' and "Atom_ID"='HD2' and cast("Val" as numeric) > 12.0;

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='HIS' and "Atom_ID"='HE1' and cast("Val" as numeric) > 15.0;

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='HIS' and "Atom_ID"='HE2' and cast("Val" as numeric) < 2.0;

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID" in ('ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY','HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL') 
  and "Atom_type"='H' and "Atom_ID" not in ('H','HE','HE1','HE2','HD1','HD2','HZ','HH') 
  and not (cast("Val" as numeric) between -2.5 and 10.0);

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='ARG' and "Atom_ID"='NE' and not (cast("Val" as numeric) between 60.0 and 100.0);

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='ARG' and "Atom_ID" similar to 'NH[12]' and not (cast("Val" as numeric) between 50 and 90);

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='PRO' and "Atom_ID"='N' and not (cast("Val" as numeric) between 110 and 150);

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='LYS' and "Atom_ID"='NZ' and not (cast("Val" as numeric) between 12 and 52);

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='HIS' and ("Atom_ID"='ND1' or "Atom_ID"='NE2') and not (cast("Val" as numeric) between 160 and 230);

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='HIS' and "Atom_ID" similar to 'CD[12]' and cast("Val" as numeric) < 90;

insert into cs_stat_exclude_aa select distinct "Entry_ID" from macromolecules."Atom_chem_shift" 
  where "Comp_ID"='TYR' and "Atom_ID"='CZ' and cast("Val" as numeric) < 90;

--
-- peptide restricted set for validator
--
select distinct comp_id,atom_id,count(val),min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std 
  into temporary table cs_stat_aa_filt_raw
  from
    (select "Comp_ID" as comp_id,"Atom_ID" atom_id,cast("Val" as numeric) as val from macromolecules."Atom_chem_shift" 
    where "Comp_ID" in ('ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY','HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL')
    and "Entry_ID" not in (select distinct id from cs_stat_exclude_all union select distinct id from cs_stat_exclude_aa)) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

--
-- re-do peptide full set with H3s collapsed
--
drop table if exists web.cs_stat_aa_full;
select distinct comp_id,atom_id,
  case when comp_id='ALA' and atom_id='MB' then count(val)/3
  when comp_id='VAL' and atom_id='MG1' then count(val)/3
  when comp_id='VAL' and atom_id='MG2' then count(val)/3
  when comp_id='ILE' and atom_id='MG' then count(val)/3
  when comp_id='ILE' and atom_id='MD' then count(val)/3
  when comp_id='LEU' and atom_id='MD1' then count(val)/3
  when comp_id='LEU' and atom_id='MD2' then count(val)/3
  when comp_id='THR' and atom_id='MG' then count(val)/3
  when comp_id='MET' and atom_id='ME' then count(val)/3
  when comp_id='LYS' and atom_id='QZ' then count(val)/3
  else count(val) end as count,
  min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std 
  into table web.cs_stat_aa_full
  from
    (select "Comp_ID" as comp_id,
    case when "Comp_ID"='ALA' and "Atom_ID" similar to 'HB[123]' then 'MB'
    when "Comp_ID"='VAL' and "Atom_ID" similar to 'HG1[123]' then 'MG1'
    when "Comp_ID"='VAL' and "Atom_ID" similar to 'HG2[123]' then 'MG2'
    when "Comp_ID"='ILE' and "Atom_ID" similar to 'HG2[123]' then 'MG'
    when "Comp_ID"='ILE' and "Atom_ID" similar to 'HD1[123]' then 'MD'
    when "Comp_ID"='LEU' and "Atom_ID" similar to 'HD1[123]' then 'MD1'
    when "Comp_ID"='LEU' and "Atom_ID" similar to 'HD2[123]' then 'MD2'
    when "Comp_ID"='THR' and "Atom_ID" similar to 'HG2[123]' then 'MG'
    when "Comp_ID"='MET' and "Atom_ID" similar to 'HE[123]' then 'ME'
    when "Comp_ID"='LYS' and "Atom_ID" similar to 'HZ[123]' then 'QZ'
    else "Atom_ID" end as atom_id,
    cast("Val" as numeric) as val from macromolecules."Atom_chem_shift" 
    where "Comp_ID" in ('ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY','HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL')) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

-- add outlier count

alter table web.cs_stat_aa_full
  add column num_outliers integer;

update web.cs_stat_aa_full t set num_outliers = o.n
  from (select s.comp_id, s.atom_id, count(a."Comp_ID") as n
          from web.cs_stat_aa_full s
          left join macromolecules."Atom_chem_shift" a
            on a."Comp_ID" = s.comp_id
           and a."Atom_ID" = (case when s.atom_id='MB' then 'HB1'
                 when s.atom_id='MG1' then 'HG11'
                 when s.atom_id='MG2' then 'HG21'
                 when s.atom_id='MG' then 'HG21'
                 when s.atom_id='MD' then 'HD11'
                 when s.atom_id='MD1' then 'HD11'
                 when s.atom_id='MD2' then 'HD21'
                 when s.atom_id='ME' then 'HE1'
                 when s.atom_id='QZ' then 'HZ1'
                 else s.atom_id end)
           and (cast(a."Val" as float) > s.avg + 3 * s.std
             or cast(a."Val" as float) < s.avg - 3 * s.std)
         group by s.comp_id, s.atom_id) o
 where t.comp_id = o.comp_id and t.atom_id = o.atom_id;

--
-- peptide restricted set
--
drop table if exists web.cs_stat_aa_filt;
select distinct comp_id,atom_id,
  case when comp_id='ALA' and atom_id='MB' then count(val)/3
  when comp_id='VAL' and atom_id='MG1' then count(val)/3
  when comp_id='VAL' and atom_id='MG2' then count(val)/3
  when comp_id='ILE' and atom_id='MG' then count(val)/3
  when comp_id='ILE' and atom_id='MD' then count(val)/3
  when comp_id='LEU' and atom_id='MD1' then count(val)/3
  when comp_id='LEU' and atom_id='MD2' then count(val)/3
  when comp_id='THR' and atom_id='MG' then count(val)/3
  when comp_id='MET' and atom_id='ME' then count(val)/3
  when comp_id='LYS' and atom_id='QZ' then count(val)/3
  else count(val) end as count,
  min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std 
  into table web.cs_stat_aa_filt
  from
    (select "Comp_ID" as comp_id,
    case when "Comp_ID"='ALA' and "Atom_ID" similar to 'HB[123]' then 'MB'
    when "Comp_ID"='VAL' and "Atom_ID" similar to 'HG1[123]' then 'MG1'
    when "Comp_ID"='VAL' and "Atom_ID" similar to 'HG2[123]' then 'MG2'
    when "Comp_ID"='ILE' and "Atom_ID" similar to 'HG2[123]' then 'MG'
    when "Comp_ID"='ILE' and "Atom_ID" similar to 'HD1[123]' then 'MD'
    when "Comp_ID"='LEU' and "Atom_ID" similar to 'HD1[123]' then 'MD1'
    when "Comp_ID"='LEU' and "Atom_ID" similar to 'HD2[123]' then 'MD2'
    when "Comp_ID"='THR' and "Atom_ID" similar to 'HG2[123]' then 'MG'
    when "Comp_ID"='MET' and "Atom_ID" similar to 'HE[123]' then 'ME'
    when "Comp_ID"='LYS' and "Atom_ID" similar to 'HZ[123]' then 'QZ'
    else "Atom_ID" end as atom_id,
    cast("Val" as numeric) as val from macromolecules."Atom_chem_shift" 
    where "Comp_ID" in ('ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY','HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL')
    and "Entry_ID" not in (select distinct id from cs_stat_exclude_all union select distinct id from cs_stat_exclude_aa)) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

-- add outlier count

alter table web.cs_stat_aa_filt
  add column num_outliers integer;

update web.cs_stat_aa_filt t set num_outliers = o.n
  from (select s.comp_id, s.atom_id, count(a."Comp_ID") as n
          from web.cs_stat_aa_filt s
          left join macromolecules."Atom_chem_shift" a
            on a."Comp_ID" = s.comp_id
           and a."Atom_ID" = (case when s.atom_id='MB' then 'HB1'
                 when s.atom_id='MG1' then 'HG11'
                 when s.atom_id='MG2' then 'HG21'
                 when s.atom_id='MG' then 'HG21'
                 when s.atom_id='MD' then 'HD11'
                 when s.atom_id='MD1' then 'HD11'
                 when s.atom_id='MD2' then 'HD21'
                 when s.atom_id='ME' then 'HE1'
                 when s.atom_id='QZ' then 'HZ1'
                 else s.atom_id end)
           and (cast(a."Val" as float) > s.avg + 3 * s.std
             or cast(a."Val" as float) < s.avg - 3 * s.std)
         group by s.comp_id, s.atom_id) o
 where t.comp_id = o.comp_id and t.atom_id = o.atom_id;

--
-- everything else
--
drop table if exists web.cs_stat_nstd;
select comp_id,atom_id,count(val) as count,min(val) as min,max(val) as max,round(avg(val),3) as avg,round(stddev(val),3) as std
  into table web.cs_stat_nstd 
  from
    (select "Comp_ID" as comp_id,"Atom_ID" as atom_id,cast("Val" as numeric) as val from macromolecules."Atom_chem_shift" 
    where "Comp_ID" not in ('A','C','G','U','DA','DC','DG','DT','ALA','ARG','ASN','ASP','CYS','GLN','GLU','GLY',
    'HIS','ILE','LEU','LYS','MET','PHE','PRO','SER','THR','TRP','TYR','VAL')) as qry
  group by comp_id,atom_id order by comp_id,atom_id;

--
-- export
--

\copy web.cs_stat_rna_full to '/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/rna_full.csv' csv header
\copy web.cs_stat_rna_filt to '/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/rna_filt.csv' csv header
\copy web.cs_stat_dna_full to '/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/dna_full.csv' csv header
\copy web.cs_stat_dna_filt to '/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/dna_filt.csv' csv header
\copy web.cs_stat_aa_full to '/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/aa_full.csv' csv header
\copy web.cs_stat_aa_filt to '/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/aa_filt.csv' csv header
\copy web.cs_stat_nstd to '/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/others.csv' csv header

--
-- these are for validator
--
-- \copy web.cs_stat_rna_filt to 'rna_refdb.csv' csv header
-- \copy cs_stat_dna_filt_raw to 'dna_refdb.csv' csv header
-- \copy cs_stat_aa_filt_raw to 'aa_refdb.csv' csv header
--
-- eof
