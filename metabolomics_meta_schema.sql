--
--
--
drop schema if exists meta cascade;
create schema meta;
grant usage on schema meta to public;
grant select on all tables in schema meta to public;
alter default privileges in schema meta grant select on tables to public;

set search_path = meta, pg_catalog;

create table alphabetized_list (
    dir_name text primary key,
    common_name text,
    alpha_name text,
    first_char character(1),
    has_theoretical text,
    image_file text,
    struct_file text,
    date_struct_finalized date,
    struct_source text
);

create sequence basic_data_entries_info_id_seq;

create table basic_data_entries_info (
    id integer primary key default nextval('basic_data_entries_info_id_seq'),
    bmrbtype text,
    bmrbid text,
    dirname text,
    bottlename text,
    commonname text
);

create sequence bmse_mol_formulas_id_seq;

create table bmse_mol_formulas (
    id integer primary key default nextval('bmse_mol_formulas_id_seq'),
    bmrb_id text,
    dir_name text,
    common_name text,
    formula text
);

create sequence c13_peaks_id_seq;

create table c13_peaks (
    id integer primary key default nextval('c13_peaks_id_seq'),
    mol_dir_name text,
    bmrbacc text,
    sub_id integer,
    chem_shift double precision,
    intensity double precision
);

create sequence c13peakcount_id_seq;

create table c13peakcount (
    id integer primary key default nextval('c13peakcount_id_seq'),
    bmrbid character(10),
    peakcount integer
);

create sequence c_transtable_id_seq;

create table c_transtable (
    id integer primary key default nextval('c_transtable_id_seq'),
    peak_id integer,
    val double precision,
    bmrbid text
);

create sequence h1_peaks_id_seq;

create table h1_peaks (
    id integer primary key default nextval('h1_peaks_id_seq'),
    mol_dir_name text,
    bmrbacc text,
    sub_id integer,
    chem_shift double precision,
    intensity double precision
);

create sequence h1peakcount_id_seq;

create table h1peakcount (
    id integer primary key default nextval('h1peakcount_id_seq'),
    bmrbid text,
    peakcount integer
);

create sequence h_transtable_id_seq;

create table h_transtable (
    id integer primary key default nextval('h_transtable_id_seq'),
    peak_id integer,
    val double precision,
    bmrbid text
);

create sequence hsqc_peaks_id_seq;

create table hsqc_peaks (
    id integer primary key default nextval('hsqc_peaks_id_seq'),
    mol_dir_name text,
    bmrbacc text,
    sub_id integer,
    x_chem_shift double precision,
    y_chem_shift double precision,
    x_width double precision,
    y_width double precision,
    height double precision,
    volume double precision
);

create sequence hsqcpeakcount_id_seq;

create table hsqcpeakcount (
    id integer primary key default nextval('hsqcpeakcount_id_seq'),
    bmrbid text,
    peakcount integer
);

create sequence hsqctrans_id_seq;

create table hsqctrans (
    id integer primary key default nextval('hsqctrans_id_seq'),
    peak_id integer,
    h_val double precision,
    c_val double precision,
    intensity_rank double precision,
    bmrbid text
);

create sequence jr_classifications_id_seq;

create table jr_classifications (
    id integer primary key default nextval('jr_classifications_id_seq'),
    bmrbid text,
    mol_dir_name text,
    classification text
);

create sequence mol_name_formula_kegg_pubchem_id_seq;

create table mol_name_formula_kegg_pubchem (
    id integer primary key default nextval('mol_name_formula_kegg_pubchem_id_seq'),
    mol_name text,
    formula text,
    kegg_id text,
    pubchem_sid text
);

create table monoisotopic_mol_masses (
    formula text,
    c_isotope integer,
    n_isotope integer,
    mass double precision,
    primary key (formula, c_isotope, n_isotope)
);


create table nist_most_common_atom_masses (
    atomnum integer,
    symbol text,
    isotope integer,
    mass double precision,
    mass_stddev double precision,
    percentcomp double precision,
    percent_stdv double precision,
    stand_mw double precision,
    stand_mw_stdv double precision,
    theoreticalval character(1)
);
