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
