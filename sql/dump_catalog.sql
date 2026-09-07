-- Frozen catalog dictionary, captured once at T0 against the SOURCE primary.
-- After this point the decoder never touches the primary's catalog, so the
-- primary is free to vacuum. See README for why that matters.
--
-- Emits a single JSON document on stdout. Run with:
--   psql -X -t -A -f sql/dump_catalog.sql -v pubname=p > catalog.json

select json_build_object(
  'database_oid',   (select oid::bigint from pg_database where datname = current_database()),
  'wal_segment_size', (select setting::bigint from pg_settings where name = 'wal_segment_size'),
  'server_version_num', (select setting::int from pg_settings where name = 'server_version_num'),
  'system_identifier', (select system_identifier::text from pg_control_system()),
  'captured_at_lsn', pg_current_wal_lsn()::text,
  -- Relfilenodes of the catalogs that define a relation's rowtype. Writes to
  -- these are what can invalidate the frozen dictionary. Captured rather than
  -- assumed, because these are mapped relations whose filenode changes if the
  -- catalog is ever rewritten.
  'rowtype_catalogs', json_build_object(
     'pg_class',     pg_relation_filenode('pg_catalog.pg_class')::bigint,
     'pg_attribute', pg_relation_filenode('pg_catalog.pg_attribute')::bigint),
  'relations',      coalesce(json_agg(r order by r->>'relfilenode'), '[]'::json)
)
from (
  select json_build_object(
    'oid',           c.oid::bigint,
    'relfilenode',   pg_relation_filenode(c.oid)::bigint,
    'tablespace_oid', c.reltablespace::bigint,
    'nspname',       n.nspname,
    'relname',       c.relname,
    'relreplident',  c.relreplident,
    -- toast heap for this table, so the reorder buffer can route chunk inserts
    'toast_relfilenode', nullif(pg_relation_filenode(c.reltoastrelid), 0)::bigint,
    'columns', (
      select coalesce(json_agg(json_build_object(
               'attnum',     a.attnum,
               'attname',    a.attname,
               'atttypid',   a.atttypid::bigint,
               'atttypmod',  a.atttypmod,
               'attlen',     a.attlen,
               'attbyval',   a.attbyval,
               'attalign',   a.attalign,
               'attstorage', a.attstorage,
               'attnotnull', a.attnotnull,
               'attisdropped', a.attisdropped,
               'attispkey',  coalesce(k.iskey, false),
               'attgenerated', a.attgenerated,
               'typname',    t.typname,
               'typnsp',     tn.nspname,
               -- Array element metadata, needed to walk the array's payload and
               -- encode each element. Null for non-array columns.
               'elem', case when t.typcategory = 'A' then json_build_object(
                              'oid',      et.oid::bigint,
                              'typname',  et.typname,
                              'typlen',   et.typlen,
                              'typalign', et.typalign) end
             ) order by a.attnum), '[]'::json)
      from pg_attribute a
      join pg_type t  on t.oid = a.atttypid
      join pg_namespace tn on tn.oid = t.typnamespace
      left join pg_type et on et.oid = t.typelem
      left join lateral (
        select true as iskey
        from pg_index i
        where i.indrelid = c.oid and i.indisreplident
          and a.attnum = any (i.indkey::smallint[])
        limit 1
      ) k on true
      -- attisdropped columns are kept: dropped attrs still occupy tuple slots
      where a.attrelid = c.oid and a.attnum > 0
    )
  ) as r
  from pg_publication_rel pr
  join pg_class c     on c.oid = pr.prrelid
  join pg_namespace n on n.oid = c.relnamespace
  join pg_publication p on p.oid = pr.prpubid
  where p.pubname = :'pubname'
) s;
