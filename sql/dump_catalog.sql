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
  'timeline_id', (select timeline_id from pg_control_checkpoint()),
  -- restart_lsn of the temporary slot `pgs3 init` creates in this session: the
  -- point from which every later commit decodes completely. Null otherwise.
  'start_lsn', (select restart_lsn::text from pg_replication_slots
                where slot_name = 'pgs3_init_' || pg_backend_pid()),
  -- Relfilenodes of the catalogs that define a relation's rowtype. Writes to
  -- these are what can invalidate the frozen dictionary. Captured rather than
  -- assumed, because these are mapped relations whose filenode changes if the
  -- catalog is ever rewritten.
  'rowtype_catalogs', json_build_object(
     'pg_class',     pg_relation_filenode('pg_catalog.pg_class')::bigint,
     'pg_attribute', pg_relation_filenode('pg_catalog.pg_attribute')::bigint),
  'relations',      coalesce(json_agg(r order by r->>'relfilenode'), '[]'::json),
  -- Every type a published value can contain, so the encoder can produce its
  -- send() form: array elements, domain bases and composite attributes,
  -- followed recursively.
  'types', (
    with recursive reach(oid) as (
      select a.atttypid
      from pg_publication_tables pt
      join pg_namespace n on n.nspname = pt.schemaname
      join pg_class c     on c.relnamespace = n.oid and c.relname = pt.tablename
      join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
      where pt.pubname = :'pubname'
      union
      select x.oid
      from reach
      join pg_type ty on ty.oid = reach.oid
      cross join lateral (
        select ty.typelem where ty.typelem <> 0 and ty.typlen = -1
        union all
        select ty.typbasetype where ty.typtype = 'd'
        union all
        select a.atttypid from pg_attribute a
        where ty.typtype = 'c' and a.attrelid = ty.typrelid and a.attnum > 0 and not a.attisdropped
      ) x(oid)
    )
    select coalesce(json_agg(json_build_object(
      'oid',      ty.oid::bigint,
      'typname',  ty.typname,
      'typtype',  ty.typtype,
      'typlen',   ty.typlen,
      'typalign', ty.typalign,
      'elem',     case when ty.typelem <> 0 and ty.typlen = -1 then ty.typelem::bigint end,
      'basetype', case when ty.typtype = 'd' then ty.typbasetype::bigint end,
      'labels',   case when ty.typtype = 'e' then (
                    select json_agg(json_build_array(e.oid::bigint, e.enumlabel) order by e.enumsortorder)
                    from pg_enum e where e.enumtypid = ty.oid) end,
      'attrs',    case when ty.typtype = 'c' then (
                    select json_agg(json_build_object(
                             'atttypid', a.atttypid::bigint, 'attlen', a.attlen,
                             'attalign', a.attalign, 'attisdropped', a.attisdropped) order by a.attnum)
                    from pg_attribute a where a.attrelid = ty.typrelid and a.attnum > 0) end
    ) order by ty.oid), '[]'::json)
    from reach join pg_type ty on ty.oid = reach.oid
  )
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
               'typnsp',     tn.nspname
             ) order by a.attnum), '[]'::json)
      from pg_attribute a
      join pg_type t  on t.oid = a.atttypid
      join pg_namespace tn on tn.oid = t.typnamespace
      left join lateral (
        select true as iskey
        from pg_index i
        -- DEFAULT identity is the primary key; indisreplident is only set by
        -- REPLICA IDENTITY USING INDEX.
        where i.indrelid = c.oid
          and case c.relreplident when 'd' then i.indisprimary
                                  when 'i' then i.indisreplident
                                  else false end
          and a.attnum = any (i.indkey::smallint[])
        limit 1
      ) k on true
      -- attisdropped columns are kept: dropped attrs still occupy tuple slots
      where a.attrelid = c.oid and a.attnum > 0
    )
  ) as r
  -- pg_publication_tables, not pg_publication_rel: the latter is empty for
  -- FOR ALL TABLES and lists partitioned roots rather than the leaves whose
  -- relfilenodes actually appear in WAL.
  from pg_publication_tables pt
  join pg_namespace n on n.nspname = pt.schemaname
  join pg_class c     on c.relnamespace = n.oid and c.relname = pt.tablename
  where pt.pubname = :'pubname'
) s;
