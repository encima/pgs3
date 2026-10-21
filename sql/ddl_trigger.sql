-- Installed by `pgs3 init` on the source. Whenever DDL touches a published
-- table, a type, or the publication itself, write a fresh dictionary into WAL
-- inside the same transaction, so pgs3 can switch to it exactly where the
-- change happens instead of refusing to go on.
--
-- psql variables: pubname. pgs3.dictionary(text) is created just before this,
-- from sql/dump_catalog.sql.

create or replace function pgs3.emit_dictionary() returns event_trigger
language plpgsql security definer set search_path = pg_catalog, pg_temp as $f$
declare
  pub  text;
  dict text;
begin
  -- One message per publication a pgs3 serves; each pgs3 keeps its own.
  for pub in select p.pubname from pg_publication p
             where obj_description(p.oid, 'pg_publication') = 'pgs3'
  loop
    continue when not exists (
      select 1 from pg_event_trigger_ddl_commands() c
      where c.object_type in ('publication', 'publication relation', 'publication namespace',
                              'type', 'domain')
         or (c.classid = 'pg_class'::regclass and c.objid in (
               select cl.oid from pg_publication_tables pt
               join pg_namespace n on n.nspname = pt.schemaname
               join pg_class cl on cl.relnamespace = n.oid and cl.relname = pt.tablename
               where pt.pubname = pub)));
    -- Never fail the user's DDL. Without a dictionary, pgs3 stops at this
    -- change and says so, exactly as it would with no trigger at all.
    begin
      dict := pgs3.dictionary(pub)::text;
    exception when others then
      raise warning 'pgs3: no dictionary recorded for publication % (%); pgs3 will stop at this change',
        pub, sqlerrm;
      continue;
    end;
    -- Outside the exception block, so it is the DDL's own (sub)transaction
    -- that carries it, and it goes wherever the DDL goes on rollback.
    perform pg_logical_emit_message(true, 'pgs3.dictionary', dict);
  end loop;
end
$f$;

drop event trigger if exists pgs3_dictionary;
-- EXECUTE PROCEDURE, not FUNCTION: the spelling every supported major accepts.
create event trigger pgs3_dictionary on ddl_command_end execute procedure pgs3.emit_dictionary();

-- The publications to write dictionaries for are the ones marked like this.
-- A comment someone else put there is not overwritten.
select set_config('pgs3.publication', :'pubname', false);
do $$
declare
  pub text := current_setting('pgs3.publication');
  cur text := obj_description((select oid from pg_publication where pubname = pub), 'pg_publication');
begin
  if cur is distinct from 'pgs3' and cur is not null then
    raise exception 'publication % already has a comment (%); pgs3 marks the publications it '
                    'follows with the comment ''pgs3''', pub, cur;
  end if;
  execute format('comment on publication %I is %L', pub, 'pgs3');
end
$$;
