-- borrowed from xlucasdemelo (GitHub)
-- https://gist.github.com/xlucasdemelo/51d8d4a94e9a402daadf0579b0144a76#file-dropschemas-sql

SET search_path = _global, pg_catalog;

CREATE OR REPLACE FUNCTION drop_all () 
   RETURNS VOID  AS
   $$
   DECLARE rec RECORD; 
   BEGIN
        FOR rec IN
        select distinct schemaname
         from pg_catalog.pg_tables
         where schemaname not like 'pg_catalog'  
           LOOP
             EXECUTE 'DROP SCHEMA ' || rec.schemaname || ' CASCADE'; 
           END LOOP; 
           RETURN; 
   END;
   $$ LANGUAGE plpgsql;

select drop_all();