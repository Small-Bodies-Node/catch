# catch v0.4.2
SBN astronomical survey data search tool 

## Adding new surveys

Detailed instructions are TBD.

1. Create mapper object in schema.

1. Add source name and mapper object to `Catch.SOURCES`.

1. Create script to ingest data into database and save to `script/`.

1. Connect to the database and optimize the observation table's spatial index:
```
VACUUM ANALYZE obs;
```

## Modifying existing surveys

After inserting, updating, or deleting survey observations, connect to the database and optimize the observation table's spatial index: `VACUUM ANALYZE obs;`.

If the observation table for an existing survey is modified, especially if new observations are added, then the queries against that survey must be reset.  If the survey source name is named "asdf" (defined via attribute `Catch.SOURCES`), then connect to the database and execute the following SQL command:

```
DELETE FROM catch_queries WHERE source='asdf';
```

## Database tasks

The database shouldn't need any periodic maintenance.  However, the following tasks may be of use.

### Query reset

To clear the query table, connect to the database and execute the following SQL command:
```
DELETE FROM catch_queries WHERE TRUE;
```
which will clear the `catch_queries` and `caught` tables.  To verify:
```
surveys=> SELECT COUNT(*) FROM catch_queries;
 count 
-------
     0
(1 row)

surveys=> SELECT COUNT(*) FROM caught;
 count 
-------
     0
(1 row)
```

Note, the `obj` and `found` tables will still be populated with objects and observations of found objects, respectively.  Their existence will not have an adverse effect on new CATCH queries.

### Found objects reset

To reset the found objects, both the `catch_queries` and `obj` tables must be cleared.  Connect to the database and execute the following SQL commands:
```
DELETE FROM catch_queries WHERE TRUE;
DELETE FROM obj WHERE TRUE;
```
which will also clear the found objects table, `found`.

Note, the `found` table cannot be reset independently from the `catch_queries` table, otherwise new queries with the cache enabled (`Catch.query(... cached=True)`) may find a previous query, assume the object was not found in the database, and return no results.

### Backup

Save the database to a file with `pg_dump`.  For a CATCH database named "surveys":
```
pg_dump -Fc -b -v -f "catch-surveys.backup" surveys
```

### Restore

Before the CATCH database can be restored, the PostGIS extensions must be loaded on the server and the SBSearch coordinate system (a normal sphere), must be defined.  This requirement is documented in the [`sbsearch` README](https://github.com/Small-Bodies-Node/sbsearch/blob/master/README.md).  If the PostgreSQL server has already been used for CATCH, then verify that the coordinate system exists with:
```
surveys=> SELECT * FROM public.spatial_ref_sys WHERE auth_name = 'SBSearch';
 srid  | auth_name | auth_srid |                                                                  srtext                                                                   |              proj4text               
-------+-----------+-----------+-------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------
 40001 | SBSearch  |         1 | GEOGCS["Normal Sphere (r=6370997)",DATUM["unknown",SPHEROID["sphere",6370997,0]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]] | +proj=longlat +ellps=sphere +no_defs
(1 row)
```
If this is a new PostgreSQL instance, then see the [`sbsearch` README](https://github.com/Small-Bodies-Node/sbsearch/blob/master/README.md) for setup instructions.

Once the PostGIS extension is loaded and SBSearch coordinate system defined, the database backup created with `pg_dump` (as above) may be restored with `pg_restore`.  Again, for a CATCH database named "surveys":
```
pg_restore --clean --if-exists -d surveys catch-surveys.backup
```
