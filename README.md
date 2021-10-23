# catch v1.0.0-dev

SBN astronomical survey data search tool.


## Initial setup

1. Install catch and supporting libraries, e.g.:

   ```bash
   pip install https://github.com/Small-Bodies-Node/catch
   ```

1. Copy ``catch.example.config`` to ``catch.config`` and edit:
   1. Decide which database backend will be used, e.g., sqlite, postgresql, or mariadb.
      1. For sqlite: set "database" in ``catch.config`` to, e.g., "sqlite:////path/to/catch.db".
      1. For postgresql:
         1. Set "database" to, e.g.:
            1. postgresql:///catch
            1. postgresql://user:password@/catch?host=/tmp
            1. postgresql://user:password@host/catch
         1. Create that database and allow user access, e.g.:

            ```bash
            createdb catch
            psql -d catch
            ```

            For the user who maintains catch:

            ```sql
            GRANT ALL PRIVILEGES ON DATABASE catch TO user;
            ```

            For the user who runs catch: TBD

      1. For mariadb, ?

   1. Edit log file location.
1. Run the provided script `scripts/catch` to initialize the databases: `python3 scripts/catch verify`.

## Harvest metadata
### NEAT Palomar / GEODSS

The NEAT scripts require the PDS labels and FITS headers.  The scripts assume the FITS files are compressed (fpacked) with a ".fz" suffix, but can be easily modified to change that.  The scripts examine one directory at a time, looking for PDS3 labels (*.lbl):

```bash
python3 add-neat-palomar.py /path/to/archive/neat/tricam/data/p20011120/obsdata/
```

All NEAT tricam data directories may be discovered and ingested with a for-loop:

```bash
for d in /path/to/archive/neat/tricam/data/*/obsdata; do
  python3 add-neat-palomar.py $d;
done
```

And similarly for the GEODSS script and data:

```bash
for d in /path/to/archive/neat/geodss/data/*/obsdata; do
  python3 add-neat-maui-geodss.py $d;
done
```

The tricam ingestion would fail on two directories in V1.0 of the PDS3 data set: p20020627 and p20020814.  p20020627 has three files duplicated from p20020626, and p20020814 has three duplicated from p20020813.  The checksums are different, but a visual inspection of the images suggests they are essentially the same data.  The duplicate file names in p20020627 and p20020814 are hard-coded into the ingestion script, and skipped to avoid duplication.  It relies on the PRODUCT_CREATION_TIME keyword in the PDS3 label, which are different.

Then, optimize the new tables (see below).

### SkyMapper DR2

SkyMapper Data Release 2 exposure (images) and CCD tables are available at [http://skymapper.anu.edu.au/_data/DR2/].  Download these tables and run the corresponding catch script:

```bash
python3 add-skymapper.py dr2_images.csv.gz dr2_ccds.csv.gz
```

Then, optimize the new tables (see below).

## Modifying existing surveys

After inserting, updating, or deleting survey observations, connect to the database and optimize the observation table's spatial index: `VACUUM ANALYZE skymapper, skymapper_spatial_terms;`.


## Adding new surveys

Detailed instructions are TBW.

1. Download and inspect the data source, determine which metadata are important for user searches, i.e., which will be saved to the database.  Exposure start/stop and field of view are required.

1. Copy `sbsearch/model/example_survey.py` from the sbsearch repository to `catch/model/survey_name.py`, and edit as instructed by the comments.  Add additional methods to the main survey object as described in `catch/model/neat_palomar_tricam.py`.

1. Edit `sbsearch/model/__init__.py` to import the main data object from that file, following the other surveys as examples.

1. Create a script to harvest metadata into the database and save to `scripts/`.  Run it.


## Database tasks

The database shouldn't need any periodic maintenance.  However, the following tasks may be of use.

### Optimize observation tables

After adding new observations to a survey, the tables may be optimized with, e.g.,:

```sql
VACUUM ANALYZE observation, skymapper, skymapper_spatial_terms;
```


### Query reset

To clear the query table, connect to the database and execute the following SQL command:

```sql
DELETE FROM catch_query WHERE TRUE;
```

which will clear the `catch_query` and `caught` tables.  To verify:

```sql
surveys=> SELECT COUNT(*) FROM catch_query;
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

Revise: ??? To reset the found objects, both the `catch_query` and `obj` tables must be cleared ???  Connect to the database and execute the following SQL commands:

```sql
DELETE FROM catch_query WHERE TRUE;
DELETE FROM obj WHERE TRUE;
```

which will also clear the found objects table, `found`.

Note, the `found` table cannot be reset independently from the `catch_query` table, otherwise new queries with the cache enabled (`Catch.query(... cached=True)`) may find a previous query, assume the object was not found in the database, and return no results.

### Backup

Save the database to a file with `pg_dump`.  For a CATCH database named "surveys":

```bash
pg_dump -Fc -b -v -f "catch-surveys.backup" surveys
```

### Restore

The database backup created with `pg_dump` (as above) may be restored with `pg_restore`.  Again, for a CATCH database named "surveys":

```bash
pg_restore --clean --if-exists -d surveys catch-surveys.backup
```
