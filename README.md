# catch v0.5.0
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

### SkyMapper DR2

SkyMapper Data Release 2 exposure (images) and CCD tables are available [for download](http://skymapper.anu.edu.au/_data/DR2/).  Download these tables and run the corresponding catch script:
```bash
python3 add-skymapper.py dr2_images.csv.gz dr2_ccds.csv.gz
```

### PanSTARRS 1 DR2

PanSTARRS 1 Data Release 2 exposure (frame) and warp (CCD) tables are available via [MAST CasJobs](http://mastweb.stsci.edu/mcasjobs/).  Run the following query and download the result as a FITS table:
```sql
SELECT w.forcedWarpID,w.projectionID,w.skyCellID,w.filterID,w.frameID,f.telescopeID,f.expStart,f.expTime,f.airmass,w.crval1,w.crval2,w.crpix1,w.crpix2 FROM ForcedWarpMeta as w INNER JOIN FrameMeta as f ON w.frameID = f.frameID;
```

PS1 DR2 has a [cutout service](https://outerspace.stsci.edu/display/PANSTARRS/PS1+Image+Cutout+Service) that accepts several parameters, including RA, Dec, size, and format (FITS or JPEG).  The API requires a file name, but as of 2020 Oct 7, there is no way to determine the single-epoch warp image file name from the DR2 database.  The issue is that the file names are based on observation dates, but the dates do not match the metadata in the database or FITS headers.  Until this is remedied, an additional table is required, manually generated at STScI (with careful removal of duplicate entries; thanks to Rick White).  The table columns are filename, mjdobs, projcell, skycell, and filterid, extracted from the warp image FITS headers.  Note that this table has 21 million rows, but the CasJobs query returns 19 million.  Moreover, joining these two tables together with a ±1 or 10 s tolerance returns 17,568,050 matches.  However, we take a conservative approach and require exact matches.

Finally, a [table of warp image sizes](https://outerspace.stsci.edu/download/attachments/10257181/ps1grid.fits?version=3&modificationDate=1532367528459&api=v2) is also needed.  See [PS1 Sky Tessellation Patterns](https://outerspace.stsci.edu/display/PANSTARRS/PS1+Sky+tessellation+patterns) for details.

With the three tables, run the ingestion script:
```bash
python3 add-ps1-dr2.py ps1warp.fit mjd-table.fits ps1grid.fits
```
A temporary SQLite database will be built, which takes a few hours.  Computing image corners and inserting observations into the CATCH database takes about 12 hours.


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
