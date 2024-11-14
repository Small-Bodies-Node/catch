# catch

[![CI Tests](https://github.com/Small-Bodies-Node/catch/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/Small-Bodies-Node/catch/actions/workflows/ci-tests.yml)
[![codecov](https://codecov.io/gh/Small-Bodies-Node/catch/graph/badge.svg?token=KFB69L7G65)](https://codecov.io/gh/Small-Bodies-Node/catch)

Planetary Data System Small Bodies Node (PDS-SBN) astronomical survey data search tool.

## Overview

`catch` finds observations of comets, asteroids, and fixed targets in sky survey data. The primary goal is to enable searches on PDS-SBN-archived datasets, but datasets archived elsewhere may be included. It is built on the [`sbsearch`](https://github.com/Small-Bodies-Node/sbsearch) library, and designed to support web-based searches via [`catch-apis`](https://github.com/Small-Bodies-Node/catch-apis).

To illustrate the technique, an example search for comet 65P/Gunn in July/August 2017 is shown in Figure 1. The ephemeris was transformed into a database query using the S2 library, which describes lines or areas in spherical coordinates with a nested grid of cells. The ephemeris cells are compared to the `catch` database, which stores the cell identifiers for all images in the SkyMapper survey. Observations with cells matching any of the ephemeris cells are considered as candidate observations of the comet. Each candidate is checked in detail for the intersection of the ephemeris and observation field-of-view, including time.

Figure 1. Ephemeris (dashed line) and S2 query cells (thin solid lines) for comet 65P/Gunn over the time period 2017 Jul 15 to Aug 15. The field-of-view for a SkyMapper Southern Survey image that matched the ephemeris query is shown (thick solid line). Of all the cells queried, the shaded S2 cell matched the SkyMapper image.
![S2 cell boundaries and comet 65P ephemeris](figures/query-cells-65p-20170715-20170815.png)

## Initial setup

1. Install catch and supporting libraries, e.g.:

   ```bash
   pip install https://github.com/Small-Bodies-Node/catch
   ```

1. Copy `catch.example.config` to `catch.config` and edit:

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

      For day-to-day operations, a more limited set of privileges may be used:

      ```sql
      GRANT SELECT ON all tables IN SCHEMA public TO user;
      GRANT INSERT ON catch_query, found, obj, designation TO user;
      GRANT UPDATE ON catch_query, found TO user;
      GRANT USAGE ON SEQUENCE catch_query_query_id_seq, found_found_id_seq, obj_object_id_seq,designation_desg_id_seq TO user;
      ```

   1. Edit log file location.

1. Run the installed script `catch` to initialize the databases: `catch verify`. See `catch` script below for more.

## `catch` script

The `catch` script is installed along with the library. `catch --help` displays the online help.

- The CATCH configuration file can be explicitly specified as a parameter: `catch --config=/path/to/catch.config`.

- As an alternative, the database and log path may be directly specified, e.g.: `catch --database=postgresql://@/catch --log=catch.temp.log`

- `catch` has three sub-commands:
  - `verify` to verify the database tables (and create them as needed), e.g., `catch verify`.
  - `sources` to list obervational sources, e.g., `catch sources`.
  - `search` to search for a target, e.g., `catch search 65P`. The search and results are stored in the CATCH database tables, as usual.

## Harvest metadata

### NEAT Palomar / GEODSS

The NEAT scripts require the PDS labels and FITS headers. The scripts assume the FITS files are compressed (fpacked) with a ".fz" suffix, but can be easily modified to change that. The scripts examine one directory at a time, looking for PDS3 labels (\*.lbl):

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

The tricam ingestion would fail on two directories in V1.0 of the PDS3 data set: p20020627 and p20020814. p20020627 has three files duplicated from p20020626, and p20020814 has three duplicated from p20020813. The checksums are different, but a visual inspection of the images suggests they are essentially the same data. The duplicate file names in p20020627 and p20020814 are hard-coded into the ingestion script, and skipped to avoid duplication. It relies on the PRODUCT_CREATION_TIME keyword in the PDS3 label, which are different.

Then, optimize the new tables (see below).

### SkyMapper DR4

SkyMapper Data Release 4 exposure (images) and CCD tables are available at [http://skymapper.anu.edu.au/_data/DR4/]. Download these tables and run the corresponding catch script:

```bash
python3 add-skymapper.py SkyMapper.DR4.images.csv.gz SkyMapper.DR4.ccds.part*.csv.gz
```

Use the --noop option to test out the files before adding to the database.

### Pan-STARRS 1 DR2

[Pan-STARRS 1 DR2](https://panstarrs.stsci.edu/) as archived at STScI's MAST is available. CATCH currently searches the "warp" images, which are processed data created from multiple CCDs taken from the same exposure. Three files are needed to add this survey to the database.

- warp metadata: downloaded from MAST CASJobs

  - Query:

    ```sql
    SELECT w.forcedWarpID,w.projectionID,w.skyCellID,w.filterID,w.frameID,f.telescopeID,
    f.expStart,f.expTime,f.airmass,w.crval1,w.crval2,w.crpix1,w.crpix2
    FROM ForcedWarpMeta as w INNER JOIN FrameMeta as f ON w.frameID = f.frameID;
    ```

  - Download as FITS binary table.

- warp files list: a FITS table of warp file names and observation dates (MJD-OBS keyword in the FITS headers). This is required in order to associate an observation time from the warp metadata table (expStart) with a specific warp FITS file. The table was originally provided by Rick White at STScI, generated from the archive.
- PS1 project cell definitions: <https://outerspace.stsci.edu/download/attachments/10257181/ps1grid.fits?version=3&modificationDate=1532367528459&api=v2>

Harvest the metadata with the `add-ps1-dr2.py` script:

```bash
python3 add-ps1-dr2.py ps1warp.fit mjd-table.fits ps1grid.fits
```

IIRC, these data take several hours to ingest (be sure to optimize the tables afterward). The script has a few options that may be useful in case the process is interrupted and needs to be restarted.

### Catalina Sky Survey

The SBN is the main archive for the Catalina Sky Survey (CSS). As of February 2022, the CSS data archive is continually updated with observations from five telescopes ([Seaman et al. 2022](https://sbn.psi.edu/pds/resource/css.html)), including the Bok NEO Survey at Kitt Peak. The harvesting script, `add-css.py`, will keep track of which PDS4 labels have been previously examined and only harvest new metadata.

```bash
python3 add-css.py
```

Within CATCH, the archive is split by observing site (not telescope), i.e., Mt. Bigelow, Mt. Lemmon, and Kitt Peak. The script will direct each observation to the appropriate tables.

The `scripts/daily-harvest` directory contains scripts that may be useful in daily checks for new data. Copy them to a new location and setup the virtual environment: `bash _build_venv`. Then, periodically check for new data with `bash daily-harvest.sh`.

### Spacewatch

The SBN is the main archive for the Spacewatch survey and the 0.9m Mosaic Camera Survey ([Brucker et al. 2022](https://sbn.psi.edu/pds/resource/sw.html)) from 2003 to 2016 has been archived.

The script `scripts/add-spacewatch.py` will harvest metadata from calibrated data labels. It requires a local copy of the labels. They may be downloaded with:

`wget -r -R *.fits --no-parent https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/`

### LONEOS

The SBN also archives the LONEOS survey.

Download the labels and harvest metadata with the `scripts/add-loneos.py` script.

`wget -r -R *.fits --no-parent https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.loneos.survey/data_augmented/`

## Modifying existing surveys

After deleting any observations, the observation spatial index must be regenerated: `REINDEX ix_observation_spatial_terms`.

## Adding new surveys

Detailed instructions are TBW.

1. Download and inspect the data source, determine which metadata are important for user searches, i.e., which will be saved to the database. Exposure start/stop and field of view are required.

1. Copy `sbsearch/model/example_survey.py` from the sbsearch repository to `catch/model/survey_name.py`, and edit as instructed by the comments. Add additional methods to the main survey object as described in `catch/model/neat_palomar_tricam.py`.

1. Edit `sbsearch/model/__init__.py` to import the main data object from that file, following the other surveys as examples.

1. Create a script to harvest metadata into the database and save to `scripts/`. Run it.

## Database tasks

The database shouldn't need any periodic maintenance. However, the following tasks may be of use.

### Optimize observation tables

After adding new observations to a survey, the tables may be optimized with, e.g.,:

```sql
VACUUM ANALYZE;
```

### Query reset

To clear the query table, connect to the database and execute the following SQL command:

```sql
DELETE FROM catch_query WHERE TRUE;
```

To verify:

```sql
surveys=> SELECT COUNT(*) FROM catch_query;
 count
-------
     0
(1 row)
```

Note, the `obj` and `found` tables will still be populated with objects and observations of found objects, respectively. Their existence will not have an adverse effect on new CATCH queries, except potentially on performance (i.e., database bloat).

### Found objects reset

To reset the found objects, the `catch_query` and `found` tables must be cleared. Connect to the database and execute the following SQL commands:

```sql
DELETE FROM catch_query WHERE TRUE;
DELETE FROM found WHERE TRUE;
```

Note, the `found` table cannot be reset independently from the `catch_query` table, otherwise new queries with the cache enabled (`Catch.query(... cached=True)`) may find a previous query, assume the object was not found in the database, and return no results.

### Backup

Save the database to a file with `pg_dump`. For a CATCH database named "catch":

```bash
pg_dump --format=custom --no-owner -f "catch.backup" catch
```

To save a sub-set of the tables, e.g., for copying the survey metadata but not cached searches, exclude the the following tables: catch_query, designation, ephemeris, found, obj, and orbit. The remaining tables should only be survey metadata.

```bash
# DRAFT
pg_dump --format=custom --no-owner --exclude-table='catch_query*' \
  --exclude-table='designation*' --exclude-table='ephemeris*' \
  --exclude-table='found*' --exclude-table='obj*' --exclude-table='orbit*' \
  --no-acl -f "catch.backup" catch
# DRAFT
```

### Restore

The database backup created with `pg_dump` (as above) may be restored with `pg_restore`. Again, for a CATCH database named "catch":

```bash
pg_restore --clean --if-exists -d catch catch.backup
```

**Warning** Backups limited to survey metadata cannot be restored to a previously populated database without first clearing the catch_query and found tables. See [Found objects reset](#found-objects-reset) for instructions.

## Acknowledgements

Thanks to Rick White (STScI) for assistance with the Pan-STARRS 1 archive.
