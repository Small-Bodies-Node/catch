# catch v0.2.0
SBN astronomical survey data search tool 

## Adding new surveys

1. Create mapper object in schema.

1. Provide observatory code as __obscode__ attribute.

1. Add source name and mapper object to `Catch.SURVEYS`.

1. Create script to ingest data into database and save to `script/`.