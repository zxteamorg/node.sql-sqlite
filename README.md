## Structure sql file (example)
./database/v001/10-table/01-user.sql
./database/v001/10-table/02-work.sql
./database/v001/20-data/01-user.sql
./database/v001/20-data/02-user.sql
./database/v002/migration.sql
./database/v003/migration.sql
./database/v004/migration.sql

## How use

```
const sqliteUrlTodb = "path/to/db.sql";
const factory = new SqliteProviderFactory(sqliteUrlTodb);

const currentVersion = 'v002';
const futureVersion = 'v004'; 
const pathToFiles = './database/';
factory.migration(cancellationToken, pathToFiles, currentVersion, futureVersion)
```
