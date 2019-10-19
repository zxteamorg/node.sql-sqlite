## Structure sql file (example)
```
./database/001/init.sql
./database/001/migration.js
./database/001/finalize.sql
./database/002/init.sql
./database/002/migration.js
./database/002/finalize.sql
```

## How use

```
const sqliteUrlTodb = "path/to/db.sql";
const factory = new SqliteProviderFactory(sqliteUrlTodb);

const currentVersion = '002';
const futureVersion = '004'; 
const pathToFiles = './database/';
factory.migration(cancellationToken, pathToFiles, currentVersion, futureVersion)
```
