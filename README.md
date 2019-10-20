## `Migration` guide

### Structure sql file (example)
Three files per version `init.sql`, `migration.js` and `finalize.sql` that applied one by one. Versions are applied one by one from oldest to newest (according string sorting).
```
./database/0.0.1/init.sql
./database/0.0.1/migration.js
./database/0.0.1/finalize.sql
./database/0.0.2/init.sql
./database/0.0.2/finalize.sql
./database/0.0.3/finalize.sql
```
The files inside version are optional, but at least one file should present.

### Detect DB version
To detect DB version, the `Migration` is used table with name `version` inside your DB. So do not create a table with same name to prevent conflicts.

### How use

```
import { migration } from "@zxteam/sql-sqlite";

const dbURL: URL = new URL("file:///path/to/db.sql");
const migrationFilesRootPath: string = './database/';

await migration(DUMMY_CANCELLATION_TOKEN, dbURL, migrationFilesRootPath /*, targetVersion*/ ); // targetVersion is optional, if ommited, the migration update DB to latest version
```
