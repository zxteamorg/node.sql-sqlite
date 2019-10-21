# ZXTeam's SQLite Facade
[![npm version badge](https://img.shields.io/npm/v/@zxteam/sql-sqlite.svg)](https://www.npmjs.com/package/@zxteam/sql-sqlite)
[![downloads badge](https://img.shields.io/npm/dm/@zxteam/sql-sqlite.svg)](https://www.npmjs.org/package/@zxteam/sql-sqlite)
[![commit activity badge](https://img.shields.io/github/commit-activity/m/zxteamorg/node.sql-sqlite)](https://github.com/zxteamorg/node.sql-sqlite/pulse)
[![last commit badge](https://img.shields.io/github/last-commit/zxteamorg/node.sql-sqlite)](https://github.com/zxteamorg/node.sql-sqlite/graphs/commit-activity)
[![twitter badge](https://img.shields.io/twitter/follow/zxteamorg?style=social&logo=twitter)](https://twitter.com/zxteamorg)

## Interfaces

## Classes
### SqliteProviderFactory
Provides SQLite implementation for `EmbeddedSqlProviderFactory` interface defined in [`@zxteam/sql` contact package](https://github.com/zxteamorg/node.sql)

## Functions
### migration
```typescript
import { DUMMY_CANCELLATION_TOKEN } from "@zxteam/cancellation";
import { migration } from "@zxteam/sql-sqlite";

const dbURL: URL = new URL("file:///path/to/db.sql");
const migrationFilesRootPath: string = './database/';

const sqlProviderFactory = new SqliteProviderFactory(dbURL);

await sqlProviderFactory.migration(DUMMY_CANCELLATION_TOKEN, migrationFilesRootPath /*, targetVersion*/ ); // targetVersion is optional, if ommited, the migration update DB to latest version
```


## Guides

### `Migration` guide

#### Structure sql file (example)
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

#### Detect DB version
To detect DB version, the `Migration` is used table with name `version` inside your DB. So do not create a table with same name to prevent conflicts.

#### Update DB
TBD
