import { CancellationToken, Logger } from "@zxteam/contract";
import { MigrationManager, SqlProvider } from "@zxteam/sql";


import { splitScriptToStatements } from "./splitScriptToStatements";

export class SqliteMigrationManager extends MigrationManager {

	public getCurrentVersion(cancellationToken: CancellationToken): Promise<string | null> {
		return this.sqlProviderFactory.usingProvider(cancellationToken, async (sqlProvider: SqlProvider) => {

			const isExist = await this._isVersionTableExist(cancellationToken, sqlProvider);
			if (isExist === false) { return null; }

			await this._verifyVersionTableStructure(cancellationToken, sqlProvider);

			const versionData = await sqlProvider.statement(
				`SELECT [version] FROM [${this.versionTableName}] ORDER BY [version] DESC LIMIT 1`
			).executeScalarOrNull(cancellationToken);

			if (versionData === null) {
				return null;
			}

			return versionData.asString;
		});
	}

	protected async _createVersionTable(cancellationToken: CancellationToken, sqlProvider: SqlProvider): Promise<void> {
		const tableCountData = await sqlProvider.statement(
			"SELECT COUNT(*) FROM [sqlite_master] WHERE [type] = 'table'"
		).executeScalar(cancellationToken);
		if (tableCountData.asInteger !== 0) {
			throw new SqliteMigrationManager.MigrationError("Your database has tables. Create Version Table allowed only for an empty database. Please create Version Table yourself.");
		}

		await sqlProvider.statement(
			`CREATE TABLE [${this.versionTableName}] (` +
			`[version] VARCHAR(64) NOT NULL PRIMARY KEY, ` +
			`[date_unix_deployed_at] INTEGER NOT NULL, ` +
			`[log] TEXT NOT NULL`
			+ `)`
		).execute(cancellationToken);
	}

	protected async _executeMigrationSql(
		cancellationToken: CancellationToken, sqlProvider: SqlProvider, migrationLogger: Logger, sqlText: string
	): Promise<void> {
		const statements: Array<string> = await splitScriptToStatements(cancellationToken, sqlText);
		for (const statement of statements) {
			await super._executeMigrationSql(cancellationToken, sqlProvider, migrationLogger, statement);
		}
	}

	protected async _insertVersionLog(
		cancellationToken: CancellationToken, sqlProvider: SqlProvider, version: string, logText: string
	): Promise<void> {
		await sqlProvider.statement(
			`INSERT INTO [${this.versionTableName}]([version], [date_unix_deployed_at], [log]) VALUES(?, ?, ?)`
		).execute(cancellationToken, version, Math.trunc(Date.now() / 1000), logText);
	}

	protected async _isVersionTableExist(cancellationToken: CancellationToken, sqlProvider: SqlProvider): Promise<boolean> {
		const isExistSqlData = await sqlProvider.statement(
			"SELECT 1 FROM [sqlite_master] WHERE [type] = 'table' AND [name] = ?"
		).executeScalarOrNull(cancellationToken, this.versionTableName);

		if (isExistSqlData === null) { return false; }
		if (isExistSqlData.asInteger !== 1) { throw new SqliteMigrationManager.MigrationError("Unexpected SQL result"); }

		return true;
	}

	protected async _verifyVersionTableStructure(cancellationToken: CancellationToken, sqlProvider: SqlProvider): Promise<void> {
		const isExist = await this._isVersionTableExist(cancellationToken, sqlProvider);
		if (isExist === false) { throw new SqliteMigrationManager.MigrationError(`The database does not have version table: ${this.versionTableName}`); }

		// TODO check columns
		// It is hard to check without schema name
		// SELECT * FROM information_schema.columns WHERE table_schema = '????' AND table_name = '${this.versionTableName}'
	}
}
