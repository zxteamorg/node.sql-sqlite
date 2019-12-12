import { CancellationToken, Financial, Logger } from "@zxteam/contract";
import { Disposable, Initable } from "@zxteam/disposable";
import { CancelledError, AggregateError, wrapErrorIfNeeded } from "@zxteam/errors";
import { financial, FinancialOperation } from "@zxteam/financial";
import { HttpClient } from "@zxteam/http-client";
import { DUMMY_CANCELLATION_TOKEN } from "@zxteam/cancellation";
import {
	EmbeddedSqlProviderFactory, SqlData, SqlDialect, SqlProvider,
	SqlStatementParam, SqlResultRecord, SqlStatement, SqlTemporaryTable, SqlError, SqlSyntaxError, SqlConstraintError
} from "@zxteam/sql";

import * as sqlite from "sqlite3";
import * as fs from "fs";
import { promisify } from "util";
import { URL, fileURLToPath, pathToFileURL } from "url";
import { EOL } from "os";

import * as stream from "stream";
import * as readline from "readline";

const existsAsync = promisify(fs.exists);
const readFileAsync = promisify(fs.readFile);

export class SqliteProviderFactory implements EmbeddedSqlProviderFactory {
	private readonly _financialOperation: FinancialOperation;
	private readonly _logger: Logger;
	private readonly _url: URL;

	// This implemenation wrap package https://www.npmjs.com/package/sqlite3
	public constructor(url: URL, opts?: { logger?: Logger, financialOperation?: FinancialOperation }) {
		this._financialOperation = opts !== undefined && opts.financialOperation !== undefined ? opts.financialOperation : financial;
		this._logger = opts !== undefined && opts.logger !== undefined ? opts.logger : new DummyLogger();
		this._url = url;

		this._logger.trace("SqliteProviderFactory Constructed");
	}

	public async create(cancellationToken: CancellationToken): Promise<SqlProvider> {
		this._logger.trace("Inside create() ...");

		if (this._logger.isTraceEnabled) {
			this._logger.trace(`Checking a database file  ${this._url} for existent`);
		}
		const dbPath = fileURLToPath(this._url);
		const isDatabaseFileExists = await existsAsync(dbPath);

		this._logger.trace("Check cancellationToken for interrupt");
		cancellationToken.throwIfCancellationRequested();

		if (!isDatabaseFileExists) {
			if (this._logger.isTraceEnabled) {
				this._logger.trace(`The database file ${this._url} was not found. Raise exception...`);
			}
			throw new Error(`The database file ${this._url} was not found`);
		}

		if (this._logger.isTraceEnabled) {
			this._logger.trace(`Opening the database file  ${this._url}`);
		}
		const underlayingSqliteConnection = await helpers.openDatabase(this._url);
		try {
			this._logger.trace("Check cancellationToken for interrupt");
			cancellationToken.throwIfCancellationRequested();

			const sqlProvider: SqlProvider = new SQLiteProvider(
				underlayingSqliteConnection,
				() => helpers.closeDatabase(underlayingSqliteConnection),
				this._financialOperation,
				this._logger
			);

			if (this._logger.isTraceEnabled) {
				this._logger.trace(`The database file  ${this._url} was opened successfully`);
			}

			return sqlProvider;
		} catch (e) {
			await helpers.closeDatabase(underlayingSqliteConnection);
			throw e;
		}
	}

	public isDatabaseExists(cancellationToken: CancellationToken): Promise<boolean> {
		const dbPath = fileURLToPath(this._url);
		return existsAsync(dbPath);
	}

	public async newDatabase(cancellationToken: CancellationToken, initScriptUrl?: URL): Promise<void> {
		this._logger.trace("Inside newDatabase()");

		if (this._logger.isTraceEnabled) {
			this._logger.trace(`Check is file ${this._url} exists`);
		}

		{ // scope
			const dbPath = fileURLToPath(this._url);
			const isExist = await existsAsync(dbPath);
			if (isExist) {
				if (this._logger.isTraceEnabled) {
					this._logger.trace(`The file ${this._url} already exists. Raise an exception about this problem`);
				}
				throw new Error(`Cannot create new database due the file ${this._url} already exists`);
			}
		}

		this._logger.trace("Check cancellationToken for interrupt");
		cancellationToken.throwIfCancellationRequested();

		this._logger.trace("Open SQLite database for non-exsting file");
		const db = await helpers.openDatabase(this._url);
		try {
			if (initScriptUrl !== undefined) {
				this._logger.trace("initScriptUrl is presented. Let's go to initalizeDatabaseByScript()");
				await helpers.initalizeDatabaseByScript(cancellationToken, initScriptUrl, db, this._logger);
			} else {
				this._logger.trace("initScriptUrl is NOT presented");
			}
		} finally {
			this._logger.trace("Close SQLite database");
			await helpers.closeDatabase(db);
		}

		this._logger.trace("Check cancellationToken for interrupt");
		cancellationToken.throwIfCancellationRequested();

		this._logger.trace("Double check that DB file was created");
		{ // scope
			const dbPath = fileURLToPath(this._url);
			const isExist = await existsAsync(dbPath);
			if (!isExist) {
				if (this._logger.isTraceEnabled) {
					this._logger.trace(`Something went wrong. The DB file ${this._url} still not exists after Open/Close SQLite.`);
				}
				throw new Error("Underlaying library SQLite did not create DB file.");
			}
		}
	}

	public usingProvider<T>(
		cancellationToken: CancellationToken,
		worker: (sqlProvder: SqlProvider) => T | Promise<T>
	): Promise<T> {
		const executionPromise: Promise<T> = (async () => {
			const sqlProvider: SqlProvider = await this.create(cancellationToken);
			try {
				return await worker(sqlProvider);
			} finally {
				await sqlProvider.dispose();
			}
		})();

		// this._activeProviderUsings.add(executionPromise);

		// executionPromise.catch(function () { /* BYPASS */ }).finally(() => {
		// 	this._activeProviderUsings.delete(executionPromise);
		// });

		return executionPromise;
	}

	public usingProviderWithTransaction<T>(
		cancellationToken: CancellationToken, worker: (sqlProvder: SqlProvider) => T | Promise<T>
	): Promise<T> {
		return this.usingProvider(cancellationToken, async (sqlProvider: SqlProvider) => {
			await sqlProvider.statement("BEGIN TRANSACTION").execute(cancellationToken);
			try {
				let result: T;
				const workerResult = worker(sqlProvider);
				if (workerResult instanceof Promise) {
					result = await workerResult;
				} else {
					result = workerResult;
				}
				// We have not to cancel this operation, so pass DUMMY_CANCELLATION_TOKEN
				await sqlProvider.statement("COMMIT TRANSACTION").execute(DUMMY_CANCELLATION_TOKEN);
				return result;
			} catch (e) {
				try {
					// We have not to cancel this operation, so pass DUMMY_CANCELLATION_TOKEN
					await sqlProvider.statement("ROLLBACK TRANSACTION").execute(DUMMY_CANCELLATION_TOKEN);
				} catch (e2) {
					throw new AggregateError([e, e2]);
				}
				throw e;
			}
		});
	}

}

export default SqliteProviderFactory;

class ArgumentError extends Error { }
class InvalidOperationError extends Error { }

class SQLiteProvider extends Disposable implements SqlProvider {
	public readonly dialect: SqlDialect = SqlDialect.SQLite;
	public readonly sqliteConnection: sqlite.Database;
	public readonly financialOperation: FinancialOperation;
	private readonly _log: Logger;
	private readonly _disposer: () => Promise<void>;
	public constructor(sqliteConnection: sqlite.Database, disposer: () => Promise<void>, financialOperation: FinancialOperation, log: Logger) {
		super();
		this.sqliteConnection = sqliteConnection;
		this._disposer = disposer;
		this.financialOperation = financialOperation;
		this._log = log;
		this._log.trace("SQLiteProvider Constructed");
	}

	public statement(sql: string): SQLiteStatement {
		super.verifyNotDisposed();
		if (!sql) { throw new Error("sql"); }
		this._log.trace("Statement: ", sql);
		return new SQLiteStatement(this, sql, this._log);
	}

	public async createTempTable(
		cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string
	): Promise<SqlTemporaryTable> {
		const tempTable = new SQLiteTempTable(this, tableName, columnsDefinitions);
		await tempTable.init(cancellationToken);
		return tempTable;
	}

	public verifyNotDisposed(): void {
		super.verifyNotDisposed();
	}

	protected async onDispose(): Promise<void> {
		this._log.trace("Disposing");
		await this._disposer();
		this._log.trace("Disposed");
	}
}

class SQLiteStatement implements SqlStatement {
	private readonly _log: Logger;
	private readonly _sqlText: string;
	private readonly _owner: SQLiteProvider;

	public constructor(owner: SQLiteProvider, sql: string, logger: Logger) {
		this._owner = owner;
		this._sqlText = sql;
		this._log = logger;
		this._log.trace("SQLiteStatement constructed");
	}

	public async execute(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<void> {
		if (this._log.isTraceEnabled) {
			this._log.trace("Executing Query:", this._sqlText, values);
		}

		this._owner.verifyNotDisposed();
		const underlyingResult = await helpers.sqlRun(
			this._owner.sqliteConnection,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		if (this._log.isTraceEnabled) {
			this._log.trace("Executed Scalar:", underlyingResult);
		}
	}

	public async executeSingle(
		cancellationToken: CancellationToken,
		...values: Array<SqlStatementParam>
	): Promise<SqlResultRecord> {
		if (this._log.isTraceEnabled) {
			this._log.trace("Executing Single:", this._sqlText, values);
		}
		const underlyingResult = await helpers.sqlFetch(
			this._owner.sqliteConnection,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		this._log.trace("Check cancellationToken for interrupt");
		cancellationToken.throwIfCancellationRequested();

		if (this._log.isTraceEnabled) {
			this._log.trace("Executed Scalar:", underlyingResult);
		}

		this._log.trace("Result processing");
		if (underlyingResult.length !== 1) {
			throw new InvalidOperationError("SQL query returns non-single result");
		}
		this._log.trace("Result create new SQLiteSqlResultRecord()");
		return new SQLiteSqlResultRecord(underlyingResult[0], this._owner.financialOperation);
	}

	public async executeQuery(
		cancellationToken: CancellationToken,
		...values: Array<SqlStatementParam>
	): Promise<ReadonlyArray<SqlResultRecord>> {
		if (this._log.isTraceEnabled) {
			this._log.trace("Executing Query:", this._sqlText, values);
		}
		const underlyingResult = await helpers.sqlFetch(
			this._owner.sqliteConnection,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		this._log.trace("Check cancellationToken for interrupt");
		cancellationToken.throwIfCancellationRequested();

		if (this._log.isTraceEnabled) {
			this._log.trace("Executed Scalar:", underlyingResult);
		}

		this._log.trace("Result processing");
		if (underlyingResult.length > 0) {
			this._log.trace("Result create new SQLiteSqlResultRecord()");
			return underlyingResult.map((row) => new SQLiteSqlResultRecord(row, this._owner.financialOperation));
		} else {
			this._log.trace("Result is empty");
			return [];
		}
	}

	// tslint:disable-next-line: max-line-length
	public async executeQueryMultiSets(
		cancellationToken: CancellationToken,
		...values: Array<SqlStatementParam>
	): Promise<ReadonlyArray<ReadonlyArray<SqlResultRecord>>> {
		if (this._log.isTraceEnabled) {
			this._log.trace("Method executeQueryMultiSets not supported. Raise an exception.");
		}
		throw new Error("Not supported");
	}

	public async executeScalar(
		cancellationToken: CancellationToken,
		...values: Array<SqlStatementParam>
	): Promise<SqlData> {
		if (this._log.isTraceEnabled) {
			this._log.trace("Executing Scalar:", this._sqlText, values);
		}

		const underlyingResult = await helpers.sqlFetch(
			this._owner.sqliteConnection,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		this._log.trace("Check cancellationToken for interrupt");
		cancellationToken.throwIfCancellationRequested();

		if (this._log.isTraceEnabled) {
			this._log.trace("Executed Scalar:", underlyingResult);
		}

		this._log.trace("Result processing");
		if (underlyingResult.length > 0) {
			const underlyingResultFirstRow = underlyingResult[0];
			const results = underlyingResultFirstRow[Object.keys(underlyingResultFirstRow)[0]];
			const fields = Object.keys(underlyingResultFirstRow)[0];

			if (this._log.isTraceEnabled) {
				this._log.trace("Create SQLiteData and return result", results, fields);
			}

			return new SQLiteData(results, fields, this._owner.financialOperation);
		} else {
			if (this._log.isTraceEnabled) {
				this._log.trace("Returns not enough data to complete request. Raise an exception.", underlyingResult);
			}
			throw new Error("Underlying SQLite provider returns not enough data to complete request.");
		}
	}

	public async executeScalarOrNull(
		cancellationToken: CancellationToken,
		...values: Array<SqlStatementParam>
	): Promise<SqlData | null> {
		if (this._log.isTraceEnabled) {
			this._log.trace("Executing ScalarOrNull:", this._sqlText, values);
		}

		const underlyingResult = await helpers.sqlFetch(
			this._owner.sqliteConnection,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		this._log.trace("Check cancellationToken for interrupt");
		cancellationToken.throwIfCancellationRequested();

		if (this._log.isTraceEnabled) {
			this._log.trace("Executed Scalar:", underlyingResult);
		}

		this._log.trace("Result processing");
		if (underlyingResult.length > 0) {
			const underlyingResultFirstRow = underlyingResult[0];
			const results = underlyingResultFirstRow[Object.keys(underlyingResultFirstRow)[0]];
			const fields = Object.keys(underlyingResultFirstRow)[0];

			if (this._log.isTraceEnabled) {
				this._log.trace("Create SQLiteData and return result", results, fields);
			}

			return new SQLiteData(results, fields, this._owner.financialOperation);
		} else {
			this._log.trace("Returns no records. Result is null.");
			return null;
		}
	}
}

namespace SQLiteSqlResultRecord {
	export type NameMap = {
		[name: string]: any;
	};
}
class SQLiteSqlResultRecord implements SqlResultRecord {
	private readonly _financialOperation: FinancialOperation;
	private readonly _fieldsData: any;
	private _nameMap?: SQLiteSqlResultRecord.NameMap;

	public constructor(fieldsData: any, financialOperation: FinancialOperation) {
		if (!fieldsData) {
			throw new Error("Internal error. Don't have data");
		}
		this._fieldsData = fieldsData;
		this._financialOperation = financialOperation;
	}

	public get(name: string): SqlData;
	public get(index: number): SqlData;
	public get(nameOrIndex: string | number): SqlData {
		if (typeof nameOrIndex === "string") {
			return this.getByName(nameOrIndex);
		} else {
			return this.getByIndex(nameOrIndex);
		}
	}

	private get nameMap(): SQLiteSqlResultRecord.NameMap {
		if (this._nameMap === undefined) {
			const nameMap: SQLiteSqlResultRecord.NameMap = {};
			const total = Object.keys(this._fieldsData).length;
			for (let index = 0; index < total; ++index) {
				const fi = Object.keys(this._fieldsData)[index];
				if (fi in nameMap) { throw new Error("Cannot access SqlResultRecord by name due result set has name duplicates"); }
				nameMap[fi] = fi;
			}
			this._nameMap = nameMap;
		}
		return this._nameMap;
	}

	private getByIndex(index: number): SqlData {
		const fi = Object.keys(this._fieldsData)[index];
		const value: any = this._fieldsData[fi];
		return new SQLiteData(value, fi, this._financialOperation);
	}
	private getByName(name: string): SqlData {
		const fi = this.nameMap[name];
		const value: any = this._fieldsData[fi];
		return new SQLiteData(value, fi, this._financialOperation);
	}
}

class SQLiteTempTable extends Initable implements SqlTemporaryTable {

	private readonly _owner: SQLiteProvider;
	private readonly _tableName: string;
	private readonly _columnsDefinitions: string;

	public constructor(owner: SQLiteProvider, tableName: string, columnsDefinitions: string) {
		super();
		this._owner = owner;
		this._tableName = tableName;
		this._columnsDefinitions = columnsDefinitions;
	}

	public bulkInsert(cancellationToken: CancellationToken, bulkValues: Array<Array<SqlStatementParam>>): Promise<void> {
		return this._owner.statement(`INSERT INTO temp.${this._tableName}`).execute(cancellationToken, bulkValues as any);
	}
	public clear(cancellationToken: CancellationToken): Promise<void> {
		return this._owner.statement(`DELETE FROM temp.${this._tableName}`).execute(cancellationToken);
	}
	public insert(cancellationToken: CancellationToken, values: Array<SqlStatementParam>): Promise<void> {
		return this._owner.statement(`INSERT INTO temp.${this._tableName}`).execute(cancellationToken, ...values);
	}

	protected async onInit(cancellationToken: CancellationToken): Promise<void> {
		await this._owner.statement(`CREATE TEMPORARY TABLE ${this._tableName} (${this._columnsDefinitions})`).execute(cancellationToken);
	}
	protected async onDispose(): Promise<void> {
		try {
			await this._owner.statement(`DROP TABLE temp.${this._tableName}`).execute(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			// dispose never raise error
			if (e instanceof Error && e.name === "CancelledError") {
				return; // skip error message if task was cancelled
			}
			// Should never happened
			console.error(e); // we cannot do anymore here, just log
		}
	}
}

class SQLiteData implements SqlData {
	private readonly _financialOperation: FinancialOperation;
	private readonly _sqliteValue: any;
	private readonly _fName: string;

	public get asBoolean(): boolean {
		if (typeof this._sqliteValue === "number") {
			return this._sqliteValue !== 0;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableBoolean(): boolean | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (typeof this._sqliteValue === "number") {
			return this._sqliteValue !== 0;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asString(): string {
		if (this._sqliteValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._sqliteValue === "string") {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableString(): string | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (typeof this._sqliteValue === "string") {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asInteger(): number {
		if (this._sqliteValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._sqliteValue === "number" && Number.isInteger(this._sqliteValue)) {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableInteger(): number | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (typeof this._sqliteValue === "number" && Number.isInteger(this._sqliteValue)) {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNumber(): number {
		if (this._sqliteValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._sqliteValue === "number") {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableNumber(): number | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (typeof this._sqliteValue === "number") {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asFinancial(): Financial {
		if (this._sqliteValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._sqliteValue === "number") {
			return this._financialOperation.fromFloat(this._sqliteValue);
		} else if (typeof this._sqliteValue === "string") {
			return this._financialOperation.parse(this._sqliteValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableFinancial(): Financial | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (typeof this._sqliteValue === "number") {
			return this._financialOperation.fromFloat(this._sqliteValue);
		} else if (typeof this._sqliteValue === "string") {
			return this._financialOperation.parse(this._sqliteValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asDate(): Date {
		if (typeof this._sqliteValue === "number" || typeof this._sqliteValue === "string") {
			try {
				return new Date(this._sqliteValue);
			} catch (e) {
				throw new InvalidOperationError(this.formatWrongDataTypeMessage(e));
			}
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableDate(): Date | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (typeof this._sqliteValue === "number" || typeof this._sqliteValue === "string") {
			try {
				return new Date(this._sqliteValue);
			} catch (e) {
				throw new InvalidOperationError(this.formatWrongDataTypeMessage(e));
			}
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asBinary(): Uint8Array {
		if (this._sqliteValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (this._sqliteValue instanceof Uint8Array) {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableBinary(): Uint8Array | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (this._sqliteValue instanceof Uint8Array) {
			return this._sqliteValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}

	public constructor(sqliteValue: any, fName: string, financialOperation: FinancialOperation) {
		if (sqliteValue === undefined) {
			throw new ArgumentError("sqlite Value");
		}
		this._sqliteValue = sqliteValue;
		this._fName = fName;
		this._financialOperation = financialOperation;
	}

	private formatWrongDataTypeMessage(err?: any): string {
		const text = `Invalid conversion: requested wrong data type of field '${this._fName}'`;
		const message = (err) ? `Error: ${err} ` + text : text;
		return message;
	}
}

class DummyLogger implements Logger {
	public getLogger(name?: string | undefined): Logger { return this; }

	public get isTraceEnabled(): boolean { return false; }
	public get isDebugEnabled(): boolean { return false; }
	public get isInfoEnabled(): boolean { return false; }
	public get isWarnEnabled(): boolean { return false; }
	public get isErrorEnabled(): boolean { return false; }
	public get isFatalEnabled(): boolean { return false; }

	public trace(message: string, ...args: any[]): void {
		// dummy
	}
	public debug(message: string, ...args: any[]): void {
		// dummy
	}
	public info(message: string, ...args: any[]): void {
		// dummy
	}
	public warn(message: string, ...args: any[]): void {
		// dummy
	}
	public error(message: string, ...args: any[]): void {
		// dummy
	}
	public fatal(message: string, ...args: any[]): void {
		// dummy
	}
}

namespace helpers {
	export function openDatabase(filename: URL): Promise<sqlite.Database> {
		return new Promise((resolve, reject) => {
			let db: sqlite.Database;
			const fullPathDb = fileURLToPath(filename);
			db = new sqlite.Database(fullPathDb, (error) => {
				if (error) { return reject(error); }
				return resolve(db);
			});
		});
	}
	export function closeDatabase(db: sqlite.Database): Promise<void> {
		return new Promise((resolve, reject) => {
			db.close((error) => {
				if (error) { return reject(error); }
				return resolve();
			});
		});
	}
	export function sqlRun(instansDb: sqlite.Database, sql: string, params?: Array<any>): Promise<sqlite.RunResult> {
		return new Promise((resolve, reject) => {
			try {
				const [friendlySql, friendlyParams] = unwrapSqlAndParams(sql, params);
				instansDb.run(friendlySql, friendlyParams, function (error) {
					if (error) {
						return reject(errorWrapper(error));
					}
					return resolve(this);
				});
			} catch (e) {
				return reject(errorWrapper(e));
			}
		});
	}
	export function sqlFetch(instansDb: sqlite.Database, sql: string, params?: Array<any>): Promise<Array<any>> {
		return new Promise((resolve, reject) => {
			try {
				const [friendlySql, friendlyParams] = unwrapSqlAndParams(sql, params);
				instansDb.all(friendlySql, friendlyParams, (error, rows) => {
					if (error) {
						return reject(errorWrapper(error));
					}
					return resolve(rows);
				});
			} catch (e) {
				return reject(errorWrapper(e));
			}
		});
	}
	export function statementArgumentsAdapter(financialOperation: FinancialOperation, args: Array<SqlStatementParam>): Array<any> {
		return args.map(value => {
			if (typeof value === "object") {
				if (value !== null && financialOperation.isFinancial(value)) {
					return value.toString(); // Financial should be converted to string (SQLite know nothing about)
				}
			}
			return value;
		});
	}
	export async function initalizeDatabaseByScript(
		cancellationToken: CancellationToken, initScriptUrl: URL, db: sqlite.Database, logger: Logger
	) {
		if (initScriptUrl !== undefined) {
			logger.trace("Loading script...");
			const sqlCommands = await loadScriptAndParseScript(cancellationToken, initScriptUrl);

			if (sqlCommands.length === 0) {
				logger.trace("File init script do not have sql commands");
				return;
			}

			logger.trace("Check cancellationToken for interrupt");
			cancellationToken.throwIfCancellationRequested();

			for (let i = 0; i < sqlCommands.length; i++) {
				const command = sqlCommands[i];

				if (logger.isTraceEnabled) {
					logger.trace("Execute sql script: ", command);
				}

				await helpers.sqlRun(db, command);

				logger.trace("Check cancellationToken for interrupt");
				cancellationToken.throwIfCancellationRequested();
			}
		}
	}
	function unwrapSqlAndParams(sql: string, params?: Array<any>): [string, Array<any>] {
		let finalSql = "";
		let searchStart = 0;
		let paramCount = 0;
		const unwrappedParams: Array<any> = [];
		while (true) {
			const position = sql.indexOf("?", searchStart);
			if (position === -1) {
				finalSql += sql.substr(searchStart);
				return [finalSql, unwrappedParams];
			} else {
				if (params === undefined) { break; }
				if (paramCount >= params.length) { break; }
				const param = params[paramCount];
				if (Array.isArray(param)) {
					finalSql += sql.substr(searchStart, position);
					finalSql += param.map(() => "?").join(",");
					unwrappedParams.push(...param);
				} else {
					finalSql += sql.substr(searchStart, ((position + 1) - searchStart));
					unwrappedParams.push(param);
				}
				paramCount += 1;
				searchStart = position + 1;
			}
		}
		throw new Error(`Cannot unwrap query: ${sql}`);

	}

	async function loadScriptAndParseScript(cancellationToken: CancellationToken, urlPath: URL): Promise<Array<string>> {
		const sqlScriptContent = await loadScript(cancellationToken, urlPath);
		const sqlCommands = await splitScriptToStatements(cancellationToken, sqlScriptContent);
		return sqlCommands;
	}
	function loadScript(cancellationToken: CancellationToken, urlPath: URL): Promise<string> {
		if (urlPath.protocol === "file:") {
			const initScriptPath = fileURLToPath(urlPath);
			return loadScriptFromFile(cancellationToken, initScriptPath);
		} else if (urlPath.protocol === "http:" || urlPath.protocol === "https:") {
			return loadScriptFromWeb(cancellationToken, urlPath);
		}
		throw new Error(`Do not support this protocol: ${urlPath.protocol}`);
	}
	async function loadScriptFromFile(cancellationToken: CancellationToken, filename: string): Promise<string> {
		return readFileAsync(filename, "utf8");
	}
	async function loadScriptFromWeb(cancellationToken: CancellationToken, urlPath: URL): Promise<string> {
		const httpClient = new HttpClient();
		const invokeResponse = await httpClient.invoke(cancellationToken, { url: urlPath, method: "GET" });

		if (invokeResponse.statusCode !== 200) {
			throw new Error(`Cannot read remote SQL script. Unsuccessful operation code status ${invokeResponse.statusCode}`);
		}

		return invokeResponse.body.toString("utf8");
	}
	async function splitScriptToStatements(cancellationToken: CancellationToken, sqlScriptContent: string): Promise<Array<string>> {
		const allCommands: Array<string> = [];
		const sqlScriptContentStream = new StringReadable(sqlScriptContent);
		const readlineInterface = readline.createInterface({
			input: sqlScriptContentStream,
			crlfDelay: Infinity
		});

		let statement = "";
		readlineInterface.on("line", function (line: string) {
			if (line.startsWith("--")) {
				if (line.startsWith("-- GO")) {
					const trimmedStatement = statement.trim();
					if (trimmedStatement.length > 0) {
						allCommands.push(statement);
					}
					statement = "";
				}
			} else {
				if (statement.length > 0) {
					statement = `${statement}${EOL}${line}`;
				} else {
					statement = `${statement}${line}`;
				}
			}
		});

		await new Promise(function (resolve, reject) {
			function complete() {
				resolve();
				cancellationToken.removeCancelListener(cancel);
				readlineInterface.removeListener("close", complete);
			}
			function cancel() {
				reject(new CancelledError());
				cancellationToken.removeCancelListener(cancel);
				readlineInterface.removeListener("close", complete);
			}

			readlineInterface.on("close", complete);
			cancellationToken.addCancelListener(cancel);
		});

		if (statement.length > 0) {
			allCommands.push(statement);
		}

		return allCommands;
	}
	function errorWrapper(errorLike: any): SqlError {
		if (errorLike instanceof SqlError) { return errorLike; }

		const err = wrapErrorIfNeeded(errorLike);

		if (err.message.includes("SQLITE_ERROR")) {
			if (err.message.includes("syntax error")) {
				return new SqlSyntaxError(`Looks like wrong SQL syntax detected: ${err.message}. See innerError for details.`, err);
			}
		}

		if (err.message.includes("SQLITE_CONSTRAINT")) {
			return new SqlConstraintError(`SQL Constrain restriction happened: ${err.message}`, "???", err);
		}

		return new SqlError(`Unexpected error: ${err.message}`, err);
	}

	class StringReadable extends stream.Readable {
		private _data: Buffer | null;

		public constructor(str: string) {
			super();
			this._data = Buffer.from(str);
		}

		public _read(size: number): void {
			if (this._data !== null) {
				const chunk: Buffer = this._data.slice(0, size);
				this._data = this._data.slice(chunk.length);
				this.push(chunk);
				if (this._data.length === 0) {
					this._data = null;
				}
			} else {
				this.push(null);
			}
		}
	}
}
