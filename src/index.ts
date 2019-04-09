
import { Logger, CancellationToken, Task as TaskLike, Financial as FinancialLike } from "@zxteam/contract";
import { Disposable, Initable } from "@zxteam/disposable";
import { financial, Financial } from "@zxteam/financial.js";
import { WebClient } from "@zxteam/webclient";
import { Task, WrapError } from "ptask.js";
import * as sqlite from "sqlite3";
import * as fs from "fs";
import { promisify } from "util";
import { URL, fileURLToPath, pathToFileURL } from "url";

import * as contract from "@zxteam/contract.sql";

const existsAsync = promisify(fs.exists);
const readFileAsync = promisify(fs.readFile);

const FINACIAL_NUMBER_DEFAULT_FRACTION = 12;

export class SqliteProviderFactory implements contract.EmbeddedSqlProviderFactory {
	private readonly _logger: Logger;
	private readonly _url: URL;

	// This implemenation wrap package https://www.npmjs.com/package/sqlite3
	public constructor(url: URL, logger?: Logger) {
		this._logger = logger || new DummyLogger();
		this._url = url;

		this._logger.trace("SqliteProviderFactory Constructed");
	}

	public create(cancellationToken?: CancellationToken): Task<contract.SqlProvider> {
		return Task.run(async (ct) => {
			this._logger.trace("Inside create() ...");

			if (this._logger.isTraceEnabled) {
				this._logger.trace(`Checking a database file  ${this._url} for existent`);
			}
			const dbPath = fileURLToPath(this._url);
			const isDatabaseFileExists = await existsAsync(dbPath);

			this._logger.trace("Check cancellationToken for interrupt");
			ct.throwIfCancellationRequested();

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
				ct.throwIfCancellationRequested();

				const sqlProvider: contract.SqlProvider = new SQLiteProvider(
					underlayingSqliteConnection,
					() => helpers.closeDatabase(underlayingSqliteConnection),
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
		}, cancellationToken);
	}

	public isDatabaseExists(cancellationToken: CancellationToken): Task<boolean> {
		return Task.run(() => { // scope
			const dbPath = fileURLToPath(this._url);
			return existsAsync(dbPath);
		});
	}

	public newDatabase(cancellationToken: CancellationToken, initScriptUrl?: URL): Task<void> {
		return Task.run(async () => {
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
		});
	}
}

export default SqliteProviderFactory;

class ArgumentError extends Error { }
class InvalidOperationError extends Error { }

class SQLiteProvider extends Disposable implements contract.SqlProvider {
	public readonly sqliteConnection: sqlite.Database;
	private readonly _log: Logger;
	private readonly _disposer: () => Promise<void>;
	public constructor(sqliteConnection: sqlite.Database, disposer: () => Promise<void>, log: Logger) {
		super();
		this.sqliteConnection = sqliteConnection;
		this._disposer = disposer;
		this._log = log;
		this._log.trace("SQLiteProvider Constructed");
	}

	public statement(sql: string): SQLiteStatement {
		super.verifyNotDisposed();
		if (!sql) { throw new Error("sql"); }
		this._log.trace("Statement: ", sql);
		return new SQLiteStatement(this, sql, this._log);
	}

	// tslint:disable-next-line:max-line-length
	public createTempTable(cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string): TaskLike<contract.SqlTemporaryTable> {
		return Task.run(async (ct) => {
			const tempTable = new SQLiteTempTable(this, ct, tableName, columnsDefinitions);
			await tempTable.init();
			return tempTable;
		}, cancellationToken || undefined);
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

class SQLiteStatement implements contract.SqlStatement {
	private readonly _log: Logger;
	private readonly _sqlText: string;
	private readonly _owner: SQLiteProvider;

	public constructor(owner: SQLiteProvider, sql: string, logger: Logger) {
		this._owner = owner;
		this._sqlText = sql;
		this._log = logger;
		this._log.trace("SQLiteStatement constructed");
	}

	public execute(cancellationToken: CancellationToken, ...values: Array<contract.SqlStatementParam>): Task<void> {
		return Task.run(async () => {
			if (this._log.isTraceEnabled) {
				this._log.trace("Executing Query:", this._sqlText, values);
			}

			this._owner.verifyNotDisposed();
			const underlyingResult = await helpers.sqlRun(
				this._owner.sqliteConnection,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);

			if (this._log.isTraceEnabled) {
				this._log.trace("Executed Scalar:", underlyingResult);
			}
		}, cancellationToken);
	}

	public executeQuery(
		cancellationToken: CancellationToken,
		...values: Array<contract.SqlStatementParam>
	): Task<Array<contract.SqlResultRecord>> {
		return Task.run(async (ct: CancellationToken) => {
			if (this._log.isTraceEnabled) {
				this._log.trace("Executing Query:", this._sqlText, values);
			}
			const underlyingResult = await helpers.sqlFetch(
				this._owner.sqliteConnection,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);

			this._log.trace("Check cancellationToken for interrupt");
			ct.throwIfCancellationRequested();

			if (this._log.isTraceEnabled) {
				this._log.trace("Executed Scalar:", underlyingResult);
			}

			this._log.trace("Result processing");
			if (underlyingResult.length > 0) {
				this._log.trace("Result create new SQLiteSqlResultRecord()");
				return underlyingResult.map((row) => new SQLiteSqlResultRecord(row));
			} else {
				this._log.trace("Result is empty");
				return [];
			}
		}, cancellationToken);
	}

	// tslint:disable-next-line: max-line-length
	public executeQueryMultiSets(cancellationToken: CancellationToken, ...values: Array<contract.SqlStatementParam>): Task<Array<Array<contract.SqlResultRecord>>> {
		return Task.run((ct: CancellationToken) => {
			if (this._log.isTraceEnabled) {
				this._log.trace("Method executeQueryMultiSets not supported. Raise an exception.");
			}
			throw new Error("Not supported");
		}, cancellationToken);
	}

	public executeScalar(cancellationToken: CancellationToken, ...values: Array<contract.SqlStatementParam>): Task<contract.SqlData> {
		return Task.run(async (ct: CancellationToken) => {
			if (this._log.isTraceEnabled) {
				this._log.trace("Executing Scalar:", this._sqlText, values);
			}

			const underlyingResult = await helpers.sqlFetch(
				this._owner.sqliteConnection,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);

			this._log.trace("Check cancellationToken for interrupt");
			ct.throwIfCancellationRequested();

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

				return new SQLiteData(results, fields);
			} else {
				if (this._log.isTraceEnabled) {
					this._log.trace("Returns not enough data to complete request. Raise an exception.", underlyingResult);
				}
				throw new Error("Underlying SQLite provider returns not enough data to complete request.");
			}
		}, cancellationToken);
	}
}

namespace SQLiteSqlResultRecord {
	export type NameMap = {
		[name: string]: any;
	};
}
class SQLiteSqlResultRecord implements contract.SqlResultRecord {
	private readonly _fieldsData: any;
	private _nameMap?: SQLiteSqlResultRecord.NameMap;

	public constructor(fieldsData: any) {
		if (!fieldsData) {
			throw new Error("Internal error. Don't have data");
		}
		this._fieldsData = fieldsData;
	}

	public get(name: string): contract.SqlData;
	public get(index: number): contract.SqlData;
	public get(nameOrIndex: string | number): contract.SqlData {
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

	private getByIndex(index: number): contract.SqlData {
		const fi = Object.keys(this._fieldsData)[index];
		const value: any = this._fieldsData[fi];
		return new SQLiteData(value, fi);
	}
	private getByName(name: string): contract.SqlData {
		const fi = this.nameMap[name];
		const value: any = this._fieldsData[fi];
		return new SQLiteData(value, fi);
	}
}

class SQLiteTempTable extends Initable implements contract.SqlTemporaryTable {

	private readonly _owner: SQLiteProvider;
	private readonly _cancellationToken: CancellationToken;
	private readonly _tableName: string;
	private readonly _columnsDefinitions: string;

	public constructor(owner: SQLiteProvider, cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string) {
		super();
		this._owner = owner;
		this._cancellationToken = cancellationToken;
		this._tableName = tableName;
		this._columnsDefinitions = columnsDefinitions;
	}

	public bulkInsert(cancellationToken: CancellationToken, bulkValues: Array<Array<contract.SqlStatementParam>>): TaskLike<void> {
		return this._owner.statement(`INSERT INTO temp.${this._tableName}`).execute(cancellationToken, bulkValues as any);
	}
	public crear(cancellationToken: CancellationToken): TaskLike<void> {
		return this._owner.statement(`DELETE FROM temp.${this._tableName}`).execute(cancellationToken);
	}
	public insert(cancellationToken: CancellationToken, values: Array<contract.SqlStatementParam>): Task<void> {
		return this._owner.statement(`INSERT INTO temp.${this._tableName}`).execute(cancellationToken, ...values);
	}

	protected async onInit(): Promise<void> {
		await this._owner.statement(`CREATE TEMPORARY TABLE ${this._tableName} (${this._columnsDefinitions})`).execute(this._cancellationToken);
	}
	protected async onDispose(): Promise<void> {
		try {
			await this._owner.statement(`DROP TABLE temp.${this._tableName}`).execute(this._cancellationToken);
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

class SQLiteData implements contract.SqlData {
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
	public get asFinancial(): FinancialLike {
		if (this._sqliteValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._sqliteValue === "number") {
			return financial(this._sqliteValue, FINACIAL_NUMBER_DEFAULT_FRACTION);
		} else if (typeof this._sqliteValue === "string") {
			return financial(this._sqliteValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableFinancial(): FinancialLike | null {
		if (this._sqliteValue === null) {
			return null;
		} else if (typeof this._sqliteValue === "number") {
			return financial(this._sqliteValue, FINACIAL_NUMBER_DEFAULT_FRACTION);
		} else if (typeof this._sqliteValue === "string") {
			return financial(this._sqliteValue);
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

	public constructor(sqliteValue: any, fName: string) {
		if (sqliteValue === undefined) {
			throw new ArgumentError("sqlite Value");
		}
		this._sqliteValue = sqliteValue;
		this._fName = fName;
	}

	private formatWrongDataTypeMessage(err?: any): string {
		const text = `Invalid conversion: requested wrong data type of field '${this._fName}'`;
		const message = (err) ? `Error: ${err} ` + text : text;
		return message;
	}
}

class DummyLogger implements Logger {
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
				instansDb.run(sql, params, function (error) {
					if (error) {
						return reject(error);
					}
					return resolve(this);
				});
			} catch (e) {
				return reject(e);
			}
		});
	}
	export function sqlFetch(instansDb: sqlite.Database, sql: string, params?: Array<any>): Promise<Array<any>> {
		return new Promise((resolve, reject) => {
			try {
				instansDb.all(sql, params, (error, rows) => {
					if (error) {
						return reject(error);
					}
					return resolve(rows);
				});
			} catch (e) {
				return reject(e);
			}
		});
	}
	export function statementArgumentsAdapter(args: Array<contract.SqlStatementParam>): Array<any> {
		return args.map(value => {
			if (typeof value === "object") {
				if (value !== null && Financial.isFinancialLike(value)) {
					return Financial.toString(value); // Financial should be converted to string (SQLite know nothing about)
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
	async function loadScriptAndParseScript(cancellationToken: CancellationToken, urlPath: URL): Promise<Array<string>> {
		const sqlScriptContent = await loadScript(cancellationToken, urlPath);
		const sqlCommands = parseCommands(sqlScriptContent);
		return sqlCommands;
	}
	function loadScript(cancellationToken: CancellationToken, urlPath: URL): Promise<string> {
		if (urlPath.protocol === "file:") {
			const initScriptPath = fileURLToPath(urlPath);
			return loadScriptFromFile(cancellationToken, initScriptPath);
		} else if (urlPath.protocol === "http:") {
			return loadScriptFromHttp(cancellationToken, urlPath);
		}
		throw new Error(`Do not support this protocol: ${urlPath.protocol}`);
	}
	async function loadScriptFromFile(cancellationToken: CancellationToken, filename: string): Promise<string> {
		return readFileAsync(filename, "utf8");
	}
	async function loadScriptFromHttp(cancellationToken: CancellationToken, urlPath: URL): Promise<string> {
		const webClient = new WebClient();
		const invokeResponse = await webClient.invoke(cancellationToken, { url: urlPath, method: "GET" });

		if (invokeResponse.statusCode !== 200) {
			throw new Error(`Cannot read remote SQL script. Unsuccessful operation code status ${invokeResponse.statusCode}`);
		}

		return invokeResponse.body.toString("utf8");
	}
	function parseCommands(commands: string): Array<string> {
		const lines = commands.split("\n");
		let allCommands = [];
		let command = "";
		for (let i = 0; i < lines.length; i++) {
			const line = lines[i];
			if (!((!line) || line.startsWith("--"))) {
				command += line;
				if (command.endsWith(";")) {
					allCommands.push(command);
					command = "";
				}
			}
		}
		return allCommands;
	}
}
