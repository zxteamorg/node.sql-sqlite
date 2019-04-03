
import { Factory, Logger, CancellationToken, Task as TaskLike, Financial as FinancialLike } from "@zxteam/contract";
import {
	SqlProvider, SqlStatement, SqlStatementParam, SqlResultRecord, SqlData, SqlTemporaryTable
} from "@zxteam/contract.sql";
import { Disposable, Initable } from "@zxteam/disposable";
import { financial } from "@zxteam/financial.js";
import { Task, CancelledError } from "ptask.js";
import * as sqlite from "sqlite3";
const fs = require("fs");

const FINACIAL_NUMBER_DEFAULT_FRACTION = 12;

function sqliteRunScript(instansDb: sqlite.Database, sql: string, params?: Array<any>): Promise<sqlite.RunResult> {
	return new Promise((resolve, reject) => {
		try {
			instansDb.run(sql, params, function (error) {
				if (error) {
					reject(error);
					return;
				}
				resolve(this);
			});
		} catch (e) {
			reject(e);
		}
	});
}
function sqliteAllScript(instansDb: sqlite.Database, sql: string, params?: Array<any>): Promise<Array<any>> {
	return new Promise((resolve, reject) => {
		try {
			instansDb.all(sql, params, (error, rows) => {
				if (error) {
					reject(error);
					return;
				}
				resolve(rows);
			});
		} catch (e) {
			reject(e);
		}
	});
}

enum SqliteOrNull { sqlite, null }

export class SQLiteProviderFactory implements Factory<SqlProvider> {
	private readonly _logger: Logger;
	private readonly _fullPathDb: string;

	private _providesCount: number;
	private _sqliteConnection: sqlite.Database | null;

	// This implemenation wrap package https://www.npmjs.com/package/sqlite3
	public constructor(opts: { fullPathDb: string, logger?: Logger }) {
		this._sqliteConnection = null;
		this._providesCount = 0;
		this._logger = opts.logger || new DummyLogger();
		this._fullPathDb = opts.fullPathDb;

		this._logger.trace("SQLiteProviderFactory Constructed");
	}

	public create(cancellationToken?: CancellationToken): Task<SqlProvider> {
		const disposer = (connection: sqlite.Database): Promise<void> => {
			connection.close((error) => {
				if (error) {
					Promise.reject(error);
					return;
				}
				Promise.resolve();
			});
			return Promise.resolve();
		};

		return Task.run((ct) => new Promise<SqlProvider>((resolve, reject) => {
			this._logger.trace("Creating SQLite SqlProvider..");

			if (ct.isCancellationRequested) { return reject(new CancelledError()); }


			if (!fs.existsSync(this._fullPathDb)) {
				throw new Error(`Don't exist file database ${this._fullPathDb}`);
			}
			if (this._sqliteConnection === null) {
				const sqlite3 = sqlite.verbose();
				this._sqliteConnection = new sqlite3.Database(this._fullPathDb, (error) => {
					if (error) {
						reject(error);
						return;
					}
				});
			}
			if (ct.isCancellationRequested) { return reject(new CancelledError()); }
			try {
				this._logger.trace("Created SQLite SqlProvider");
				// const dbSqlite = this._sqliteConnection;
				if (this._sqliteConnection === null) { throw new Error("Don't have database Sqlite"); }
				const sqlProvider: SqlProvider = new SQLiteProvider(this._sqliteConnection, this._logger);
				this._logger.trace("Created SQLite SqlProvider");
				return resolve(sqlProvider);
			} catch (e) {
				this._sqliteConnection.close((error) => {
					if (error) {
						Promise.reject(error);
						return;
					}
					Promise.resolve();
				});
				this._logger.trace("Failed to create SQLite SqlProvider", e);
				return reject(e);
			}
		}), cancellationToken);
	}
}

export default SQLiteProviderFactory;

class ArgumentError extends Error { }
class InvalidOperationError extends Error { }

class SQLiteProvider extends Disposable implements SqlProvider {
	public readonly sqliteConnection: sqlite.Database;
	private readonly _log: Logger;
	private readonly _disposer: () => Promise<void>;
	public constructor(sqliteConnection: sqlite.Database, log: Logger) {
		super();
		this.sqliteConnection = sqliteConnection;
		// this._disposer = disposer;
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
	public createTempTable(cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string): TaskLike<SqlTemporaryTable> {
		return Task.run(async (ct) => {
			const tempTable = new SQLiteTempTable(this, ct, tableName, columnsDefinitions);
			await tempTable.init();
			return tempTable;
		}, cancellationToken || undefined);
	}

	protected async onDispose(): Promise<void> {
		this._log.trace("Disposing");
		// await this._disposer();
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

	public execute(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<void> {
		return Task.run((ct: CancellationToken) => {
			return new Promise<void>((resolve, reject) => {
				if (this._log.isTraceEnabled) {
					this._log.trace("Executing Query:", this._sqlText, values);
				}
				sqliteRunScript(this._owner.sqliteConnection, this._sqlText, values).then((underlyingResult) => {
					if (this._log.isTraceEnabled) {
						this._log.trace("Executed Scalar:", underlyingResult);
					}
					return resolve();
				}).catch((err) => {
					if (err) {
						if (this._log.isTraceEnabled) {
							this._log.trace("Executed Scalar with error:", err);
						}
						return reject(err);
					}
				});
			});
		}, cancellationToken);
	}

	public executeQuery(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<Array<SqlResultRecord>> {
		return Task.run((ct: CancellationToken) => {
			return new Promise<Array<SqlResultRecord>>((resolve, reject) => {
				if (this._log.isTraceEnabled) {
					this._log.trace("Executing Query:", this._sqlText, values);
				}
				sqliteAllScript(this._owner.sqliteConnection, this._sqlText, values).then((underlyingResult) => {
					if (this._log.isTraceEnabled) {
						this._log.trace("Executed Scalar:", underlyingResult);
					}
					if (underlyingResult.length > 0) {
						const resArray: Array<SqlResultRecord> = [];
						underlyingResult.forEach((row) => {
							const rowz = new SQLiteSqlResultRecord(row);
							resArray.push(rowz);
						});
						return resolve(resArray);
						// return resolve(underlyingResult.map(row => new SQLiteSqlResultRecord(row)));
					} else {
						return resolve([]);
					}
				}).catch((err) => {
					if (err) {
						if (this._log.isTraceEnabled) {
							this._log.trace("Executed Scalar with error:", err);
						}
						return reject(err);
					}
				});
			});
		}, cancellationToken);
	}

	// tslint:disable-next-line: max-line-length
	public executeQueryMultiSets(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<Array<Array<SqlResultRecord>>> {
		return Task.run((ct: CancellationToken) => {
			return new Promise<Array<Array<SqlResultRecord>>>((resolve, reject) => {
				throw new Error("Don't support yet!");
			});
		}, cancellationToken);
	}

	public executeScalar(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<SqlData> {
		return Task.run((ct: CancellationToken) => {
			return new Promise<SqlData>((resolve, reject) => {
				if (this._log.isTraceEnabled) {
					this._log.trace("Executing Scalar:", this._sqlText, values);
				}
				sqliteAllScript(this._owner.sqliteConnection, this._sqlText, values).then((underlyingResult) => {
					if (this._log.isTraceEnabled) {
						this._log.trace("Executed Scalar:", underlyingResult);
					}
					if (underlyingResult.length > 0) {
						const underlyingResultFirstRow = underlyingResult[0];
						const results = underlyingResultFirstRow[Object.keys(underlyingResultFirstRow)[0]];
						const fields = Object.keys(underlyingResultFirstRow)[0];
						return resolve(new SQLiteData(results, fields));
					}
					return reject(new Error("Underlying SQLite provider returns not enough data to complete request."));
				}).catch((err) => {
					if (err) {
						if (this._log.isTraceEnabled) {
							this._log.trace("Executed Scalar with error:", err);
						}
						return reject(err);
					}
				});
			});
		}, cancellationToken);
	}
}

namespace SQLiteSqlResultRecord {
	export type NameMap = {
		[name: string]: any;
	};
}
class SQLiteSqlResultRecord implements SqlResultRecord {
	private readonly _fieldsData: any;
	private _nameMap?: SQLiteSqlResultRecord.NameMap;

	public constructor(fieldsData: any) {
		if (!fieldsData) {
			throw new Error("Internal error. Don't have data");
		}
		this._fieldsData = fieldsData;
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
		return new SQLiteData(value, fi);
	}
	private getByName(name: string): SqlData {
		const fi = this.nameMap[name];
		const value: any = this._fieldsData[fi];
		return new SQLiteData(value, fi);
	}
}

class SQLiteTempTable extends Initable implements SqlTemporaryTable {

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

	public bulkInsert(cancellationToken: CancellationToken, bulkValues: Array<Array<SqlStatementParam>>): TaskLike<void> {
		return this._owner.statement(`INSERT INTO temp.${this._tableName}`).execute(cancellationToken, bulkValues as any);
	}
	public crear(cancellationToken: CancellationToken): TaskLike<void> {
		return this._owner.statement(`DELETE FROM temp.${this._tableName}`).execute(cancellationToken);
	}
	public insert(cancellationToken: CancellationToken, values: Array<SqlStatementParam>): Task<void> {
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

class SQLiteData implements SqlData {
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
