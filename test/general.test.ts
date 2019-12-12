import * as chai from "chai";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { URL, fileURLToPath, pathToFileURL } from "url";

import { CancellationToken } from "@zxteam/contract";
import { financial } from "@zxteam/financial";
import ensureFactory from "@zxteam/ensure";
import { SqlProvider, EmbeddedSqlProviderFactory, SqlSyntaxError, SqlConstraintError } from "@zxteam/sql";

import * as lib from "../src";

declare global {
	namespace Chai {
		interface Assert {
			equalBytes(val: Uint8Array, exp: Uint8Array, msg?: string): void;
		}
	}
}

chai.use(require("chai-datetime"));
chai.use(function (c, u) {
	const a = c.assert;
	a.equalBytes = function (actual: Uint8Array, expected: Uint8Array, msg?: string) {
		const message = (msg === null || msg === undefined) ?
			("expected " + actual.toString() + " to equal " + expected.toString())
			: msg;
		assert.equal(actual.length, expected.length, message);
		const len = actual.length;
		for (let index = 0; index < len; ++index) {
			const actualPart = actual[index];
			const expectedPart = expected[index];
			assert.equal(actualPart, expectedPart, message);
		}
	};
});

const { assert } = chai;

const ensureTestDbUrl = ensureFactory((message, data) => { throw new Error(`Unexpected value of TEST_DB_URL. ${message}`); });

const DUMMY_CANCELLATION_TOKEN: CancellationToken = {
	get isCancellationRequested(): boolean { return false; },
	addCancelListener(cb: Function): void { /* STUB */ },
	removeCancelListener(cb: Function): void { /* STUB */ },
	throwIfCancellationRequested(): void { /* STUB */ }
};

function getSQLiteUrltoDb(): URL {
	const tmpDirectory = os.tmpdir();
	const pathToDB = path.join(tmpDirectory, "sqlite.db");
	const urlToDB = pathToFileURL(pathToDB);
	return urlToDB;
}
function getSQLiteUrltoSqlFile(): URL {
	const pathToSqlScript = path.join(__dirname, "general.test.sql");
	const urlToDB = pathToFileURL(pathToSqlScript);
	return urlToDB;
}


describe("SQLite Tests", function () {
	let sqlProviderFactory: lib.SqliteProviderFactory;
	let sqlProvider: SqlProvider | null;

	function getSqlProvider(): SqlProvider {
		if (!sqlProvider) { throw new Error(); }
		return sqlProvider;
	}

	before(async function () {
		// runs before all tests in this block

		// Uncomment rows below to enable trace log
		/*
		configure({
			appenders: {
				out: { type: "console" }
			},
			categories: {
				default: { appenders: ["out"], level: "trace" }
			}
		});
		*/

		const pathTodb = fileURLToPath(getSQLiteUrltoDb());
		if (fs.existsSync(pathTodb)) {
			fs.unlinkSync(pathTodb);
		}

		sqlProviderFactory = new lib.SqliteProviderFactory(getSQLiteUrltoDb());
		await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN, getSQLiteUrltoSqlFile());
	});

	beforeEach(async function () {
		// runs before each test in this block
		sqlProvider = await sqlProviderFactory.create(DUMMY_CANCELLATION_TOKEN);
	});
	afterEach(async function () {
		// runs after each test in this block
		if (sqlProvider) {
			await sqlProvider.dispose();
			sqlProvider = null;
		}
	});

	it.skip("Read TRUE from multi record set through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("CALL sp_multi_fetch_ints()")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, true);
	});
	it("Read TRUE as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 1 AS c0, 0 AS c1 UNION ALL SELECT 0, 0")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, true);
	});
	it("Read 1 as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 1 AS c0, 0 AS c1 UNION ALL SELECT 0, 0")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, true);
	});
	it("Read 1 as boolean through executeScalar (Stored Procedure)", async function () {
		const result = await getSqlProvider()
			.statement("SELECT EXISTS(SELECT 1 FROM tb_1 AS t WHERE t.varcharValue = 'one') AS \"is_exist\"")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, true);
	});
	it("Read 0 as boolean through executeScalar (Stored Procedure)", async function () {
		const result = await getSqlProvider()
			.statement("SELECT EXISTS(SELECT 1 FROM tb_1 AS t WHERE t.varcharValue = 'none') AS \"is_exist\"")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, false);
	});
	it("Read FALSE as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT FALSE AS c0, TRUE AS c1 UNION ALL SELECT TRUE, TRUE")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, false);
	});
	it("Read 0 as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 0 AS c0, 1 AS c1 UNION SELECT 1, 1")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, false);
	});
	it("Read NULL as nullable boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, 1 AS c1 UNION SELECT 1, 1")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableBoolean, null);
	});

	it("Read \"Hello, world!!!\" as string through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 'Hello, world!!!' AS c0, 'stub12' AS c1 UNION ALL SELECT 'stub21', 'stub22'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asString, "Hello, world!!!");
	});
	it("Read NULL as nullable string through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, 'stub12' AS c1 UNION ALL SELECT 'stub21', 'stub22'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableString, null);
	});

	it("Read 11 as number through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 11 AS c0, 12 AS c1 UNION SELECT 21, 22")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNumber, 11);
	});
	it("Read NULL as nullable number through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, 12 AS c1 UNION SELECT 21, 22")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableNumber, null);
	});

	it("Read 11.42 as FinancialLike through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 11.42 AS c0, 12 AS c1 UNION SELECT 21, 22")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		const v = result.asFinancial;
		assert.isTrue(v.isPositive());
		assert.equal(v.toString(), "11.42");
	});
	it("Read '-11.42' as FinancialLike through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT '-11.42' AS c0, '12' AS c1 UNION SELECT '21', '22'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		const v = result.asFinancial;
		assert.isTrue(v.isNegative());
		assert.equal(v.toString(), "-11.42");
	});

	it("Read 2018-05-01T12:01:03.345 as Date through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement(
				"SELECT strftime('2018-05-01 12:01:02.345') AS c0, date('now') AS c1 UNION ALL SELECT date('now'), date('now')")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equalDate(result.asDate, new Date(2018, 4/*May month = 4*/, 1, 12, 1, 2, 345));
	});
	it("Read NULL as nullable Date through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement(
				"SELECT NULL AS c0, date('now') AS c1 UNION ALL SELECT date('now'), date('now');")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableDate, null);
	});

	it("Read 0x007FFF as Uint8Array through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT x'007FFF' AS c0, x'00' AS c1 UNION ALL SELECT x'00', x'00'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equalBytes(result.asBinary, new Uint8Array([0, 127, 255]));
	});
	it("Read NULL as Uint8Array through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, x'00' AS c1 UNION ALL SELECT x'00', x'00'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableBinary, null);
	});

	it("Read booleans through executeQuery", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT 1 AS c0, 0 AS c1 UNION ALL SELECT 0, 0 UNION ALL SELECT 1, 0")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);
		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
		assert.equal(resultArray[0].get("c0").asBoolean, true);
		assert.equal(resultArray[0].get("c1").asBoolean, false);
		assert.equal(resultArray[1].get("c0").asBoolean, false);
		assert.equal(resultArray[1].get("c1").asBoolean, false);
		assert.equal(resultArray[2].get("c0").asBoolean, true);
		assert.equal(resultArray[2].get("c1").asBoolean, false);
	});
	it("Read strings through executeQuery", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT 'one' AS c0, 'two' AS c1 UNION ALL SELECT 'three'" +
				", 'four' UNION ALL SELECT 'five', 'six'")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);
		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
		assert.equal(resultArray[0].get("c0").asString, "one");
		assert.equal(resultArray[0].get("c1").asString, "two");
		assert.equal(resultArray[1].get("c0").asString, "three");
		assert.equal(resultArray[1].get("c1").asString, "four");
		assert.equal(resultArray[2].get("c0").asString, "five");
		assert.equal(resultArray[2].get("c1").asString, "six");
	});
	it("Read strings through executeQuery (Stored Proc)", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT varcharValue, intValue FROM tb_1")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
		assert.equal(resultArray[0].get("varcharValue").asString, "one");
		assert.equal(resultArray[1].get("varcharValue").asString, "two");
		assert.equal(resultArray[2].get("varcharValue").asString, "three");
	});
	it.skip("Read (string and int)s through executeQuery (Multi record sets)", async function () {
		const resultArray = await getSqlProvider()
			.statement("CALL `sp_multi_fetch`")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
		assert.equal(resultArray[0].get("varchar").asString, "one");
		assert.equal(resultArray[0].get("int").asNumber, 1);
		assert.equal(resultArray[1].get("varchar").asString, "two");
		assert.equal(resultArray[1].get("int").asNumber, 2);
		assert.equal(resultArray[2].get("varchar").asString, "three");
		assert.equal(resultArray[2].get("int").asNumber, 3);
	});
	it("Read empty result through executeQuery (SELECT)", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT * FROM tb_1 WHERE 1=2")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 0);
	});
	it("Read empty result through executeQuery", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT 1 WHERE 1=0")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 0);
	});
	it("Call non-existing command execute", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("CALL `sp_non_existent`")
				.executeQuery(DUMMY_CANCELLATION_TOKEN);
		} catch (err) {
			expectedError = err;
		}
		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
	});
	it("Should be able to create temporary table", async function () {
		const tempTable = await getSqlProvider().createTempTable(
			DUMMY_CANCELLATION_TOKEN,
			"tb_1", // Should override(hide) existing table
			"id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR(32) NOT NULL, value SMALLINT NOT NULL"
		);
		try {
			await getSqlProvider().statement("INSERT INTO tb_1(title, value) VALUES('test title 1', ?)")
				.execute(DUMMY_CANCELLATION_TOKEN, 1);
			await getSqlProvider().statement("INSERT INTO tb_1(title, value) VALUES('test title 2', ?)")
				.execute(DUMMY_CANCELLATION_TOKEN, 2);

			const resultArray = await getSqlProvider().statement("SELECT title, value FROM tb_1")
				.executeQuery(DUMMY_CANCELLATION_TOKEN);

			assert.instanceOf(resultArray, Array);
			assert.equal(resultArray.length, 2);
			assert.equal(resultArray[0].get("title").asString, "test title 1");
			assert.equal(resultArray[0].get("value").asNumber, 1);
			assert.equal(resultArray[1].get("title").asString, "test title 2");
			assert.equal(resultArray[1].get("value").asNumber, 2);
		} finally {
			await tempTable.dispose();
		}

		// tslint:disable-next-line:max-line-length
		const resultArrayAfterDestoroyTempTable = await getSqlProvider().statement("SELECT * FROM tb_1").executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArrayAfterDestoroyTempTable, Array);
		assert.equal(resultArrayAfterDestoroyTempTable.length, 3);
		assert.equal(resultArrayAfterDestoroyTempTable[0].get("intValue").asNumber, 1);
		assert.equal(resultArrayAfterDestoroyTempTable[0].get("varcharValue").asString, "one");
	});
	it("Should be able to pass null into query args", async function () {
		const result1 = await getSqlProvider()
			.statement("SELECT 1 WHERE ? IS NULL")
			.executeScalar(DUMMY_CANCELLATION_TOKEN, null);
		assert.equal(result1.asInteger, 1);

		const result2 = await getSqlProvider()
			.statement("SELECT 1 WHERE ? IS NULL")
			.executeQuery(DUMMY_CANCELLATION_TOKEN, 0);
		assert.equal(result2.length, 0);
	});
	it("Should be able to pass Financial into query args", async function () {
		const result1 = await getSqlProvider()
			.statement("SELECT ?")
			.executeScalar(DUMMY_CANCELLATION_TOKEN, financial.parse("42.123"));
		assert.equal(result1.asString, "42.123");
	});
	it("Read with IN condition", async function () {
		const result = await getSqlProvider()
			.statement("SELECT \"varcharValue\" FROM \"tb_1\" WHERE \"intValue\" IN (?)")
			.executeQuery(DUMMY_CANCELLATION_TOKEN, [1, 3]);
		assert.isArray(result);
		assert.equal(result.length, 2);
		assert.equal(result[0].get("varcharValue").asString, "one");
		assert.equal(result[1].get("varcharValue").asString, "three");
	});
	it("Insert two variable to table tb_1 ", async function () {
		await getSqlProvider()
			.statement("INSERT INTO tb_1(varcharValue, intValue) VALUES (?, ?)")
			.execute(DUMMY_CANCELLATION_TOKEN, "One hundred", 100);
		await getSqlProvider()
			.statement("INSERT INTO tb_1(varcharValue, intValue) VALUES (?, ?);")
			.execute(DUMMY_CANCELLATION_TOKEN, "Two hundred", 200);

		const result = await getSqlProvider()
			.statement("SELECT \"varcharValue\", \"intValue\" FROM \"tb_1\" WHERE \"intValue\" IN (?,?)")
			.executeQuery(DUMMY_CANCELLATION_TOKEN, 100, 200);

		assert.isArray(result);
		assert.equal(result.length, 2);
		assert.equal(result[0].get("varcharValue").asString, "One hundred");
		assert.equal(result[1].get("varcharValue").asString, "Two hundred");
	});
	it.skip("Read two Result Sets via sp_multi_fetch", async function () {
		const resultSets = await getSqlProvider()
			.statement("CALL sp_multi_fetch()")
			.executeQueryMultiSets(DUMMY_CANCELLATION_TOKEN);
		assert.isArray(resultSets);
		assert.equal(resultSets.length, 2, "The procedure 'sp_multi_fetch' should return two result sets");

		{ // Verify first result set
			const firstResultSet = resultSets[0];
			assert.isArray(firstResultSet);
			assert.equal(firstResultSet.length, 3);
			assert.equal(firstResultSet[0].get("varchar").asString, "one");
			assert.equal(firstResultSet[0].get("int").asInteger, 1);
			assert.equal(firstResultSet[1].get("varchar").asString, "two");
			assert.equal(firstResultSet[1].get("int").asInteger, 2);
			assert.equal(firstResultSet[2].get("varchar").asString, "three");
			assert.equal(firstResultSet[2].get("int").asInteger, 3);
		}

		{ // Verify second result set
			const secondResultSet = resultSets[1];
			assert.isArray(secondResultSet);
			assert.equal(secondResultSet.length, 2);
			assert.equal(secondResultSet[0].get("first_name").asString, "Maxim");
			assert.equal(secondResultSet[0].get("last_name").asString, "Anurin");
			assert.equal(secondResultSet[1].get("first_name").asString, "Serhii");
			assert.equal(secondResultSet[1].get("last_name").asString, "Zghama");
		}
	});
	it("Read result through executeQuery (SELECT) WHERE IN many", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT * FROM \"tb_1\" WHERE intValue IN (?)")
			.executeQuery(DUMMY_CANCELLATION_TOKEN, [1, 2, 3]);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
	});
	it("Should raise when no records for executeScalar", async function () {
		let expectedError!: Error;
		try {
			await getSqlProvider()
				.statement("SELECT * FROM \"tb_1\" WHERE 1 = 0")
				.executeScalar(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.include(expectedError.message, "SQLite provider returns not enough data");
	});
	it("Should return null when no records for executeScalarOrNull", async function () {
		const executeResult = await getSqlProvider()
			.statement("SELECT * FROM \"tb_1\" WHERE 1 = 0")
			.executeScalarOrNull(DUMMY_CANCELLATION_TOKEN);

		assert.isNull(executeResult);
	});
	it("execute should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.execute(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeQuery should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeQuery(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it.skip("executeQueryMultiSets should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeQueryMultiSets(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeScalar should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeScalar(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeScalarOrNull should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeScalarOrNull(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeSingle should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeSingle(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("execute should raise SqlConstraintError for UNIQUE violation", async function () {
		let expectedError!: SqlConstraintError;
		try {
			await getSqlProvider()
				.statement("INSERT INTO tb_1 VALUES ('one', 1)")
				.execute(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlConstraintError);
		assert.isDefined(expectedError.innerError);
	});
});

