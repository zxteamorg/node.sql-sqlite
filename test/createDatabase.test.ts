import * as chai from "chai";
import { CancellationToken } from "@zxteam/contract";
import ensureFactory from "@zxteam/ensure.js";

import * as lib from "../src";
const fs = require("fs");
import { URL, fileURLToPath } from "url";

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
	const pathTodb = __dirname + "/.." + "/.tmp" + "/sqliteForCreateTest.db";
	const fullPathDb = "file:///" + pathTodb;
	return new URL(fullPathDb);
}
function getSQLiteUrltoSqlFile(): URL {
	const fullPathDb = "file:///" + __dirname + "/.." + "/test" + "/general.test.sql";
	return new URL(fullPathDb);
}
function getSQLiteUrltoSqlHref(): URL {
	const fullPathDb = "http://vmhost01.zxteam.net/~maxim.anurin/tmp/general.test.sql";
	return new URL(fullPathDb);
}

describe("SQLite Create Database", function () {
	beforeEach(async function () {
		const pathTodb = fileURLToPath(getSQLiteUrltoDb());
		if (fs.existsSync(pathTodb)) {
			await fs.unlinkSync(pathTodb);
		}
	});
	it("Create database", async function () {
		const sqlProviderFactory = new lib.SQLiteProviderFactory({ fullPathDb: getSQLiteUrltoDb() });
		await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN);
		const db = await sqlProviderFactory.create();
		try {
			const sqlData = await db.statement("SELECT 1;").executeScalar(DUMMY_CANCELLATION_TOKEN);
			assert.equal(sqlData.asNumber, 1);
		} finally {
			db.dispose();
		}
	});
	it("Cannot open database because do not exist", async function () {
		const sqlProviderFactory = new lib.SQLiteProviderFactory({ fullPathDb: getSQLiteUrltoDb() });
		try {
			const db = await sqlProviderFactory.create();
		} catch (err) {
			assert.isNotNull(err);
			return;
		}
		assert.fail("No exceptions");
	});
	it("Create database and run file init script", async function () {
		const sqlProviderFactory = new lib.SQLiteProviderFactory({ fullPathDb: getSQLiteUrltoDb() });
		await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN, getSQLiteUrltoSqlFile());
		const db = await sqlProviderFactory.create();
		try {
			const arraySqlData = await db.statement("SELECT varcharValue, intValue FROM 'tb_1';").executeQuery(DUMMY_CANCELLATION_TOKEN);
			assert.equal(arraySqlData[0].get("varcharValue").asString, "one");
			assert.equal(arraySqlData[1].get("varcharValue").asString, "two");
			assert.equal(arraySqlData[2].get("varcharValue").asString, "three");

			assert.equal(arraySqlData[0].get("intValue").asNumber, 1);
			assert.equal(arraySqlData[1].get("intValue").asNumber, 2);
			assert.equal(arraySqlData[2].get("intValue").asNumber, 3);
		} finally {
			db.dispose();
		}
	});
	it("Create database and run href init script", async function () {
		const sqlProviderFactory = new lib.SQLiteProviderFactory({ fullPathDb: getSQLiteUrltoDb() });
		await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN, getSQLiteUrltoSqlHref());
		const db = await sqlProviderFactory.create();
		try {
			const arraySqlData = await db.statement("SELECT varcharValue, intValue FROM 'tb_1';").executeQuery(DUMMY_CANCELLATION_TOKEN);
			assert.equal(arraySqlData[0].get("varcharValue").asString, "one");
			assert.equal(arraySqlData[1].get("varcharValue").asString, "two");
			assert.equal(arraySqlData[2].get("varcharValue").asString, "three");

			assert.equal(arraySqlData[0].get("intValue").asNumber, 1);
			assert.equal(arraySqlData[1].get("intValue").asNumber, 2);
			assert.equal(arraySqlData[2].get("intValue").asNumber, 3);
		} finally {
			db.dispose();
		}
	});
});

