import * as chai from "chai";
import { CancellationToken } from "@zxteam/contract";
import * as fs from "fs";
import * as http from "http";
import * as os from "os";
import * as path from "path";
import { URL, fileURLToPath, pathToFileURL } from "url";

import * as lib from "../src";

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

const DUMMY_CANCELLATION_TOKEN: CancellationToken = {
	get isCancellationRequested(): boolean { return false; },
	addCancelListener(cb: Function): void { /* STUB */ },
	removeCancelListener(cb: Function): void { /* STUB */ },
	throwIfCancellationRequested(): void { /* STUB */ }
};

function getSQLiteUrltoDb(): URL {
	const tmpDirectory = os.tmpdir();
	const pathToDB = path.join(tmpDirectory, "sqliteForCreateTest.db");
	const urlToDB = pathToFileURL(pathToDB);
	const sqliteFileUrl = new URL(`file+sqlite://${urlToDB.pathname}`);
	return sqliteFileUrl;
}
function getSQLiteUrltoSqlFile(): URL {
	const pathToSqlScript = path.join(__dirname, "general.test.sql");
	const urlToDB = pathToFileURL(pathToSqlScript);
	return urlToDB;
}
function getSQLiteUrltoSqlHref(): URL {
	return new URL("http://localhost:8080/general.test.sql");
}

describe("SQLite Create Database", function () {
	beforeEach(async function () {
		const dbUrl: URL = getSQLiteUrltoDb();
		const pathTodb = fileURLToPath(new URL(`file://${dbUrl.pathname}`));
		if (fs.existsSync(pathTodb)) {
			await fs.unlinkSync(pathTodb);
		}
	});
	it("Create database", async function () {
		const sqlProviderFactory = new lib.SqliteProviderFactory({ url: getSQLiteUrltoDb() });
		await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN);
		const db = await sqlProviderFactory.create(DUMMY_CANCELLATION_TOKEN);
		try {
			const sqlData = await db.statement("SELECT 1;").executeScalar(DUMMY_CANCELLATION_TOKEN);
			assert.equal(sqlData.asNumber, 1);
		} finally {
			await db.dispose();
		}
	});
	it("Cannot open database because do not exist", async function () {
		const sqlProviderFactory = new lib.SqliteProviderFactory({ url: getSQLiteUrltoDb() });
		try {
			const db = await sqlProviderFactory.create(DUMMY_CANCELLATION_TOKEN);
		} catch (err) {
			assert.isNotNull(err);
			return;
		}
		assert.fail("No exceptions");
	});
	it("Create database and run file init script", async function () {
		const sqlProviderFactory = new lib.SqliteProviderFactory({ url: getSQLiteUrltoDb() });
		await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN, getSQLiteUrltoSqlFile());
		const db = await sqlProviderFactory.create(DUMMY_CANCELLATION_TOKEN);
		try {
			const arraySqlData = await db.statement("SELECT varcharValue, intValue FROM 'tb_1';").executeQuery(DUMMY_CANCELLATION_TOKEN);

			assert.equal(arraySqlData.length, 3);

			assert.equal(arraySqlData[0].get("varcharValue").asString, "one");
			assert.equal(arraySqlData[1].get("varcharValue").asString, "two");
			assert.equal(arraySqlData[2].get("varcharValue").asString, "three");

			assert.equal(arraySqlData[0].get("intValue").asNumber, 1);
			assert.equal(arraySqlData[1].get("intValue").asNumber, 2);
			assert.equal(arraySqlData[2].get("intValue").asNumber, 3);
		} finally {
			await db.dispose();
		}
	});
	it("Create database and run href init script", async function () {
		const pathTodb = fileURLToPath(getSQLiteUrltoSqlFile());
		const scriptContent = fs.readFileSync(pathTodb, "utf-8");
		const httpServer = await helper.startHttpServer(scriptContent);
		try {
			const sqlProviderFactory = new lib.SqliteProviderFactory({ url: getSQLiteUrltoDb() });
			await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN, getSQLiteUrltoSqlHref());
			const db = await sqlProviderFactory.create(DUMMY_CANCELLATION_TOKEN);
			try {
				const arraySqlData = await db.statement("SELECT varcharValue, intValue FROM 'tb_1';")
					.executeQuery(DUMMY_CANCELLATION_TOKEN);

				assert.equal(arraySqlData.length, 3);

				assert.equal(arraySqlData[0].get("varcharValue").asString, "one");
				assert.equal(arraySqlData[1].get("varcharValue").asString, "two");
				assert.equal(arraySqlData[2].get("varcharValue").asString, "three");

				assert.equal(arraySqlData[0].get("intValue").asNumber, 1);
				assert.equal(arraySqlData[1].get("intValue").asNumber, 2);
				assert.equal(arraySqlData[2].get("intValue").asNumber, 3);
			} finally {
				await db.dispose();
			}
		} finally {
			helper.stopHttpServer(httpServer);
		}
	});
});

namespace helper {
	export function startHttpServer(bindContent: string): Promise<http.Server> {
		return new Promise((resolve, reject) => {
			const server = http.createServer(function (request, response) { response.end(bindContent); })
				.on("listening", function () { resolve(server); })
				.on("error", reject)
				.listen(8080, "127.0.0.1");
		});
	}
	export function stopHttpServer(server: http.Server) {
		return new Promise((resolve) => {
			server.close(() => { resolve(); });
		});
	}
}
