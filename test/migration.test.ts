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
	return urlToDB;
}
function getSQLiteUrltoScripts(): string {
	return path.join(__dirname, "database");
}

describe("SQLite Migration Database", function () {
	beforeEach(async function () {
		const pathTodb = fileURLToPath(getSQLiteUrltoDb());
		if (fs.existsSync(pathTodb)) {
			await fs.unlinkSync(pathTodb);
		}
	});
	it("Migration database", async function () {
		const sqlProviderFactory = new lib.SqliteProviderFactory(getSQLiteUrltoDb());
		await sqlProviderFactory.migration(DUMMY_CANCELLATION_TOKEN, getSQLiteUrltoScripts(), undefined);
		const db = await sqlProviderFactory.create(DUMMY_CANCELLATION_TOKEN);
		try {
			const resultArray = await db
				.statement("SELECT varcharValue, intValue FROM tb_1")
				.executeQuery(DUMMY_CANCELLATION_TOKEN);

			assert.instanceOf(resultArray, Array);
			assert.equal(resultArray.length, 3);
			assert.equal(resultArray[0].get("varcharValue").asString, "one");
			assert.equal(resultArray[1].get("varcharValue").asString, "two");
			assert.equal(resultArray[2].get("varcharValue").asString, "three");
		} finally {
			await db.dispose();
		}
	});
});
