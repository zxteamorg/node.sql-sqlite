import { Logger } from "@zxteam/contract";
import { DUMMY_CANCELLATION_TOKEN } from "@zxteam/cancellation";
import { logger } from "@zxteam/logger";
import { MigrationSources } from "@zxteam/sql";

import { assert } from "chai";
import { PendingSuiteFunction, Suite, SuiteFunction } from "mocha";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";

import { SqliteMigrationManager } from "../src/SqliteMigrationManager";
import { SqliteProviderFactory } from "../src";

const { myDescribe, TEST_MIGRATION_DB_URL } = (function (): {
	myDescribe: PendingSuiteFunction | SuiteFunction;
	TEST_MIGRATION_DB_URL: string | null
} {
	let { TEST_MIGRATION_DB_URL: testDbUrl } = process.env;

	if (!testDbUrl || testDbUrl === "file+sqlite://") {
		const tmpDirectory = os.tmpdir();
		const pathToDB = path.join(tmpDirectory, "sqlite.db");
		const urlToDB = pathToFileURL(pathToDB);
		const sqliteFileUrl = new URL(`file+sqlite://${urlToDB.pathname}`);
		return { myDescribe: describe, TEST_MIGRATION_DB_URL: sqliteFileUrl.toString() };
	}

	switch (testDbUrl) {
		case "postgres://": {
			const host = "localhost";
			const port = 5432;
			const user = "postgres";
			testDbUrl = `postgres://${user}@${host}:${port}/emptytestdb`;
			return { myDescribe: describe, TEST_MIGRATION_DB_URL: testDbUrl };
		}
	}

	let url: URL;
	try { url = new URL(testDbUrl); } catch (e) {
		console.warn(`The tests ${__filename} are skipped due TEST_MIGRATION_DB_URL has wrong value. Expected URL like postgres://testuser:testpwd@127.0.0.1:5432/db`);
		return { myDescribe: describe.skip, TEST_MIGRATION_DB_URL: testDbUrl };
	}

	switch (url.protocol) {
		case "postgres:": {
			return { myDescribe: describe, TEST_MIGRATION_DB_URL: testDbUrl };
		}
		default: {
			console.warn(`The tests ${__filename} are skipped due TEST_MIGRATION_DB_URL has wrong value. Unsupported protocol: ${url.protocol}`);
			return { myDescribe: describe.skip, TEST_MIGRATION_DB_URL: testDbUrl };
		}
	}
})();

myDescribe("MigrationManager", function (this: Suite) {
	let sqlProviderFactory: SqliteProviderFactory;
	let log: Logger;

	this.beforeEach(async function () {
		const dbUrl: URL = new URL(TEST_MIGRATION_DB_URL!);

		const pathTodb = fileURLToPath(new URL(`file://${dbUrl.pathname}`));
		if (fs.existsSync(pathTodb)) {
			fs.unlinkSync(pathTodb);
		}

		log = this.currentTest !== undefined ? logger.getLogger(this.currentTest.title) : logger;

		sqlProviderFactory = new SqliteProviderFactory({ url: dbUrl, log });
		await sqlProviderFactory.newDatabase(DUMMY_CANCELLATION_TOKEN);
	});

	it("Migrate to latest version (omit targetVersion)", async () => {
		//await sqlProviderFactory.init(DUMMY_CANCELLATION_TOKEN);
		try {
			const migrationSources: MigrationSources = await MigrationSources.loadFromFilesystem(
				DUMMY_CANCELLATION_TOKEN,
				path.normalize(path.join(__dirname, "..", "test.files", "MigrationManager_1"))
			);

			const manager = new SqliteMigrationManager({
				migrationSources, sqlProviderFactory, log
			});

			await manager.install(DUMMY_CANCELLATION_TOKEN);

		} finally {
			//await sqlProviderFactory.dispose();
		}
	});
});
