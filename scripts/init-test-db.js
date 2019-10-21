const sqlite = require("sqlite3");
const fs = require("fs");
const path = require("path");
const glob = require("../node_modules/glob");

//** Путь к папкам где хранятся sql файлы */
const fullPathSql = __dirname + "/../test/";
//** Путь к базе */
const dirName = ".tmp";
const fullPathDb = path.join(__dirname, "..", dirName, "sqlite.db");

/** Установка environment */
let env = "";
process.argv.forEach(function (val, index, array) {
	if (index === 2 && val) {
		env = val;
	}
});

try {
	if (!fs.existsSync(dirName)) {
		fs.mkdirSync(dirName);
	}
	if (fs.existsSync(fullPathDb)) {
		fs.unlinkSync(fullPathDb);
	}
} catch (e) {
	console.log("Couldn't delete file with db:", e);
}

function pathSqlFiles() {
	return new Promise(resolve => {
		const zfiles = glob.sync(fullPathSql + "/**/*.sql");
		resolve(zfiles);
	});
}
function getFolderEnv(pathSql) {
	const splitPath = pathSql.split("/");
	const folderName = splitPath[splitPath.length - 2];
	const splitFolder = folderName.split("-");
	const folderEnv = splitFolder[2];
	return folderEnv;
}

async function run() {
	const dateStart = new Date().getTime();
	console.log("Start app:", dateStart);
	let lastCommand = "";
	if (!env) { console.log("Set environment: default"); } else { console.log("Set environment:", env); }
	try {
		const pathSqls = await pathSqlFiles();
		const sqliteDb = new sqlite.Database(fullPathDb);
		for (let i = 0; i < pathSqls.length; i++) {
			const pathSql = pathSqls[i];
			const folderEnv = getFolderEnv(pathSql);
			if (folderEnv === undefined || folderEnv === env) {
				console.log("Processing:", pathSql);
				const sqlScripts = await fs.readFileSync(pathSql, "utf8");
				const lines = sqlScripts.split("\n");
				//console.log("lines", lines);
				let allCommands = [];
				let command = "";
				let executionChain = Promise.resolve();
				for (let i = 0; i < lines.length; i++) {
					let line = lines[i];
					while (line.endsWith("\r")) {
						line = line.substring(0, line.length - 1);
					}


					if (!((!line) || line.startsWith("--"))) {
						command += line;
						if (command.endsWith(";")) {
							allCommands.push(command);
							command = "";
						}
					}
				}
				for (let z = 0; z < allCommands.length; z++) {
					const command = allCommands[z];
					lastCommand = command;
					executionChain = executionChain.then(() => sqliteRunScript(sqliteDb, command));
				}

				await executionChain;
			}
		}
		console.log("Close db");
		sqliteDb.close();
		console.log("Well done :)");
		const dateEnd = new Date().getTime();
		console.log("End app:", dateEnd);
		const diff = dateEnd - dateStart;
		console.log("App was launched (seconds): ", diff / 1000)
	} catch (e) {
		console.error("Last command:", lastCommand, "Error msg:", e);
	}
}

function sqliteRunScript(instansDb, sql) {
	const hrstart = process.hrtime();
	return new Promise((resolve, reject) => {
		try {
			console.log(sql);
			instansDb.run(sql, function (error) {
				const hrend = process.hrtime(hrstart);
				console.log("SQL Execution time (hr): %ds", hrend[0] + hrend[1] / 1000000000);

				if (error) {
					reject(error);
					return;
				}
				resolve(this);
			});
		} catch (e) {
			console.log("Piece of shit", e);
			reject(e);
		}
	});
}


try {
	run();
}
catch (e) {
	console.error(e);
}


