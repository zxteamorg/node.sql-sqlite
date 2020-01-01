import { CancellationToken } from "@zxteam/contract";
import { CancelledError } from "@zxteam/errors";

import { EOL } from "os";

import * as stream from "stream";
import * as readline from "readline";

export async function splitScriptToStatements(cancellationToken: CancellationToken, sqlScriptContent: string): Promise<Array<string>> {
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
