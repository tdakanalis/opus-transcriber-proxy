/*
 * Copyright @ 2025 - present 8x8, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { Server } from 'http';

/** Why a drain ended: every connection closed, the limit passed, or it was stopped (a second signal). */
export type DrainEnd = 'ended' | 'limit' | 'stopped';

export interface Drain {
	/** Settles once the open connections have all ended, the limit has passed, or `stop()` was called. */
	readonly done: Promise<DrainEnd>;
	/** Settles once the server has closed: after `done`, when the shutdown has closed what was left open. */
	readonly closed: Promise<void>;
	/** Ends the drain at once. */
	stop(): void;
}

export interface DrainOptions {
	/** How long to wait for the open connections, in seconds; 0 ends the drain at once. */
	limitS: number;
	/** Called every `reportS` seconds while connections stay open. */
	onWaiting?: (open: number, waitedS: number) => void;
	reportS?: number;
	pollMs?: number;
}

/**
 * Stops `server` accepting connections and waits for the ones already open, counted by `openConnections`, to end.
 * An open connection goes on as before until its client closes it: a transcription session keeps sending its audio
 * to the backend, so a meeting in progress loses nothing when the process is replaced. New connections are refused,
 * and a client that retries reaches another copy of the service, when one answers to the same name.
 */
export function drain(server: Server, openConnections: () => number, options: DrainOptions): Drain {
	const { limitS, onWaiting, reportS = 60, pollMs = 1000 } = options;
	const closed = new Promise<void>((resolve) => server.close(() => resolve()));
	let stop: () => void = () => {};
	const done = new Promise<DrainEnd>((resolve) => {
		if (openConnections() === 0) return resolve('ended');
		if (limitS <= 0) return resolve('limit');
		const started = Date.now();
		let nextReportMs = reportS * 1000;
		const finish = (end: DrainEnd) => {
			clearInterval(poll);
			clearTimeout(limit);
			resolve(end);
		};
		const poll = setInterval(() => {
			const open = openConnections();
			if (open === 0) return finish('ended');
			const waitedMs = Date.now() - started;
			if (onWaiting && waitedMs >= nextReportMs) {
				nextReportMs += reportS * 1000;
				onWaiting(open, Math.round(waitedMs / 1000));
			}
		}, pollMs);
		const limit = setTimeout(() => finish('limit'), limitS * 1000);
		stop = () => finish('stopped');
	});
	return { done, closed, stop: () => stop() };
}
