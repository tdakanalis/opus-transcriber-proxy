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

/**
 * The drain on SIGTERM (src/drain.ts), over real sockets: the server stops accepting connections, a session already
 * open goes on sending its audio until its client closes it, and the drain ends then, or when its limit passes.
 */

import http from 'http';
import type { AddressInfo } from 'net';
import { afterEach, describe, expect, it } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import { drain } from '../../src/drain';

// A server like server.ts: WebSocket upgrades, each session forwarding what its client sends to a backend.
async function startServer() {
	const forwarded: string[] = [];
	const server = http.createServer((_req, res) => res.end('OK'));
	const wss = new WebSocketServer({ noServer: true });
	server.on('upgrade', (request, socket, head) => {
		wss.handleUpgrade(request, socket, head, (ws) => ws.on('message', (data) => forwarded.push(data.toString())));
	});
	await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
	const url = `ws://127.0.0.1:${(server.address() as AddressInfo).port}/transcribe`;
	return { server, wss, url, forwarded };
}

function connect(url: string): Promise<WebSocket> {
	return new Promise((resolve, reject) => {
		const ws = new WebSocket(url);
		ws.once('open', () => resolve(ws));
		ws.once('error', reject);
	});
}

async function until(condition: () => boolean, timeoutMs = 2000): Promise<void> {
	const started = Date.now();
	while (!condition()) {
		if (Date.now() - started > timeoutMs) throw new Error('timed out');
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
}

const cleanups: Array<() => void> = [];
afterEach(() => cleanups.splice(0).forEach((cleanup) => cleanup()));

describe('drain', () => {
	it('keeps an open session sending audio until it ends, and refuses new connections', async () => {
		const { server, wss, url, forwarded } = await startServer();
		cleanups.push(() => server.closeAllConnections());
		const session = await connect(url);
		session.send('audio 1');
		await until(() => forwarded.length === 1);

		const draining = drain(server, () => wss.clients.size, { limitS: 30, pollMs: 10 });
		await expect(connect(url)).rejects.toThrow(/ECONNREFUSED/);
		session.send('audio 2');
		session.send('audio 3');
		await until(() => forwarded.length === 3);
		expect(forwarded).toEqual(['audio 1', 'audio 2', 'audio 3']);

		let end: string | undefined;
		void draining.done.then((value) => (end = value));
		await new Promise((resolve) => setTimeout(resolve, 50));
		expect(end).toBeUndefined();

		session.close();
		expect(await draining.done).toBe('ended');
		await draining.closed;
	});

	it('ends at its limit while a session stays open', async () => {
		const { server, wss, url } = await startServer();
		cleanups.push(() => server.closeAllConnections());
		const session = await connect(url);
		cleanups.push(() => session.terminate());

		const started = Date.now();
		const draining = drain(server, () => wss.clients.size, { limitS: 0.2, pollMs: 10 });
		expect(await draining.done).toBe('limit');
		expect(Date.now() - started).toBeGreaterThanOrEqual(190);
		expect(session.readyState).toBe(WebSocket.OPEN);
	});

	it('ends at once when stopped, with a limit of 0, or with nothing open', async () => {
		const first = await startServer();
		cleanups.push(() => first.server.closeAllConnections());
		const session = await connect(first.url);
		cleanups.push(() => session.terminate());
		const draining = drain(first.server, () => first.wss.clients.size, { limitS: 30, pollMs: 10 });
		draining.stop();
		expect(await draining.done).toBe('stopped');

		const second = await startServer();
		cleanups.push(() => second.server.closeAllConnections());
		const other = await connect(second.url);
		cleanups.push(() => other.terminate());
		expect(await drain(second.server, () => second.wss.clients.size, { limitS: 0 }).done).toBe('limit');

		const idle = await startServer();
		const idleDrain = drain(idle.server, () => idle.wss.clients.size, { limitS: 30 });
		expect(await idleDrain.done).toBe('ended');
		await idleDrain.closed;
	});

	it('reports the open connections while it waits', async () => {
		const { server, wss, url } = await startServer();
		cleanups.push(() => server.closeAllConnections());
		const session = await connect(url);
		const reports: number[] = [];
		const draining = drain(server, () => wss.clients.size, {
			limitS: 30,
			pollMs: 10,
			reportS: 0.05,
			onWaiting: (open) => reports.push(open),
		});
		await until(() => reports.length >= 2);
		expect(reports.every((open) => open === 1)).toBe(true);
		session.close();
		expect(await draining.done).toBe('ended');
	});
});
