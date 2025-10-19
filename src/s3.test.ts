import { describe, it, skip } from 'node:test';
import assert from 'node:assert/strict';
import * as FS from 'node:fs';
import * as CR from 'node:crypto';
import { fileURLToPath } from 'node:url';

import S3 from './s3.ts';

describe('S3 Tests', () => {
	const options = (() => {
		try {
			const url = new URL('../bucket.json', import.meta.url);
			const file = fileURLToPath(url);
			const text = FS.readFileSync(file, 'utf-8');
			return JSON.parse(text);
		} catch {
			return null;
		}
	})();
	const s3 = options ? new S3(options) : null;
	const prefix = `test-${process.pid}`;
	const filename = 'my-test-file';
	const content = CR.randomBytes(50);
	let etag: string | null = null;
	describe('basic', () => {
		test(s3)('put', async () => {
			assertS3(s3);
			const result = await s3.put(`${prefix}/${filename}`, content);
			assert(result);
			etag = result;
		});
		test(s3)('put(etag)', async () => {
			assertS3(s3);
			const result = await s3.put(`${prefix}/${filename}`, content);
			assert.equal(result, etag);
		});
		test(s3)('get', async () => {
			assertS3(s3);
			const result = await s3.get(`${prefix}/${filename}`);
			assert.equal(result?.toString('hex'), content.toString('hex'));
		});
		test(s3)('stream', async () => {
			assertS3(s3);
			const result = Buffer.concat(
				await Array.fromAsync(s3.stream(`${prefix}/${filename}`)),
			);
			assert.equal(result?.toString('hex'), content.toString('hex'));
		});

		test(s3)('head', async () => {
			assertS3(s3);
			const result = await s3.head(`${prefix}/${filename}`);
			assert.equal(result.etag, etag);
		});
		test(s3)('list', async () => {
			assertS3(s3);
			const result = await Array.fromAsync(s3.list(prefix));
			assert(result);
			assert.equal(result.length, 1);
			assert.equal(result[0].name, `${prefix}/${filename}`);
			assert.equal(result[0].size, content.length);
		});
		test(s3)('copy', async () => {
			assertS3(s3);
			const etag = await s3.copy(
				`${prefix}/${filename}-2`,
				`${prefix}/${filename}`,
			);
			assert(etag);
			const headers = await s3.head(`${prefix}/${filename}-2`);
			assert.equal(etag, headers.etag);
			const original = await s3.head(`${prefix}/${filename}`);
			assert.equal(etag, original.etag);
			await s3.del(`${prefix}/${filename}-2`);
		});
		test(s3)('del', async () => {
			assertS3(s3);
			await s3.del(`${prefix}/${filename}`);
			assert.rejects(async () => await s3.head(`${prefix}/${filename}`));
		});
	});
});

function test(condition: any) {
	return condition ? it : skip;
}
function assertS3(s3: any): asserts s3 is S3 {
	assert(s3 instanceof S3);
}
