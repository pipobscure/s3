import { describe, it, skip, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as FS from 'node:fs';
import * as PT from 'node:path';
import * as CR from 'node:crypto';
import * as OS from 'node:os';
import { fileURLToPath } from 'node:url';

import S3, { MIN_CHUNK_SIZE } from './s3.ts';

const tmpdir = FS.mkdtempDisposableSync(`${OS.tmpdir()}/test-${process.pid}`);
describe('MultiPart', () => {
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
	let etag: string | null;
	after(async () => {
		await tmpdir.remove();
		if (s3) await s3.del(`${prefix}/${filename}`);
	});
	describe('large stream', () => {
		before(async () => {
			let length = 0;
			const file = await FS.promises.open(
				PT.join(tmpdir.path, 'blob.dat'),
				'w',
			);
			while (length < MIN_CHUNK_SIZE * 3.5) {
				const deferred = Promise.withResolvers<Buffer>();
				CR.randomBytes(1024, (err, result) => {
					if (err) return deferred.reject(err);
					deferred.resolve(result);
				});
				const content = await await deferred.promise;
				await file.appendFile(content);
				length += content.length;
			}
			await file.close();
		});
		test(s3)('put', async () => {
			assertS3(s3);
			const stream = FS.createReadStream(PT.join(tmpdir.path, 'blob.dat'));
			const result = await s3.put(`${prefix}/${filename}`, stream);
			assert(result);
			etag = result;
		});
		test(s3)('validate', async () => {
			assertS3(s3);
			const headers = await s3.head(`${prefix}/${filename}`);
			assert.equal(JSON.parse(headers?.get('etag') ?? '""'), etag);
		});
	});
	describe('small stream', () => {
		test(s3)('put', async () => {
			assertS3(s3);
			const result = await s3.put(`${prefix}/${filename}`, smallData());
			assert(result);
			etag = result;
		});
		test(s3)('validate', async () => {
			assertS3(s3);
			const headers = await s3.head(`${prefix}/${filename}`);
			assert.equal(JSON.parse(headers?.get('etag') ?? '""'), etag);
		});
	});
});

function test(condition: any) {
	return condition ? it : skip;
}
function assertS3(s3: any): asserts s3 is S3 {
	assert(s3 instanceof S3);
}
async function* smallData() {
	let cnt = 3;
	while (cnt) {
		const deferred = Promise.withResolvers<Buffer<ArrayBufferLike>>();
		CR.randomBytes(50, (err, result) => {
			if (err) return deferred.reject(err);
			deferred.resolve(result);
		});
		yield await deferred.promise;
		cnt -= 1;
	}
}
