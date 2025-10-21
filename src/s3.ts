import * as CRYPTO from 'node:crypto';
import * as XML from 'xml-js';

export const MIN_CHUNK_SIZE = 5242880; // 5MB
export type S3Options = {
	key: string;
	secret: string;
	endpoint: string;
	bucket: string;
	hostBased?: boolean;
};
export type S3Method = 'GET' | 'PUT' | 'DELETE' | 'HEAD' | 'POST';

export type AWSProperty = 'If-Match' | 'Range' | 'Content-Type';
export type AWShdrs = Record<AWSProperty, string>;
const AWSParams = [
	'acl',
	'lifecycle',
	'location',
	'logging',
	'notification',
	'partNumber',
	'policy',
	'requestPayment',
	'uploadId',
	'uploads',
	'versionId',
	'versioning',
	'versions',
	'website',
];
type UploadPart = {
	number: number;
	etag: string;
	sha1?: string;
	sha256?: string;
};
export class S3 {
	#key: string;
	#secret: string;
	#endpoint: string;
	#bucket: string;
	#hostBased: boolean;
	constructor(options: S3Options) {
		const { key, secret, endpoint, bucket, hostBased = false } = options;
		this.#key = key;
		this.#secret = secret;
		this.#endpoint = endpoint;
		this.#bucket = bucket;
		this.#hostBased = hostBased;
	}
	#url(filename: string) {
		if (this.#hostBased) {
			return new URL(
				`/${filename}`.split(/\/+/).join('/'),
				`https://${this.#bucket}.${this.#endpoint}`,
			);
		} else {
			return new URL(
				`/${this.#bucket}/${filename}`.split(/\/+/).join('/'),
				`https://${this.#endpoint}`,
			);
		}
	}
	async #request(
		method: S3Method,
		url: URL,
		hdrs: Record<string, string>,
		sendbody?: Buffer,
		stream?: boolean,
		signal?: AbortSignal,
	) {
		const date = new Date();
		let resource = `${this.#hostBased ? `/${this.#bucket}` : ''}${url.pathname}`;
		let first = true;
		for (const [key, val] of url.searchParams) {
			if (!AWSParams.includes(key)) continue;
			resource = `${resource}${first ? '?' : '&'}${key}${val ? `=${encodeURIComponent(val)}` : ''}`;
			first = false;
		}
		hdrs.Date = date.toUTCString();
		hdrs.Host = url.host;
		if (sendbody?.length) {
			hdrs['Content-Length'] = `${sendbody.length}`;
			hdrs['Content-MD5'] = CRYPTO.createHash('md5')
				.update(sendbody)
				.digest('base64');
			hdrs['Content-Type'] = hdrs['Content-Type'] ?? 'application/octet-stream';
		}
		const signature = sign(
			this.#secret,
			method,
			hdrs['Content-MD5'] ?? '',
			hdrs['Content-Type'] ?? '',
			hdrs.Date,
			canonicalizeHeaders(hdrs),
			resource,
		);
		hdrs.Authorization = `AWS ${this.#key}:${signature}`;
		const body = sendbody as Uint8Array | undefined;
		const response = await fetch(url, {
			method,
			headers: hdrs,
			body,
			signal,
		} as any as RequestInit);
		signal?.throwIfAborted();
		if (!response.ok) {
			const bytes = (await response.bytes()) ?? null;
			throw Object.assign(
				new Error(
					`HTTP(${response.status}) ${response.statusText} [${method} ${url}]`,
				),
				{
					url: `${url}`,
					status: response.status,
					'request-headers': hdrs,
					'response-headers': Object.fromEntries(response.headers.entries()),
					text: bytes ? Buffer.from(bytes).toString('utf-8') : undefined,
				},
			);
		}
		const content = stream
			? response.body
			: Buffer.from(await response.bytes());
		const headers = response.headers;
		return { headers, content };
	}
	async head(name: string, signal?: AbortSignal) {
		const { headers } = await this.#request('HEAD', this.#url(name), {}, undefined, undefined, signal);
		const size = +(headers.get('content-length') ?? 0);
		const type = headers.get('content-type') ?? 'application/octet-stream';
		const etag = (JSON.parse(headers.get('etag') ?? '""') || undefined) as
			| string
			| undefined;
		const modified = new Date(
			headers.has('last-modified')
				? Date.parse(headers.get('last-modified') as string)
				: Date.now(),
		).toISOString();
		return { name, size, type, modified, etag };
	}
	get(resource: string, etagOrSignal?: string | AbortSignal): Promise<Buffer>;
	async get(resource: string, etag?: string | AbortSignal, signal?: AbortSignal) {
		if (etag && (etag instanceof AbortSignal)) {
			if (signal instanceof AbortSignal) {
				signal = AbortSignal.any([etag, signal]);
			} else {
				signal = etag;
			}
			etag = undefined;
		}
		const headers: Record<string, string> = {};
		if (etag) {
			headers['If-None-Match'] = JSON.stringify(etag);
		}
		const { content } = await this.#request(
			'GET',
			this.#url(resource),
			headers,
			undefined,
			undefined,
			signal
		);
		return content as Buffer;
	}
	stream(resource: string, etagOrSignal?: string | AbortSignal): AsyncGenerator<Buffer>;
	async *stream(resource: string, etag?: string | AbortSignal, signal?: AbortSignal): AsyncIterable<Buffer> {
		if (etag && (etag instanceof AbortSignal)) {
			if (signal instanceof AbortSignal) {
				signal = AbortSignal.any([etag, signal]);
			} else {
				signal = etag;
			}
			etag = undefined;
		}
		const headers: Record<string, string> = {};
		if (etag) {
			headers['If-None-Match'] = JSON.stringify(etag);
		}
		const { content } = await this.#request(
			'GET',
			this.#url(resource),
			headers,
			undefined,
			true,
			signal
		);
		if (!content) throw new Error('missing content');
		for await (const chunk of content as ReadableStream<Uint8Array>) {
			signal?.throwIfAborted();
			yield Buffer.from(chunk) as Buffer;
			signal?.throwIfAborted();
		}
	}
	async put(
		resource: string,
		content: AsyncIterable<Buffer> | Buffer | string | object,
		type?: string | AbortSignal,
		etag?: string | AbortSignal,
		signal?: AbortSignal
	) {
		if (!signal) {
			const signals = [type, etag, signal].filter(x => (x instanceof AbortSignal));
			signal = signals.length ? AbortSignal.any(signals) : undefined;
			type = type instanceof AbortSignal ? undefined : (type as (string | undefined));
			etag = etag instanceof AbortSignal ? undefined : (etag as (string | undefined));
		}
		if (!type && isMimeType(etag)) {
			type = etag;
			etag = undefined;
		}
		if (type && !isMimeType(type)) {
			etag = type;
			type = undefined;
		}
		type = type ?? 'application/octet-stream' as string;
		if ((content as any)[Symbol.asyncIterator]) {
			return await this.#putStream(
				resource,
				content as AsyncIterable<Buffer>,
				type as string,
				etag as string | undefined,
				signal,
			);
		}

		const hdrs: Record<string, string> = { 'Content-Type': type as string };
		if (etag) {
			hdrs['If-Match'] = etag as string;
		} else {
			hdrs['If-None-Match'] = '*';
		}
		if ('string' === typeof content) {
			content = Buffer.from(content, 'utf-8');
			hdrs['Content-Type'] =
				hdrs['Content-Type'] ?? 'text/plain; charset=utf-8';
		}
		if (!Buffer.isBuffer(content)) {
			content = Buffer.from(JSON.stringify(content), 'utf-8');
			hdrs['Content-Type'] =
				hdrs['Content-Type'] ?? 'application/json; charset=utf-8';
		}
		hdrs['Content-Type'] = hdrs['Content-Type'] ?? 'application/octet-stream';
		const { headers } = await this.#request(
			'PUT',
			this.#url(resource),
			hdrs,
			content as Buffer,
			false,
			signal,
		);
		return JSON.parse(headers.get('etag') ?? '""') as string;
	}
	del(resource: string, etagOrSignal?: string | AbortSignal): Promise<void>;
	async del(resource: string, etag?: string | AbortSignal, signal?: AbortSignal) {
		if (etag && (etag instanceof AbortSignal)) {
			if (signal instanceof AbortSignal) {
				signal = AbortSignal.any([etag, signal]);
			} else {
				signal = etag;
			}
			etag = undefined;
		}
		const hdrs: Record<string, string> = {};
		if (etag) {
			hdrs['If-Match'] = JSON.stringify(etag);
		}
		await this.#request('DELETE', this.#url(resource), hdrs, undefined, false, signal);
	}
	async #list(
		prefix?: string,
		options?: Record<string, string> & { signal?: AbortSignal },
		continuation?: string,
	): Promise<{
		continuation?: string;
		items: { name: string; size: number; etag: string; modified: number }[];
	}> {
		const url = this.#url('/');
		if (options) {
			for (const [key, val] of Object.entries(options)) {
				if (key === 'signal') continue;
				url.searchParams.set(key, `${val}`);
			}
		}
		url.searchParams.set('list-type', '2');
		if (continuation) {
			url.searchParams.set('continuation-token', continuation);
		}
		if (prefix) {
			url.searchParams.set('delimiter', '/');
			url.searchParams.set('prefix', prefix);
		}

		const { content } = (await this.#request('GET', url, {}, undefined, false, options?.signal)) as {
			headers: Headers;
			content: Buffer;
		};
		if (!content?.length) {
			throw new Error(`failed to list objects in ${prefix}`);
		}
		const data = XML.xml2js(content.toString('utf-8'), {
			compact: true,
			trim: true,
			ignoreDeclaration: true,
			ignoreInstruction: true,
			ignoreAttributes: true,
			ignoreComment: true,
			alwaysArray: true,
		}) as any;
		const contents = data?.ListBucketResult?.[0]?.Contents ?? [];
		return {
			continuation: data?.ListBucketResult?.[0]?.NextContinuationToken,
			items: contents.map((item: any) => {
				const name = item?.Key?.[0]?._text?.join('');
				const modified =
					item?.LastModified?.[0]?._text?.join('') ?? new Date().toISOString();
				const size = +(item?.Size?.[0]?._text?.join('') ?? 0);
				const etag = item?.ETag?.[0]?._text?.join('') ?? 'invalid';
				return { name, size, etag, modified };
			}),
		};
	}
	async *list(prefix?: string, options?: Record<string, string> & { signal?: AbortSignal }) {
		prefix = prefix
			?.split(/\/+/)
			.flatMap((x) => ((x = x.trim()), x ? [x] : []))
			.join('/');
		prefix = prefix ? `${prefix}/` : undefined;

		let continuation: string | undefined;
		let items: { name: string; size: number; modified: number }[] = [];
		do {
			({ items, continuation } = await this.#list(prefix, options));
			yield* items;
		} while (continuation);
	}
	copy(target: string, source: string, etagOrSignal?: string | AbortSignal): Promise<string>;
	async copy(target: string, source: string, etag?: string | AbortSignal, signal?: AbortSignal) {
		if (etag && (etag instanceof AbortSignal)) {
			if (signal instanceof AbortSignal) {
				signal = AbortSignal.any([etag, signal]);
			} else {
				signal = etag;
			}
			etag = undefined;
		}
		source = [this.#bucket, ...source.split(/\/+/)]
			.filter((x) => (x = x.trim()))
			.join('/');
		const hdrs: Record<string, string> = { 'x-amz-copy-source': source };
		if (etag) {
			hdrs['If-Match'] = JSON.stringify(etag);
		} else {
			hdrs['If-None-Match'] = '*';
		}
		const { content } = await this.#request('PUT', this.#url(target), hdrs, undefined, false, signal);
		if (!content) throw new Error('missing response');
		const response = XML.xml2js(content.toString('utf-8'), {
			compact: true,
			trim: true,
			ignoreDeclaration: true,
			ignoreInstruction: true,
			ignoreAttributes: true,
			ignoreComment: true,
			alwaysArray: true,
		}) as any;
		const newetag = response?.CopyObjectResult?.[0]?.ETag?.[0]?._text?.join('');
		return JSON.parse(newetag ?? '""');
	}
	async #startMultipart(resource: string, headers: Partial<AWShdrs> = {}, signal?: AbortSignal) {
		const url = this.#url(resource);
		url.search = 'uploads';
		const { content } = (await this.#request(
			'POST',
			url,
			headers,
			Buffer.alloc(0),
			false,
			signal
		)) as { content: Buffer };
		if (!content?.length) {
			throw new Error(`failed to start multi-part upload for ${resource}`);
		}
		const data = XML.xml2js(content.toString('utf-8'), {
			compact: true,
			trim: true,
			ignoreDeclaration: true,
			ignoreInstruction: true,
			ignoreAttributes: true,
			ignoreComment: true,
			alwaysArray: true,
		}) as any;
		const uploadId =
			data.InitiateMultipartUploadResult?.[0]?.UploadId?.[0]?._text?.[0];
		return uploadId;
	}
	async #uploadPart(
		resource: string,
		uploadId: string,
		num: number,
		content: Buffer,
		hdrs: Partial<AWShdrs> = {},
		signal?: AbortSignal
	): Promise<UploadPart> {
		const url = this.#url(resource);
		url.searchParams.set('partNumber', `${num}`);
		url.searchParams.set('uploadId', uploadId);
		const { headers } = await this.#request('PUT', url, hdrs, content, false, signal);
		return {
			number: num,
			etag: JSON.parse(headers.get('etag') ?? '""') as string,
		};
	}
	async #commitMultipart(
		resource: string,
		uploadId: string,
		parts: Iterable<UploadPart>,
		hdrs: Partial<AWShdrs> = {},
		signal?: AbortSignal
	) {
		const url = this.#url(resource);
		url.searchParams.set('uploadId', uploadId);
		const data = Buffer.from(Array.from(partsXML(parts)).join('\n'));
		const { content } = await this.#request(
			'POST',
			url,
			{ ...hdrs, 'Content-Type': 'application/xml; charset=UTF-8' },
			data,
			false,
			signal
		);
		if (!content) throw new Error('missing response');
		const response = XML.xml2js(content.toString('utf-8'), {
			compact: true,
			trim: true,
			ignoreDeclaration: true,
			ignoreInstruction: true,
			ignoreAttributes: true,
			ignoreComment: true,
			alwaysArray: true,
		}) as any;
		const etag =
			response?.CompleteMultipartUploadResult?.[0]?.ETag?.[0]?._text?.[0];
		return etag;
	}
	async #abortMultipart(resource: string, uploadId: string, signal?: AbortSignal) {
		const url = this.#url(resource);
		url.searchParams.set('uploadId', uploadId);
		const { headers } = await this.#request('DELETE', url, {}, undefined, false, signal);
		return headers;
	}
	async #putStream(
		name: string,
		content: AsyncIterable<Buffer>,
		type: string,
		etag?: string,
		signal?: AbortSignal
	): Promise<string> {
		const hdrs: Record<string, string> = { 'Content-Type': type };
		if (etag) {
			hdrs['If-Match'] = etag;
		} else {
			hdrs['If-None-Match'] = '*';
		}
		const store: Buffer[] = [];
		let length = 0;
		let uploadId: string | null = null;
		const parts: Promise<UploadPart | undefined>[] = [];
		try {
			const controller = new AbortController();
			for await (const chunk of content) {
				store.push(chunk);
				length += chunk.length;
				if (length > MIN_CHUNK_SIZE) {
					uploadId =
						uploadId ?? ((await this.#startMultipart(name, hdrs, signal)) as string);
					const content = Buffer.concat(store.splice(0, store.length), length);
					length = 0;
					parts.push(
						this.#uploadPart(
							name,
							uploadId as string,
							parts.length + 1,
							content,
						).catch((err) => (controller.abort(err), undefined)),
					);
					controller.signal.throwIfAborted();
				}
			}
			if (uploadId) {
				if (length) {
					const content = Buffer.concat(store, length);
					parts.push(
						this.#uploadPart(name, uploadId, parts.length + 1, content).catch(
							(err) => (controller.abort(err), undefined),
						),
					);
					controller.signal.throwIfAborted();
				}
				const allparts = await Promise.all(parts);
				controller.signal.throwIfAborted();
				const sorted = (allparts as UploadPart[]).sort(
					(a, b) => b.number - a.number,
				);
				const etag = await this.#commitMultipart(name, uploadId, sorted, hdrs);
				return etag;
			}
		} catch (err) {
			try {
				if (uploadId) {
					await this.#abortMultipart(name, uploadId, signal);
				}
			} catch (suberr) {
				throw new AggregateError([err, suberr], (err as Error).message);
			}
			throw err;
		}
		const data = Buffer.concat(store, length);
		return await this.put(name, data, hdrs['Content-Type'], hdrs['If-Match'], signal);
	}
}
export default S3;

function canonicalizeHeaders(headers: Record<string, string>) {
	const result: string[] = [];
	for (let [key, val] of Object.entries(headers)) {
		key = key.toLowerCase();
		if (!key.startsWith('x-amz') || key === 'x-amz-date') continue;
		result.push(`${key}:${val}`);
	}
	return result.sort();
}
function sign(
	secret: string,
	method: S3Method,
	md5: string,
	type: string,
	date: string,
	awsheaders: string[],
	resource: string,
) {
	const message = [method, md5, type, date, ...awsheaders, resource].join('\n');
	return CRYPTO.createHmac('sha1', secret)
		.update(Buffer.from(message, 'utf-8'))
		.digest('base64');
}

function* partXML(part: UploadPart) {
	yield '<Part>';
	if (part.sha1) yield `\t<ChecksumSHA1>${part.sha1}</ChecksumSHA1>`;
	if (part.sha256) yield `\t<ChecksumSHA256>${part.sha256}</ChecksumSHA256>`;
	yield `\t<ETag>${part.etag}</ETag>`;
	yield `\t<PartNumber>${part.number}</PartNumber>`;
	yield '</Part>';
}
function* partsXML(parts: Iterable<UploadPart>) {
	yield '<?xml version="1.0" encoding="UTF-8"?>';
	yield '<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">';
	for (const part of parts) {
		for (const line of partXML(part)) yield `\t${line}`;
	}
	yield '</CompleteMultipartUpload>';
	yield '';
}
type MimeType = `${string}/${string}`;
function isMimeType(item: any): item is MimeType {
	if ('string' !== typeof item) return false;
	return /^(?:\w|-)+\/(?:\w|-)+(?:;\scharset=[\w|-]+)?$/.test(item);
}