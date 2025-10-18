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

export type AWSProperty = 'If-Match' | 'Content-Type';
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
		} as any as RequestInit);

		const bytes = (await response.bytes()) ?? null;
		if (!response.ok) {
			throw Object.assign(
				new Error(
					`HTTP(${response.status}) ${response.statusText} [${method} ${url}]`,
				),
				{ text: bytes ? Buffer.from(bytes).toString('utf-8') : undefined },
			);
		}
		const content = bytes ? Buffer.from(bytes) : null;
		const headers = response.headers;
		return { headers, content };
	}
	async head(resource: string): Promise<Headers> {
		const { headers } = await this.#request('HEAD', this.#url(resource), {});
		return headers;
	}
	async get(
		resource: string,
		etag?: string,
	): Promise<Buffer<ArrayBufferLike> | null> {
		const headers: Record<string, string> = {};
		if (etag) {
			headers['If-None-Match'] = etag;
		}
		const { content } = await this.#request(
			'GET',
			this.#url(resource),
			headers,
		);
		return content;
	}
	async put(
		resource: string,
		content:
			| AsyncIterable<Buffer<ArrayBufferLike>>
			| Buffer<ArrayBufferLike>
			| string
			| object,
		hdrs: Partial<AWShdrs> = {},
	) {
		if ((content as any)[Symbol.asyncIterator])
			return await this.#putStream(
				resource,
				content as AsyncIterable<Buffer<ArrayBufferLike>>,
				hdrs,
			);
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
			content as Buffer<ArrayBufferLike>,
		);
		return JSON.parse(headers.get('etag') as string);
	}
	async del(resource: string) {
		const hdrs: Record<string, string> = {};
		const { headers } = await this.#request(
			'DELETE',
			this.#url(resource),
			hdrs,
		);
		return headers;
	}
	async #list(
		prefix?: string,
		options?: Record<string, string>,
		continuation?: string,
	): Promise<{
		continuation?: string;
		items: { name: string; size: number; modified: number }[];
	}> {
		const url = this.#url('/');
		if (options) {
			for (const [key, val] of Object.entries(options)) {
				url.searchParams.set(key, val);
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

		const { content } = await this.#request('GET', url, {});
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
				return { name, size, modified };
			}),
		};
	}
	async *list(prefix?: string, options?: Record<string, string>) {
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
	async copy(source: string, target: string, hdrs: Partial<AWShdrs> = {}) {
		source = [this.#bucket, ...source.split(/\/+/)]
			.filter((x) => (x = x.trim()))
			.join('/');
		const { content } = await this.#request('PUT', this.#url(target), {
			...hdrs,
			'x-amz-copy-source': source,
		});
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
		const etag = response?.CopyObjectResult?.[0]?.ETag?.[0]?._text?.[0];
		return JSON.parse(etag ?? '""');
	}
	async #startMultipart(resource: string, headers: Partial<AWShdrs> = {}) {
		const url = this.#url(resource);
		url.search = 'uploads';
		const { content } = await this.#request(
			'POST',
			url,
			headers,
			Buffer.alloc(0),
		);
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
		content: Buffer<ArrayBufferLike>,
		hdrs: Partial<AWShdrs> = {},
	) {
		const url = this.#url(resource);
		url.searchParams.set('partNumber', `${num}`);
		url.searchParams.set('uploadId', uploadId);
		const { headers } = await this.#request('PUT', url, hdrs, content);
		return JSON.parse(headers.get('etag') as string);
	}
	async #commitMultipart(
		resource: string,
		uploadId: string,
		parts: Iterable<UploadPart>,
		hdrs: Partial<AWShdrs> = {},
	) {
		const url = this.#url(resource);
		url.searchParams.set('uploadId', uploadId);
		const data = Buffer.from(Array.from(partsXML(parts)).join('\n'));
		const { content } = await this.#request(
			'POST',
			url,
			{ ...hdrs, 'Content-Type': 'application/xml; charset=UTF-8' },
			data,
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
	async #abortMultipart(resource: string, uploadId: string) {
		const url = this.#url(resource);
		url.searchParams.set('uploadId', uploadId);
		const { headers } = await this.#request('DELETE', url, {});
		return headers;
	}
	async #putStream(
		name: string,
		content: AsyncIterable<Buffer<ArrayBufferLike>>,
		hdrs: Partial<AWShdrs> = {},
	): Promise<string> {
		const store: Buffer<ArrayBufferLike>[] = [];
		let length = 0;
		let uploadId: string | null = null;
		const parts: UploadPart[] = [];
		try {
			for await (const chunk of content) {
				store.push(chunk);
				length += chunk.length;
				if (length > MIN_CHUNK_SIZE) {
					uploadId = uploadId ?? ((await this.#startMultipart(name)) as string);
					const content = Buffer.concat(store.splice(0, store.length), length);
					length = 0;
					const number = parts.length + 1;
					const etag = await this.#uploadPart(
						name,
						uploadId as string,
						number,
						content,
					);
					parts.push({ etag, number });
				}
			}
			if (uploadId) {
				if (length) {
					const content = Buffer.concat(store, length);
					const number = parts.length + 1;
					const etag = await this.#uploadPart(name, uploadId, number, content);
					parts.push({ etag, number });
				}
				const etag = await this.#commitMultipart(name, uploadId, parts, hdrs);
				return etag;
			}
		} catch (err) {
			try {
				if (uploadId) {
					await this.#abortMultipart(name, uploadId);
				}
			} catch (suberr) {
				throw new AggregateError([err, suberr], (err as Error).message);
			}
			throw err;
		}
		const data = Buffer.concat(store, length);
		return await this.put(name, data, hdrs);
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
