import * as CRYPTO from "node:crypto";
import * as XML from "xml-js";

export type S3Options = {
	key: string;
	secret: string;
	endpoint: string;
	bucket: string;
	hostBased?: boolean;
};
export type S3Method = "GET" | "PUT" | "DELETE" | "HEAD" | "POST";

const QUERY_WHITELIST = [
	"acl",
	"delete",
	"lifecycle",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"requestPayment",
	"torrent",
	"uploadId",
	"uploads",
	"versionId",
	"versioning",
	"versions",
	"website",
];

export type AWSProperty = "If-Match";
export type AWSProperties = Record<AWSProperty, string>;
export type UploadPart = {
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
		const {
			key,
			secret,
			endpoint,
			bucket,
			hostBased = false,
		} = options;
		this.#key = key;
		this.#secret = secret;
		this.#endpoint = endpoint;
		this.#bucket = bucket;
		this.#hostBased = hostBased;
	}
	#url(filename: string) {
		if (this.#hostBased) {
			return new URL(
				`/${filename}`.split(/\/+/).join("/"),
				`https://${this.#bucket}.${this.#endpoint}`,
			);
		} else {
			return new URL(
				`/${this.#bucket}/${filename}`.split(/\/+/).join("/"),
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
		for (const key of url.searchParams.keys()) {
			if (!QUERY_WHITELIST.includes(key)) url.searchParams.delete(key);
		}
		const date = new Date();
		const resource = `${url.pathname}${url.search}`;
		hdrs.Date = date.toUTCString();
		hdrs.Host = url.host;
		if (sendbody) {
			hdrs["Content-Length"] = `${sendbody.byteLength}`;
			hdrs["Content-MD5"] = CRYPTO.createHash("md5").update(sendbody).digest("base64");
			hdrs["Content-Type"] = hdrs["Content-Type"] ?? "application/octet-stream";
		}
		const signature = sign(
			this.#secret,
			method,
			hdrs["Content-MD5"] ?? "",
			hdrs["Content-Type"] ?? "",
			hdrs.Date,
			canonicalizeHeaders(hdrs),
			resource,
		);
		hdrs.Authorization = `AWS ${this.#key}:${signature}`;
		const body = sendbody as (Uint8Array | null);
		const response = await fetch(url, { method, headers: hdrs, body } as any as RequestInit);
		if (!response.ok) throw new Error(`HTTP(${response.status}) ${response.statusText}`);
		const bytes = (await response.bytes()) ?? null;
		const content = bytes ? Buffer.from(bytes) : null;
		const headers = response.headers;
		return { headers, content };
	}
	async head(resource: string): Promise<Headers> {
		const { headers } = await this.#request("HEAD", this.#url(resource), {});
		return headers;
	}
	async get(
		resource: string,
		etag?: string,
	): Promise<Buffer<ArrayBufferLike> | null> {
		const headers: Record<string, string> = {};
		if (etag) {
			headers["If-None-Match"] = etag;
		}
		const { content } = await this.#request(
			"GET",
			this.#url(resource),
			headers,
		);
		return content;
	}
	async put(
		resource: string,
		content: Buffer<ArrayBufferLike> | string | any,
		properties: Partial<AWSProperties & { type: string }> = {},
	) {
		let { type, ...hdrs } = properties;
		if ("string" === typeof content) {
			content = Buffer.from(content, "utf-8");
			type = type ?? "text/plain; charset=utf-8";
		}
		if (!Buffer.isBuffer(content)) {
			content = Buffer.from(JSON.stringify(content), "utf-8");
			type = type ?? "application/json; charset=utf-8";
		}
		const { headers } = await this.#request(
			"PUT",
			this.#url(resource),
			Object.assign(hdrs, { type: type ?? "application/octet-stream" }),
			content,
		);
		return headers;
	}
	async del(resource: string) {
		const hdrs: Record<string, string> = {};
		const { headers } = await this.#request(
			"DELETE",
			this.#url(resource),
			hdrs,
		);
		return headers;
	}
	async list(
		prefix?: string,
		delimiter?: string,
		options?: Record<string, string>,
	): Promise<{ name: string; size: number; modified: number }[]> {
		const url = this.#url("/");
		if (prefix) {
			url.searchParams.set("prefix", prefix);
			url.searchParams.set("delimiter", delimiter ?? "/");
		}
		if (options) {
			for (const [key, val] of Object.entries(options))
				url.searchParams.set(key, val);
		}
		const { content } = await this.#request("GET", url, {});
		if (!content?.length)
			throw new Error(`failed to list objects in ${prefix}`);
		const data = XML.xml2js(content.toString("utf-8"), {
			compact: true,
			trim: true,
			ignoreDeclaration: true,
			ignoreInstruction: true,
			ignoreAttributes: true,
			ignoreComment: true,
			alwaysArray: true,
		}) as any;
		const contents = data?.ListBucketResult?.[0]?.Contents ?? [];
		return contents.map((item: any) => {
			const name = item?.Key?.[0]?._text?.join("");
			const modified =
				item?.LastModified?.[0]?._text?.join("") ?? new Date().toISOString();
			const size = +(item?.Size?.[0]?._text?.join("") ?? 0);
			return { name, size, modified };
		});
	}
	async copy(
		resource: string,
		source: string,
		properties?: Partial<AWSProperties>,
	) {
		const result = await this.#request(
			"PUT",
			this.#url(resource),
			Object.assign(
				{
					"x-amz-copy-source": source,
				},
				properties ?? {},
			),
		);
		if (result.content?.length) {
			return Object.assign(
				XML.xml2json(result.content.toString("utf-8")),
				result.headers,
			);
		} else {
			return result.headers;
		}
	}
	async startMultipart(resource: string, properties: Partial<AWSProperties & { type: string }> = {}) {
		const url = this.#url(resource);
		url.searchParams.set('uploads', '');
		const { type, ...hdrs } = properties;
		const { content } = await this.#request("POST", url, { ...hdrs, type: type ?? 'application/octet-stream' });
		if (!content?.length) throw new Error(`failed to start multi-part upload for ${resource}`);
		const data = XML.xml2js(content.toString("utf-8"), {
			compact: true,
			trim: true,
			ignoreDeclaration: true,
			ignoreInstruction: true,
			ignoreAttributes: true,
			ignoreComment: true,
			alwaysArray: true,
		}) as any;
		const uploadId = data.InitiateMultipartUploadResult?.[0]?.UploadId;
		return uploadId;
	}
	async uploadPart(resource: string, uploadId: string, num: number, content: Buffer<ArrayBufferLike>, properties: Partial<AWSProperties> = {}) {
		const url = this.#url(resource);
		url.searchParams.set('partNumber', `${num}`);
		url.searchParams.set('uploadId', uploadId);
		const { headers } = await this.#request("PUT", url, properties, content);
		return headers.get('etag') as string;
	}
	async commitMultipart(resource: string, uploadId: string, parts: Iterable<UploadPart>, properties: Partial<AWSProperties> = {}) {
		const url = this.#url(resource);
		url.searchParams.set('uploadId', uploadId);
		const content = Buffer.from(Array.from(partsXML(parts)).join(''));
		await this.#request("POST", url, properties, content);
	}
	async abortMultipart(resource: string, uploadId: string, properties: Partial<AWSProperties> = {}) {
		const url = this.#url(resource);
		url.searchParams.set('uploadId', uploadId);
		await this.#request("DELETE", url, properties);
	}
	static async create(options: S3Options) {
		const url = makeCreateURL(options);
		const body = Buffer.from(`<?xml version="1.0" encoding="UTF-8"?>\n<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`);
		const date = new Date();
		const headers: Record<string, string> = {};
		headers.Date = date.toUTCString();
		headers.Host = url.host;
		headers["Content-Length"] = `${body.byteLength}`;
		headers["Content-MD5"] = CRYPTO.createHash("md5")
			.update(body)
			.digest("base64");
		headers["Content-Type"] = 'text/xml; charset=UTF-8';
		const signature = sign(
			options.secret,
			"PUT",
			headers["Content-MD5"] ?? "",
			headers["Content-Type"] ?? "",
			headers.Date,
			canonicalizeHeaders(headers),
			`${url.pathname}${url.search}`,
		);
		headers.Authorization = `AWS ${options.key}:${signature}`;
		console.error(url.toString(), headers);
		const response = await fetch(url, { method: 'PUT', headers, body });
		if (!response.ok) throw new Error(`http-status(${response.status}) ${response.statusText}`);
		return options;
	}
}

function canonicalizeHeaders(headers: Record<string, string>) {
	const result: [string, string][] = [];
	for (let [key, val] of Object.entries(headers)) {
		key = key.toLowerCase();
		if (!key.startsWith("x-amz") || key === "x-amz-date") continue;
		result.push([key, val]);
	}
	result.sort((a, b) => (a[0] > b[0] ? 1 : -1));
	return result.map((...args) => args.join(":"));
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
	const message = [method, md5, type, date, ...awsheaders, resource].join("\n");
	return CRYPTO.createHmac("sha1", secret)
		.update(Buffer.from(message, "utf-8"))
		.digest("base64");
}

function* partXML(part: UploadPart) {
	yield '<Part>';
	if (part.sha1) yield `<ChecksumSHA1>${part.sha1}</ChecksumSHA1>`;
	if (part.sha256) yield `<ChecksumSHA256>${part.sha256}</ChecksumSHA256>`;
	yield `<ETag>${part.etag}</ETag>`;
	yield `<PartNumber>${part.number}</PartNumber>`;
	yield '</Part>';
}
function* partsXML(parts: Iterable<UploadPart>) {
	yield '<?xml version="1.0" encoding="UTF-8"?>';
	yield '<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">';
	for (const part of parts) yield* partXML(part);
	yield '</CompleteMultipartUpload>';
}

function makeCreateURL(options: S3Options) {
	if (options.hostBased) {
		return new URL(
			`/`,
			`https://${options.bucket}.${options.endpoint}`,
		);
	} else {
		return new URL(
			`/${options.bucket}`.split(/\/+/).join("/"),
			`https://${options.endpoint}`,
		);
	}
}
