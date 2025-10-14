import * as HTTPS from "node:https";
import * as CRYPTO from "node:crypto";
import type { IncomingHttpHeaders } from "node:http";
import * as XML from "xml-js";

export type S3Options = {
	key: string;
	secret: string;
	endpoint: string;
	bucket: string;
	signal?: AbortSignal;
	hostBased?: boolean;
};
export type S3Method = "GET" | "PUT" | "DELETE" | "HEAD";

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

export class S3 {
	#key: string;
	#secret: string;
	#endpoint: string;
	#bucket: string;
	#signal?: AbortSignal;
	#hostBased: boolean;
	constructor(options: S3Options) {
		const {
			key,
			secret,
			endpoint,
			bucket,
			signal,
			hostBased = false,
		} = options;
		this.#key = key;
		this.#secret = secret;
		this.#endpoint = endpoint;
		this.#bucket = bucket;
		this.#signal = signal;
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
	#request(
		method: S3Method,
		url: URL,
		headers: Record<string, string>,
		content?: Buffer<ArrayBufferLike>,
	) {
		return new Promise<{
			content: Buffer<ArrayBufferLike> | null;
			headers: IncomingHttpHeaders;
		}>((resolve, reject) => {
			for (const key of url.searchParams.keys()) {
				if (!QUERY_WHITELIST.includes(key)) url.searchParams.delete(key);
			}
			const date = new Date();
			const resource = `${url.pathname}${url.search}`;
			headers.Date = date.toUTCString();
			headers.Host = url.host;
			if (content) {
				headers["Content-Length"] = `${content.byteLength}`;
				headers["Content-MD5"] = CRYPTO.createHash("md5")
					.update(content)
					.digest("base64");
				headers["Content-Type"] =
					headers["Content-Type"] ?? "application/octet-stream";
			}
			const signature = sign(
				this.#secret,
				method,
				headers["Content-MD5"] ?? "",
				headers["Content-Type"] ?? "",
				headers.Date,
				canonicalizeHeaders(headers),
				resource,
			);
			headers.Authorization = `AWS ${this.#key}:${signature}`;

			HTTPS.request(
				url,
				{
					method,
					headers,
					signal: this.#signal,
				},
				async (response) => {
					try {
						const content =
							method === "HEAD" || response.statusCode === 204
								? null
								: await body(response);
						if (!response.statusCode) return reject(new Error(`No Repsonse`));
						if (response.statusCode < 200 || response.statusCode > 399)
							return reject(new Error(`HTTP ${response.statusCode}`));
						if (response.statusCode > 299)
							return reject(new Error("cannot redirect"));
						resolve({
							content: content?.length ? content : null,
							headers: response.headers,
						});
					} catch (err) {
						return reject(err);
					}
				},
			)
				.on("error", (err) => reject(err))
				.end(content);
		});
	}
	async head(resource: string): Promise<IncomingHttpHeaders> {
		const { headers } = await this.#request("HEAD", this.#url(resource), {});
		return headers;
	}
	async get(
		resource: string,
		etag?: string,
	): Promise<Buffer<ArrayBufferLike> | null> {
		const headers: Record<string, string> = {};
		if (etag) {
			headers["If-None-Match"] = JSON.stringify(etag);
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
async function body(body: AsyncIterable<Buffer<ArrayBufferLike>>) {
	const buf = [];
	let len = 0;
	for await (const chunk of body) {
		buf.push(chunk);
		len += chunk.byteLength;
	}
	return Buffer.concat(buf, len);
}
