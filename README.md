# @pipobscure/s3

This is a simple S3 library because the one from amazon has so many dependencies and is so heavy that it's no longer auditable and exceeds reason.

This is simple and low on dependecies.

## API

```typescript
import { S3 } from '@pipobscure/s3';

const s3options: {
	key: string; // AWS-Key
	secret: string; // AWS-Secret
	endpoint: string; // the endpoint to use (if host based does not include the bucket)
	bucket: string; // the bucket name
	signal?: AbortSignal; // an optional AbortSignal
	hostBased?: boolean; // use hostbased API (defaults to path-based)
} = {...};
const s3client = new S3(s3options);

const content = await s3client.get('/my/resource/name.file');
```
## License

ISC License

Copyright 2024 Philipp Dunkel

Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
