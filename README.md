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

© 2025 Philipp Dunkel <pip@pipobscure.com> [EUPL v1.2](https://eupl.eu/1.2/en)
