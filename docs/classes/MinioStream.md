# MinioStream

`iotoolz.extensions.MinioStream` is a stream interface implemented with AWS [`minio`](https://docs.min.io/docs/python-client-api-reference.html) package.

By default, `MinioStream` will try to infer the credentials from the environment variables in the following order:

- `MINIO_ACCESS_KEY` > `AWS_ACCESS_KEY_ID`
- `MINIO_SECRET_KEY` > `AWS_SECRET_ACCESS_KEY`
- `MINIO_REGION` > `AWS_REGION` > `AWS_DEFAULT_REGION`

> NOTE
>
> This is an extension module - i.e. you will need to `pip install iotoolz[minio]` before
> you can use this stream interface.
>
> Unlike `S3Stream` the uri format should include the minio endpoint:
> `minio://<endpoint>/<bucket>/<key>`

::: iotoolz.extensions.minio:MinioStream
