# throttled-store-s3-example

Demonstrates wrapping an S3 `ObjectStore` in `object_store::throttle::ThrottledStore`
and passing it to `deltalake-core` so that vacuum is paced. Runs against LocalStack's
S3-compatible API.

Pinned versions: `deltalake-core` 1.1.4, `deltalake-aws` 1.0.0, `object_store` 0.13.2,
and `localstack/localstack:4.11.1`. Later LocalStack images require a paid auth token,
so the pin is deliberate.

## Run

    docker compose up -d
    export S3_ENDPOINT=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_ALLOW_HTTP=true
    cargo build --release
    ./target/release/throttled-store-s3-example --delete-delay-ms 0
    # the table is created on every run, so clear the prefix first
    aws --endpoint-url http://localhost:4566 s3 rm s3://delta-throttle-test/events --recursive
    ./target/release/throttled-store-s3-example --delete-delay-ms 50

The program creates a table, overwrites it five times so superseded files accumulate,
then vacuums with zero retention. `--delete-delay-ms` sets
`ThrottleConfig::wait_delete_per_call`.

## Results (LocalStack 4.11.1, local machine)

| delete delay | files deleted | vacuum elapsed |
| --- | --- | --- |
| 0 ms | 4 | 0.05 s |
| 50 ms | 4 | 0.25 s |

The 50 ms run adds about 200 ms for four deletes, which matches the configured delay.

LocalStack does not enforce S3 request rate limits, so this shows the wiring and the
pacing, not a fix for real `503 SlowDown` responses. The timings come from one run each
on one machine.

## Notes

- `deltalake_aws::register_handlers` must run before the table is opened. The custom-store
  path in deltalake-core bypasses the scheme registry, but the log store lookup still
  needs the `s3` scheme registered.
- `DeltaTableBuilder::with_storage_backend` takes the wrapped store. Vacuum reads its store
  from the log store, so the wrapper covers the vacuum deletes.
