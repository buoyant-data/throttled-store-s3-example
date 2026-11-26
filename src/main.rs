//! Demonstrates pacing deltalake-core requests against an S3-compatible endpoint
//! by wrapping the object store in `object_store::throttle::ThrottledStore`.
//!
//! The table is created in the bucket, overwritten a few times so that superseded
//! data files accumulate, and then vacuumed with zero retention. Every delete
//! vacuum issues goes through the throttled store.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use deltalake_core::arrow::array::{Int64Array, StringArray};
use deltalake_core::arrow::datatypes::{DataType, Field, Schema};
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::kernel::{DataType as KernelType, StructField};
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaTable, DeltaTableBuilder};
use object_store::aws::AmazonS3Builder;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use url::Url;

const BUCKET: &str = "delta-throttle-test";
const TABLE_PREFIX: &str = "events";
const OVERWRITES: usize = 5;
const ROWS_PER_WRITE: i64 = 2_000;

fn parse_delete_delay_ms() -> Result<u64> {
    let mut args = std::env::args().skip(1);
    let mut delay = 0u64;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--delete-delay-ms" => {
                delay = args
                    .next()
                    .context("--delete-delay-ms needs a value")?
                    .parse()
                    .context("--delete-delay-ms must be an integer")?;
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }
    Ok(delay)
}

/// Build an S3 store pointed at the endpoint in `S3_ENDPOINT` (LocalStack by default).
/// Credentials come from the standard AWS_* environment variables.
fn build_s3_store() -> Result<object_store::aws::AmazonS3> {
    let endpoint = std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".into());
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "test".into()))
        .with_secret_access_key(
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "test".into()),
        )
        .build()?;
    Ok(store)
}

fn table_url() -> Result<Url> {
    Ok(Url::parse(&format!("s3://{BUCKET}/{TABLE_PREFIX}"))?)
}

fn batch(offset: i64) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids = Int64Array::from_iter_values(offset..offset + ROWS_PER_WRITE);
    let payloads = StringArray::from_iter_values(
        (offset..offset + ROWS_PER_WRITE).map(|i| format!("event-{i}")),
    );
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(payloads)],
    )?)
}

/// Build a `DeltaTable` whose object store is the throttled wrapper.
fn open_table(store: Arc<ThrottledStore<object_store::aws::AmazonS3>>) -> Result<DeltaTable> {
    let url = table_url()?;
    let table = DeltaTableBuilder::from_url(url.clone())?
        .with_storage_backend(store, url)
        .build()?;
    Ok(table)
}

#[tokio::main]
async fn main() -> Result<()> {
    // The custom-store path in deltalake-core bypasses the scheme registry, but the
    // log store lookup still needs the `s3` scheme registered by deltalake-aws.
    deltalake_aws::register_handlers(None);

    let delete_delay = Duration::from_millis(parse_delete_delay_ms()?);

    let throttle = ThrottleConfig {
        wait_delete_per_call: delete_delay,
        ..Default::default()
    };
    let store = Arc::new(ThrottledStore::new(build_s3_store()?, throttle));

    // Create the table with the same schema the writes use.
    let table = open_table(store.clone())?;
    let table = table
        .create()
        .with_table_name("events")
        .with_columns(vec![
            StructField::new("id", KernelType::LONG, false),
            StructField::new("payload", KernelType::STRING, false),
        ])
        .await
        .context("creating table")?;
    println!("created table at {}", table.table_url());

    // Overwrite repeatedly so the superseded data files are eligible for vacuum.
    let mut table = table;
    for i in 0..OVERWRITES {
        let batches = vec![batch(i as i64 * ROWS_PER_WRITE)?];
        table = table
            .write(batches)
            .with_save_mode(SaveMode::Overwrite)
            .await
            .context("writing batch")?;
        println!("wrote overwrite {} (version {:?})", i + 1, table.version());
    }

    // Vacuum with zero retention so every superseded file is a candidate.
    let started = Instant::now();
    let (table, metrics) = table
        .vacuum()
        .with_retention_period(chrono::Duration::zero())
        .with_enforce_retention_duration(false)
        .with_dry_run(false)
        .await
        .context("vacuum")?;
    let elapsed = started.elapsed();

    println!(
        "vacuum: delete_delay={}ms files_deleted={} elapsed={:.2}s table_version={:?}",
        delete_delay.as_millis(),
        metrics.files_deleted.len(),
        elapsed.as_secs_f64(),
        table.version()
    );
    Ok(())
}
