use std::sync::{atomic::AtomicUsize, Arc};

use clap::{arg, command, Parser};
use futures::{stream, StreamExt};
use object_store::{path::Path, ObjectStore, PutPayload, RetryConfig};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the bucket
    #[arg(short, long)]
    bucket: String,

    /// prefix
    #[arg(short, long)]
    prefix: String,

    #[arg(short, long, default_value = "10485760")]
    chunk_size: usize,

    #[arg(short, long, default_value = "10")]
    concurrent_files: usize,

    #[arg(short, long, default_value = "4")]
    concurrent_chunks_per_file: usize,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();

    let store = Arc::new(object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(args.bucket.clone())
        .build()
        .expect("Failed to build s3 store"));

    let file_name_stream = store.list(Some(&Path::parse(args.prefix.clone()).expect("Failed to parse prefix")));

    let global_size_counter = Arc::new(AtomicUsize::new(0));

    let global_size_counter_clone = global_size_counter.clone();
    let start = std::time::Instant::now();

    tokio::spawn(async move {
        loop {
            let size = global_size_counter_clone.load(std::sync::atomic::Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            log::info!("Speed: {} MiB/s", size as f64 / 1024.0 / 1024.0 / elapsed);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    });

    file_name_stream.map(|object| {
        let object = object.expect("Failed to get object");

        log::debug!("Downloading {:?}", object.location);

        let store = store.clone();
        let prefix = args.prefix.clone();
        let global_size_counter = global_size_counter.clone();
        let fut = async move {
            let src = object.location;
            let dest = Path::parse(
                src.to_string().strip_prefix(&prefix.clone()).expect("Failed to strip prefix").to_string()
            ).expect("Failed to parse path");
            let size = object.size;
            let mut res = stream::iter((0..size).step_by(args.chunk_size)).map(|start| {
                let end = std::cmp::min(start + args.chunk_size, size);
                let store = store.clone();
                let src = src.clone();
                async move {
                    store.get_range(&src, start..end).await.expect("Failed to get range")
                }
            }).buffered(args.concurrent_chunks_per_file);
            let local_store = object_store::local::LocalFileSystem::new_with_prefix(".").unwrap();
            let mut upload_stream = local_store.put_multipart(&dest).await.expect("Failed to start upload stream");
            while let Some(data) = res.next().await {
                log::debug!("Uploading part: {:?}, size: {:?}", dest, data.len());
                global_size_counter.fetch_add(data.len(), std::sync::atomic::Ordering::Relaxed);
                upload_stream.put_part(PutPayload::from_bytes(data)).await.unwrap();
            }
            upload_stream.complete().await.expect("failed to complete upload");
        };

        tokio::spawn(fut)
    }).buffer_unordered(args.concurrent_files).map(|x| x.unwrap()).collect::<Vec<_>>().await;

}
