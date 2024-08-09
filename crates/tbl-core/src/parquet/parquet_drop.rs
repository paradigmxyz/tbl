use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::stream::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// create temporary path target similar to the final target path
pub fn create_tmp_target(path: &std::path::Path) -> PathBuf {
    let mut new_path = path.to_path_buf();
    let suffix = "_tmp";
    if let Some(stem) = path.file_stem() {
        let mut new_stem = stem.to_string_lossy().into_owned();
        new_stem.push_str(suffix);
        if let Some(extension) = path.extension() {
            new_stem.push('.');
            new_stem.push_str(&extension.to_string_lossy());
        }
        new_path.set_file_name(new_stem);
    }

    new_path
}

/// drop columns from parquet column
pub async fn drop_parquet_columns(
    input_path: PathBuf,
    output_path: PathBuf,
    columns_to_drop: Vec<String>,
    batch_size: usize,
) -> Result<(), crate::TblError> {
    let input_file = File::open(input_path).await?;
    let tmp_output_path = create_tmp_target(output_path.as_path());
    let mut output_file = File::create(&tmp_output_path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(input_file)
        .await?
        .with_batch_size(batch_size);
    let mut reader_stream = builder.build()?;
    let original_schema = reader_stream.schema().clone();

    // Create new schema without dropped columns
    let new_schema = Arc::new(Schema::new(
        original_schema
            .fields()
            .iter()
            .filter_map(|field| {
                if !columns_to_drop.contains(field.name()) {
                    Some(field.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    ));

    let writer_props = WriterProperties::builder().build();
    let mut buffer = Vec::new();
    let mut arrow_writer = ArrowWriter::try_new(
        BufWriter::new(&mut buffer),
        new_schema.clone(),
        Some(writer_props),
    )?;

    while let Some(batch) = reader_stream.next().await {
        let batch = batch?;
        let new_columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                if !columns_to_drop.contains(original_schema.field(i).name()) {
                    Some(col.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let new_batch = RecordBatch::try_new(new_schema.clone(), new_columns)?;
        arrow_writer.write(&new_batch)?;
    }

    arrow_writer.close()?;
    output_file.write_all(&buffer).await?;
    output_file.flush().await?;

    std::fs::rename(tmp_output_path, output_path)?;

    Ok(())
}

/// drop columns from multiple parquet files
pub async fn drop_parquets_columns(
    input_output_paths: Vec<(PathBuf, PathBuf)>,
    columns_to_drop: Vec<String>,
    batch_size: usize,
    max_concurrent: usize,
) -> Result<(), crate::TblError> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

    let results = futures::stream::iter(input_output_paths)
        .map(|(input, output)| {
            let columns_to_drop = columns_to_drop.clone();
            let sem = Arc::clone(&semaphore);
            async move {
                let _permit = sem.acquire().await?;
                drop_parquet_columns(input, output, columns_to_drop, batch_size).await
            }
        })
        .buffer_unordered(max_concurrent)
        .collect::<Vec<_>>()
        .await;

    // Check if any operations failed
    for result in results {
        result?;
    }

    Ok(())
}
