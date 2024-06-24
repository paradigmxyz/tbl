use arrow::datatypes::Schema;
use arrow::error::Result;
use arrow::record_batch::RecordBatch;
use futures::stream::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt; // For async writing // Standard synchronous BufWriter

/// drop columns from parquet column
pub async fn drop_parquet_columns(
    input_path: PathBuf,
    output_path: PathBuf,
    columns_to_drop: Vec<String>,
) -> Result<()> {
    let input_file = File::open(input_path).await?;
    let mut output_file = File::create(output_path).await?;

    let builder = ParquetRecordBatchStreamBuilder::new(input_file)
        .await?
        .with_batch_size(1024); // Adjust batch size as needed

    let mut reader_stream = builder.build()?;

    let schema = reader_stream.schema().clone(); // Corrected schema retrieval
    let writer_props = WriterProperties::builder().build();

    // Create an in-memory buffer for synchronous writing
    let mut buffer = Vec::new();
    let mut arrow_writer = ArrowWriter::try_new(
        BufWriter::new(&mut buffer),
        schema.clone(),
        Some(writer_props),
    )?;

    while let Some(Ok(batch)) = reader_stream.next().await {
        let new_columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                if !columns_to_drop.contains(batch.schema().field(i).name()) {
                    Some(col.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let new_schema = Arc::new(Schema::new(
            batch
                .schema()
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

        let new_batch = RecordBatch::try_new(new_schema, new_columns)?;
        arrow_writer.write(&new_batch)?;
    }

    arrow_writer.close()?;

    // Write the buffer to the output file asynchronously
    output_file.write_all(&buffer).await?;
    output_file.flush().await?;
    Ok(())
}
