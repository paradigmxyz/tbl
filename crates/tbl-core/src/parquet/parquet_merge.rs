use crate::TblError;
use futures::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use std::io::BufWriter as StdBufWriter;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// merge parquet files into one
pub async fn merge_parquets(
    input_paths: &Vec<PathBuf>,
    output_path: &PathBuf,
    batch_size: usize,
) -> Result<(), crate::TblError> {
    if input_paths.is_empty() {
        return Err(crate::TblError::Error(
            "No input files provided".to_string(),
        ));
    }

    let tmp_output_path = super::parquet_drop::create_tmp_target(output_path.as_path());
    let mut output_file = File::create(&tmp_output_path).await?;
    let mut buffer = Vec::new();

    // Read the schema from the first file
    let first_file = File::open(&input_paths[0]).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(first_file)
        .await?
        .with_batch_size(batch_size);
    let schema = builder.schema().clone();

    let writer_props = WriterProperties::builder().build();
    let mut arrow_writer = ArrowWriter::try_new(
        StdBufWriter::new(&mut buffer),
        schema.clone(),
        Some(writer_props),
    )?;

    for input_path in input_paths {
        let input_file = File::open(input_path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(input_file)
            .await?
            .with_batch_size(batch_size);
        let mut reader_stream = builder.build()?;

        // Verify that the schema matches
        if reader_stream.schema() != &schema {
            println!("SCHEMA OF {}:", input_paths[0].to_string_lossy());
            println!("{:?}", schema);
            println!();
            println!("SCHEMA OF {}:", input_path.to_string_lossy());
            println!("{:?}", reader_stream.schema());
            return Err(TblError::SchemaError(
                "schemas of files are not equal".to_string(),
            ));
        }

        while let Some(batch) = reader_stream.next().await {
            let batch = batch?;
            arrow_writer.write(&batch)?;
        }
    }

    arrow_writer.close()?;
    output_file.write_all(&buffer).await?;
    output_file.flush().await?;
    tokio::fs::rename(tmp_output_path, output_path).await?;

    Ok(())
}
