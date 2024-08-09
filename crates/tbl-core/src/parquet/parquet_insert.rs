use crate::TblError;
use arrow::array::{ArrayRef, StringArray};
use arrow::array::{BinaryArray, BooleanArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::stream::{self};
use futures::StreamExt;
use hex;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use std::io::BufWriter as StdBufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

/// insert columns into multiple parquet files
#[allow(clippy::too_many_arguments)]
pub async fn insert_parquets_columns(
    inputs: &[PathBuf],
    outputs: &[PathBuf],
    column_names: Vec<String>,
    column_dtypes: Vec<DataType>,
    default_values: Option<Vec<String>>,
    index: Option<Vec<usize>>,
    batch_size: usize,
    max_concurrent: usize,
) -> Result<(), TblError> {
    if inputs.len() != outputs.len() {
        return Err(TblError::Error(
            "Number of inputs must match number of outputs".to_string(),
        ));
    }

    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    let results = stream::iter(inputs.iter().zip(outputs.iter()))
        .map(|(input, output)| {
            let sem_clone = semaphore.clone();
            let column_names = column_names.clone();
            let column_dtypes = column_dtypes.clone();
            let default_values = default_values.clone();
            let index = index.clone();

            async move {
                let _permit = sem_clone
                    .acquire()
                    .await
                    .map_err(|e| TblError::Error(e.to_string()))?;

                insert_parquet_columns(
                    input,
                    output,
                    column_names,
                    column_dtypes,
                    default_values,
                    index,
                    batch_size,
                )
                .await
            }
        })
        .buffer_unordered(max_concurrent)
        .collect::<Vec<_>>()
        .await;

    // Check if any of the operations resulted in an error
    for result in results {
        result?;
    }

    Ok(())
}

/// Insert columns into a parquet file
pub async fn insert_parquet_columns(
    input: &Path,
    output: &Path,
    column_names: Vec<String>,
    column_dtypes: Vec<DataType>,
    default_values: Option<Vec<String>>,
    index: Option<Vec<usize>>,
    batch_size: usize,
) -> Result<(), TblError> {
    if column_names.len() != column_dtypes.len() {
        return Err(TblError::Error(
            "Column names and dtypes must have the same length".to_string(),
        ));
    }

    if let Some(ref default_values) = default_values {
        if default_values.len() != column_names.len() {
            return Err(TblError::Error(
                "Default values must have the same length as column names and dtypes".to_string(),
            ));
        }
    }

    if let Some(ref index_values) = index {
        if index_values.len() != column_names.len() {
            return Err(TblError::Error(
                "Index values must have the same length as column names and dtypes".to_string(),
            ));
        }
    }

    let input_file = File::open(&input).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(input_file)
        .await?
        .with_batch_size(batch_size);
    let mut reader_stream = builder.build()?;
    let original_schema = reader_stream.schema();

    // Create new schema with inserted columns
    let mut new_fields = original_schema.fields().to_vec();
    let insert_positions = index.unwrap_or_else(|| {
        (0..column_names.len())
            .map(|i| new_fields.len() + i)
            .collect()
    });
    for (i, (name, dtype)) in column_names.iter().zip(column_dtypes.iter()).enumerate() {
        let pos = insert_positions[i];
        new_fields.insert(pos, Arc::new(Field::new(name, dtype.clone(), true)));
    }
    let new_schema = Arc::new(Schema::new(new_fields));

    let tmp_output_path = super::parquet_drop::create_tmp_target(output);
    let mut output_file = File::create(&tmp_output_path).await?;
    let mut buffer = Vec::new();

    let writer_props = WriterProperties::builder().build();
    let mut arrow_writer = ArrowWriter::try_new(
        StdBufWriter::new(&mut buffer),
        new_schema.clone(),
        Some(writer_props),
    )?;

    while let Some(batch) = reader_stream.next().await {
        let batch = batch?;
        let mut new_columns = batch.columns().to_vec();

        for (i, dtype) in column_dtypes.iter().enumerate() {
            let pos = insert_positions[i];
            let default_value = default_values.as_ref().map(|values| values[i].as_str());
            let new_column = create_new_column(batch.num_rows(), dtype, default_value)?;
            new_columns.insert(pos, new_column);
        }

        let new_batch = RecordBatch::try_new(new_schema.clone(), new_columns)?;
        arrow_writer.write(&new_batch)?;
    }

    arrow_writer.close()?;
    output_file.write_all(&buffer).await?;
    output_file.flush().await?;
    tokio::fs::rename(tmp_output_path, output).await?;

    Ok(())
}

fn create_new_column(
    len: usize,
    dtype: &DataType,
    default_value: Option<&str>,
) -> Result<ArrayRef, TblError> {
    match dtype {
        DataType::Int32 => {
            let value = default_value
                .map(|v| v.parse::<i32>().map_err(|e| TblError::Error(e.to_string())))
                .transpose()?;
            Ok(Arc::new(arrow::array::Int32Array::from(vec![value; len])))
        }
        DataType::Int64 => {
            let value = default_value
                .map(|v| v.parse::<i64>().map_err(|e| TblError::Error(e.to_string())))
                .transpose()?;
            Ok(Arc::new(arrow::array::Int64Array::from(vec![value; len])))
        }
        DataType::UInt32 => {
            let value = default_value
                .map(|v| v.parse::<u32>().map_err(|e| TblError::Error(e.to_string())))
                .transpose()?;
            Ok(Arc::new(UInt32Array::from(vec![value; len])))
        }
        DataType::UInt64 => {
            let value = default_value
                .map(|v| v.parse::<u64>().map_err(|e| TblError::Error(e.to_string())))
                .transpose()?;
            Ok(Arc::new(UInt64Array::from(vec![value; len])))
        }
        DataType::Float32 => {
            let value = default_value
                .map(|v| v.parse::<f32>().map_err(|e| TblError::Error(e.to_string())))
                .transpose()?;
            Ok(Arc::new(arrow::array::Float32Array::from(vec![value; len])))
        }
        DataType::Float64 => {
            let value = default_value
                .map(|v| v.parse::<f64>().map_err(|e| TblError::Error(e.to_string())))
                .transpose()?;
            Ok(Arc::new(arrow::array::Float64Array::from(vec![value; len])))
        }
        DataType::Utf8 => {
            let value = default_value.unwrap_or("");
            Ok(Arc::new(StringArray::from(vec![value; len])))
        }
        DataType::Binary => {
            let value = default_value
                .map(|v| {
                    if let Some(stripped) = v.strip_prefix("0x") {
                        hex::decode(stripped).map_err(|e| TblError::Error(e.to_string()))
                    } else {
                        Err(TblError::Error(
                            "Binary default value must start with '0x'".to_string(),
                        ))
                    }
                })
                .transpose()?
                .unwrap_or_else(Vec::new);
            Ok(Arc::new(BinaryArray::from(vec![
                Some(value.as_slice());
                len
            ])))
        }
        DataType::Boolean => {
            let value = default_value
                .map(|v| {
                    v.parse::<bool>()
                        .map_err(|e| TblError::Error(e.to_string()))
                })
                .transpose()?;
            Ok(Arc::new(BooleanArray::from(vec![value; len])))
        }
        // Add more data types as needed
        _ => Err(TblError::Error(format!(
            "Unsupported data type: {:?}",
            dtype
        ))),
    }
}
