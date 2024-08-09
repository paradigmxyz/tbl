use crate::{DataArgs, TblCliError};
use polars::prelude::*;
use std::str::FromStr;

pub(crate) fn apply_transformations(
    lf: LazyFrame,
    args: &DataArgs,
) -> Result<LazyFrame, TblCliError> {
    let lf = apply_with_columns(lf, args.with_columns.as_deref())?;
    let lf = apply_filter(lf, args.filter.as_deref())?;
    let lf = apply_drop(lf, args.drop.as_deref())?;
    let lf = apply_cast(lf, args.cast.as_deref())?;
    let lf = apply_select(lf, args.columns.as_deref())?;
    let lf = apply_head(lf, args.head)?;
    let lf = apply_tail(lf, args.tail)?;
    let lf = apply_offset(lf, args.offset)?;
    let lf = apply_value_counts(lf, args.value_counts.as_deref())?;
    let lf = apply_sort(lf, args.sort.as_deref())?;
    let lf = apply_rename(lf, args.rename.as_deref())?;
    Ok(lf)
}

pub(crate) fn apply_with_columns(
    lf: LazyFrame,
    columns: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    match columns {
        None => Ok(lf),
        Some(columns) => {
            let mut new_lf = lf;
            for col_spec in columns {
                new_lf = new_lf.with_column(parse_new_column_expr(col_spec)?);
            }
            Ok(new_lf)
        }
    }
}

fn parse_new_column_expr(col_spec: &str) -> Result<Expr, TblCliError> {
    let parts: Vec<&str> = col_spec.splitn(3, ':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(TblCliError::Error(
            "invalid format for with_column".to_string(),
        ));
    }
    let (name, type_str) = (parts[0], parts[1]);
    let value_str = parts.get(2).and_then(|s| s.split('=').nth(1));
    let dtype = parse_dtype(type_str)?;
    let expr = if let Some(value) = value_str {
        create_value_expr(value, &dtype)?
    } else {
        lit(NULL).cast(dtype)
    };
    let expr = expr.alias(name);
    Ok(expr)
}

fn parse_dtype(type_str: &str) -> Result<DataType, TblCliError> {
    match type_str.to_lowercase().as_str() {
        "i8" => Ok(DataType::Int8),
        "i16" => Ok(DataType::Int16),
        "i32" => Ok(DataType::Int32),
        "i64" => Ok(DataType::Int64),
        "u8" => Ok(DataType::UInt8),
        "u16" => Ok(DataType::UInt16),
        "u32" => Ok(DataType::UInt32),
        "u64" => Ok(DataType::UInt64),
        "f32" => Ok(DataType::Float32),
        "f64" => Ok(DataType::Float64),
        "bool" => Ok(DataType::Boolean),
        "str" => Ok(DataType::String),
        "date" => Ok(DataType::Date),
        "datetime" => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        _ => Err(TblCliError::Error("invalid data type".to_string())),
    }
}

fn create_value_expr(value: &str, dtype: &DataType) -> Result<Expr, TblCliError> {
    match dtype {
        DataType::Int8 => Ok(lit(
            i8::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::Int16 => Ok(lit(
            i16::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::Int32 => Ok(lit(
            i32::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::Int64 => Ok(lit(
            i64::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::UInt8 => Ok(lit(
            u8::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::UInt16 => Ok(lit(
            u16::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::UInt32 => Ok(lit(
            u32::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::UInt64 => Ok(lit(
            u64::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::Float32 => Ok(lit(
            f32::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::Float64 => Ok(lit(
            f64::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::Boolean => Ok(lit(
            bool::from_str(value).map_err(|_| TblCliError::Error(value.to_string()))?
        )),
        DataType::String => Ok(lit(value.to_string())),
        DataType::Date => {
            let naive_date =
                chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
                    TblCliError::Error("set default date string as %Y-%m-%d".to_string())
                })?;
            Ok(lit(naive_date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis()))
        }
        DataType::Datetime(_, _) => {
            let naive_datetime = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| TblCliError::Error(value.to_string()))?;
            Ok(lit(naive_datetime.and_utc().timestamp_millis()))
        }
        _ => Err(TblCliError::Error("Unsupported dtype".to_string())),
    }
}

pub(crate) fn apply_filter(
    lf: LazyFrame,
    filters: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    // First, get the schema of the LazyFrame
    let schema = lf
        .clone()
        .schema()
        .map_err(|e| TblCliError::Error(e.to_string()))?;

    match filters {
        None => Ok(lf),
        Some(filters) => {
            let mut new_lf = lf;
            for filter in filters {
                let parts: Vec<&str> = filter.split('=').collect();
                if parts.len() != 2 {
                    return Err(TblCliError::Error("Invalid filter format".to_string()));
                }
                let (column, value) = (parts[0], parts[1]);

                // Get the data type of the column
                let column_type = schema
                    .get(column)
                    .ok_or_else(|| TblCliError::Error(format!("Column '{}' not found", column)))?;

                new_lf = match column_type {
                    DataType::Binary => {
                        // For Binary type, convert hex to binary
                        if let Some(hex_value) = value.strip_prefix("0x") {
                            let binary_value = hex::decode(hex_value).map_err(|e| {
                                TblCliError::Error(format!("Invalid hex value: {}", e))
                            })?;
                            new_lf.filter(col(column).eq(lit(binary_value)))
                        } else {
                            return Err(TblCliError::Error(
                                "Binary value must start with 0x".to_string(),
                            ));
                        }
                    }
                    DataType::String => {
                        // For String type, use the value as-is
                        new_lf.filter(col(column).eq(lit(value)))
                    }
                    DataType::UInt64 | DataType::Int64 => {
                        // For integer types, parse the value
                        let int_value = if let Some(hex_value) = value.strip_prefix("0x") {
                            i64::from_str_radix(hex_value, 16).map_err(|e| {
                                TblCliError::Error(format!("Invalid hex integer: {}", e))
                            })?
                        } else {
                            value.parse::<i64>().map_err(|e| {
                                TblCliError::Error(format!("Invalid integer: {}", e))
                            })?
                        };
                        new_lf.filter(col(column).eq(lit(int_value)))
                    }
                    // Add more type handling as needed
                    _ => {
                        return Err(TblCliError::Error(format!(
                            "Unsupported column type for '{}': {:?}",
                            column, column_type
                        )))
                    }
                };
            }
            Ok(new_lf)
        }
    }
}

pub(crate) fn apply_rename(
    lf: LazyFrame,
    rename: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    match rename {
        None => Ok(lf),
        Some(rename) => {
            let (existing, new): (Vec<String>, Vec<String>) =
                rename
                    .iter()
                    .try_fold((Vec::new(), Vec::new()), |(mut old, mut new), r| {
                        let parts: Vec<&str> = r.split('=').collect();
                        if parts.len() != 2 {
                            return Err(TblCliError::Error("Invalid rename format".to_string()));
                        }
                        old.push(parts[0].to_string());
                        new.push(parts[1].to_string());
                        Ok((old, new))
                    })?;

            Ok(lf.rename(existing, new))
        }
    }
}

pub(crate) fn apply_drop(
    lf: LazyFrame,
    columns: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    match columns {
        None => Ok(lf),
        Some(columns) => Ok(lf.drop(columns)),
    }
}

pub(crate) fn apply_cast(lf: LazyFrame, cast: Option<&[String]>) -> Result<LazyFrame, TblCliError> {
    match cast {
        None => Ok(lf),
        Some(cast) => {
            let mut new_lf = lf;
            for c in cast {
                let parts: Vec<&str> = c.split('=').collect();
                if parts.len() != 2 {
                    return Err(TblCliError::Error("InvalidCastFormat".to_string()));
                }
                let (column, dtype_str) = (parts[0], parts[1]);
                let dtype = parse_dtype(dtype_str)?;
                new_lf = new_lf.with_column(col(column).cast(dtype));
            }
            Ok(new_lf)
        }
    }
}

pub(crate) fn apply_sort(
    lf: LazyFrame,
    raw_columns: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    match raw_columns {
        None => Ok(lf),
        Some(raw_columns) => {
            let mut columns: Vec<String> = Vec::new();
            let mut descending: Vec<bool> = Vec::new();
            for column in raw_columns.iter() {
                let column = column.to_string();
                if column.ends_with(":desc") {
                    columns.push(column[..column.len() - 5].to_string());
                    descending.push(true);
                } else {
                    columns.push(column);
                    descending.push(false);
                }
            }
            let options = polars::chunked_array::ops::SortMultipleOptions::default()
                .with_order_descending_multi(descending);
            Ok(lf.sort(columns, options))
        }
    }
}

pub(crate) fn apply_select(
    lf: LazyFrame,
    columns: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    match columns {
        None => Ok(lf),
        Some(columns) => {
            let exprs: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
            Ok(lf.select(&exprs))
        }
    }
}

pub(crate) fn apply_head(lf: LazyFrame, n: Option<usize>) -> Result<LazyFrame, TblCliError> {
    match n {
        None => Ok(lf),
        Some(n) => Ok(lf.slice(0, n as u32)),
    }
}

pub(crate) fn apply_tail(lf: LazyFrame, n: Option<usize>) -> Result<LazyFrame, TblCliError> {
    match n {
        None => Ok(lf),
        Some(n) => Ok(lf.tail(n as u32)),
    }
}

pub(crate) fn apply_offset(lf: LazyFrame, n: Option<usize>) -> Result<LazyFrame, TblCliError> {
    match n {
        None => Ok(lf),
        Some(n) => Ok(lf.slice(n as i64, u32::MAX)),
    }
}

pub(crate) fn apply_value_counts(lf: LazyFrame, n: Option<&str>) -> Result<LazyFrame, TblCliError> {
    match n {
        None => Ok(lf),
        Some(column) => {
            // let expr = col(column).value_counts(true, false, "count".to_string(), false);
            // Ok(lf.select([expr]))
            let sort_options = SortMultipleOptions::new().with_order_descending(true);
            let value_counts = lf
                .group_by(&[col(column)])
                .agg(&[col(column).count().alias("count")])
                .sort(["count"], sort_options);
            Ok(value_counts)
        }
    }
}
