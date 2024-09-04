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
    let lf = apply_set(lf, args.set.as_deref())?;
    let lf = apply_nullify(lf, args.nullify.as_deref())?;
    let lf = apply_replace(lf, args.replace.as_deref())?;
    let lf = apply_select(lf, args.columns.as_deref())?;
    let lf = apply_offset(lf, args.offset)?;
    let lf = apply_head(lf, args.head)?;
    let lf = apply_tail(lf, args.tail)?;
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
                .ok_or_else(|| TblCliError::Error("Failed to create NaiveDateTime".to_string()))?
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
    let schema = lf
        .clone()
        .schema()
        .map_err(|e| TblCliError::Error(e.to_string()))?;

    match filters {
        None => Ok(lf),
        Some(filters) => {
            let mut new_lf = lf;
            for filter in filters {
                new_lf = apply_single_filter(new_lf, filter, &schema)?;
            }
            Ok(new_lf)
        }
    }
}

fn apply_single_filter(
    lf: LazyFrame,
    filter: &str,
    schema: &Schema,
) -> Result<LazyFrame, TblCliError> {
    if filter.contains("!=") {
        apply_comparison_filter(lf, filter, schema, "!=")
    } else if filter.contains(">=") {
        apply_comparison_filter(lf, filter, schema, ">=")
    } else if filter.contains("<=") {
        apply_comparison_filter(lf, filter, schema, "<=")
    } else if filter.contains('=') {
        apply_comparison_filter(lf, filter, schema, "=")
    } else if filter.contains(">") {
        apply_comparison_filter(lf, filter, schema, ">")
    } else if filter.contains("<") {
        apply_comparison_filter(lf, filter, schema, "<")
    } else if filter.ends_with(".is_null") {
        apply_null_filter(lf, filter, schema, true)
    } else if filter.ends_with(".is_not_null") {
        apply_null_filter(lf, filter, schema, false)
    } else {
        Err(TblCliError::Error("Invalid filter format".to_string()))
    }
}

fn apply_comparison_filter(
    lf: LazyFrame,
    filter: &str,
    schema: &Schema,
    operator: &str,
) -> Result<LazyFrame, TblCliError> {
    let parts: Vec<&str> = if operator == "=" {
        filter.split('=').collect()
    } else if operator == "!=" {
        filter.split("!=").collect()
    } else if operator == ">" {
        filter.split('>').collect()
    } else if operator == "<" {
        filter.split('<').collect()
    } else if operator == ">=" {
        filter.split(">=").collect()
    } else if operator == "<=" {
        filter.split("<=").collect()
    } else {
        return Err(TblCliError::Error(format!(
            "Invalid filter operator: {}",
            operator
        )));
    };

    if parts.len() != 2 {
        return Err(TblCliError::Error("Invalid filter format".to_string()));
    }

    let (column, value) = (parts[0], parts[1]);
    let column_type = schema
        .get(column)
        .ok_or_else(|| TblCliError::Error(format!("Column '{}' not found", column)))?;

    let filter_expr = match column_type {
        DataType::Binary => {
            if let Some(hex_value) = value.strip_prefix("0x") {
                let binary_value = hex::decode(hex_value)
                    .map_err(|e| TblCliError::Error(format!("Invalid hex value: {}", e)))?;
                if operator == "=" {
                    col(column).eq(lit(binary_value))
                } else if operator == "!=" {
                    col(column).neq(lit(binary_value))
                } else if operator == ">" {
                    col(column).gt(lit(binary_value))
                } else if operator == "<" {
                    col(column).lt(lit(binary_value))
                } else if operator == ">=" {
                    col(column).gt_eq(lit(binary_value))
                } else if operator == "<=" {
                    col(column).lt_eq(lit(binary_value))
                } else {
                    return Err(TblCliError::Error(format!(
                        "Invalid filter operator: {}",
                        operator
                    )));
                }
            } else {
                return Err(TblCliError::Error(
                    "Binary value must start with 0x".to_string(),
                ));
            }
        }
        DataType::String => {
            if operator == "=" {
                col(column).eq(lit(value))
            } else if operator == "!=" {
                col(column).neq(lit(value))
            } else if operator == ">" {
                col(column).gt(lit(value))
            } else if operator == "<" {
                col(column).lt(lit(value))
            } else if operator == ">=" {
                col(column).gt_eq(lit(value))
            } else if operator == "<=" {
                col(column).lt_eq(lit(value))
            } else {
                return Err(TblCliError::Error(format!(
                    "Invalid filter operator: {}",
                    operator
                )));
            }
        }
        DataType::UInt64 | DataType::Int64 | DataType::UInt32 | DataType::Int32 => {
            let int_value = if let Some(hex_value) = value.strip_prefix("0x") {
                i64::from_str_radix(hex_value, 16)
                    .map_err(|e| TblCliError::Error(format!("Invalid hex integer: {}", e)))?
            } else {
                value
                    .parse::<i64>()
                    .map_err(|e| TblCliError::Error(format!("Invalid integer: {}", e)))?
            };
            if operator == "=" {
                col(column).eq(lit(int_value))
            } else if operator == "!=" {
                col(column).neq(lit(int_value))
            } else if operator == ">" {
                col(column).gt(lit(int_value))
            } else if operator == "<" {
                col(column).lt(lit(int_value))
            } else if operator == ">=" {
                col(column).gt_eq(lit(int_value))
            } else if operator == "<=" {
                col(column).lt_eq(lit(int_value))
            } else {
                return Err(TblCliError::Error(format!(
                    "Invalid filter operator: {}",
                    operator
                )));
            }
        }
        _ => {
            return Err(TblCliError::Error(format!(
                "Unsupported column type for '{}': {:?}",
                column, column_type
            )))
        }
    };

    Ok(lf.filter(filter_expr))
}

fn apply_null_filter(
    lf: LazyFrame,
    filter: &str,
    schema: &Schema,
    is_null: bool,
) -> Result<LazyFrame, TblCliError> {
    let column = filter.trim_end_matches(if is_null { ".is_null" } else { ".is_not_null" });

    if schema.get(column).is_none() {
        return Err(TblCliError::Error(format!("Column '{}' not found", column)));
    }

    let filter_expr = if is_null {
        col(column).is_null()
    } else {
        col(column).is_not_null()
    };

    Ok(lf.filter(filter_expr))
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

pub(crate) fn apply_set(lf: LazyFrame, set: Option<&[String]>) -> Result<LazyFrame, TblCliError> {
    match set {
        None => Ok(lf),
        Some(set) => {
            let mut new_lf = lf;
            let schema = new_lf
                .schema()
                .map_err(|e| TblCliError::Error(e.to_string()))?;

            for s in set {
                println!("s: {:?}", s);
                let parts: Vec<&str> = s.split('=').collect();
                println!("parts: {:?}", parts);
                if parts.len() != 2 {
                    return Err(TblCliError::Error("Invalid set format".to_string()));
                }
                let (column, value) = (parts[0], parts[1]);

                let column_type = schema
                    .get(column)
                    .ok_or_else(|| TblCliError::Error(format!("Column '{}' not found", column)))?;
                println!("column_type: {:?}", column_type);
                println!("column: {:?}", column);

                let set_expr = raw_str_to_lit(column, value, column_type)?;
                println!("set_expr: {:?}", set_expr);
                new_lf = new_lf.with_column(set_expr.cast(column_type.clone()));
                println!("done");
            }
            Ok(new_lf)
        }
    }
}

fn raw_str_to_lit(column: &str, value: &str, dtype: &DataType) -> Result<Expr, TblCliError> {
    let lit_value = match dtype {
        DataType::Int8 => lit(i8::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid i8 value: {}", value)))?),
        DataType::Int16 => lit(i16::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid i16 value: {}", value)))?),
        DataType::Int32 => lit(i32::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid i32 value: {}", value)))?),
        DataType::Int64 => lit(i64::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid i64 value: {}", value)))?),
        DataType::UInt8 => lit(u8::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid u8 value: {}", value)))?),
        DataType::UInt16 => lit(u16::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid u16 value: {}", value)))?),
        DataType::UInt32 => lit(u32::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid u32 value: {}", value)))?),
        DataType::UInt64 => lit(u64::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid u64 value: {}", value)))?),
        DataType::Float32 => lit(f32::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid f32 value: {}", value)))?),
        DataType::Float64 => lit(f64::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid f64 value: {}", value)))?),
        DataType::Boolean => lit(bool::from_str(value)
            .map_err(|_| TblCliError::Error(format!("Invalid boolean value: {}", value)))?),
        DataType::String => lit(value.to_string()),
        DataType::Date => {
            let naive_date =
                chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
                    TblCliError::Error("Invalid date format. Use YYYY-MM-DD".to_string())
                })?;
            lit(naive_date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| TblCliError::Error("Failed to create NaiveDateTime".to_string()))?
                .and_utc()
                .timestamp_millis())
        }
        DataType::Datetime(_, _) => {
            let naive_datetime = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| {
                    TblCliError::Error(
                        "Invalid datetime format. Use YYYY-MM-DD HH:MM:SS".to_string(),
                    )
                })?;
            lit(naive_datetime.and_utc().timestamp_millis())
        }
        DataType::Binary => {
            if let Some(hex_value) = value.strip_prefix("0x") {
                let binary_value = hex::decode(hex_value)
                    .map_err(|e| TblCliError::Error(format!("Invalid hex value: {}", e)))?;
                lit(binary_value)
            } else {
                return Err(TblCliError::Error(
                    "Binary value must start with 0x".to_string(),
                ));
            }
        }
        _ => {
            return Err(TblCliError::Error(format!(
                "Unsupported column type for '{}': {:?}",
                column, dtype
            )))
        }
    };

    Ok(lit_value.alias(column))
}

pub(crate) fn apply_nullify(
    lf: LazyFrame,
    raw_columns: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    match raw_columns {
        None => Ok(lf),
        Some(columns) => {
            let mut new_lf = lf;
            let schema = new_lf
                .schema()
                .map_err(|e| TblCliError::Error(e.to_string()))?;

            for column in columns.iter() {
                let column_type = schema
                    .get(column)
                    .ok_or_else(|| TblCliError::Error(format!("Column '{}' not found", column)))?;
                new_lf = new_lf.with_column(
                    lit(LiteralValue::Null)
                        .cast(column_type.clone())
                        .alias(column),
                );
            }
            Ok(new_lf)
        }
    }
}

pub(crate) fn apply_replace(
    lf: LazyFrame,
    raw_values: Option<&[String]>,
) -> Result<LazyFrame, TblCliError> {
    match raw_values {
        None => Ok(lf),
        Some(values) => {
            let mut new_lf = lf;
            let schema = new_lf
                .schema()
                .map_err(|e| TblCliError::Error(e.to_string()))?;

            for value in values.iter() {
                // get column
                let parts: Vec<&str> = value.split('.').collect();
                if parts.len() != 2 {
                    return Err(TblCliError::Error("Invalid format".to_string()));
                }
                let (column, before_after) = (parts[0], parts[1]);

                // get old_value / new_value
                let parts: Vec<&str> = before_after.split('=').collect();
                if parts.len() != 2 {
                    return Err(TblCliError::Error("Invalid format".to_string()));
                }
                let (old_value, new_value) = (parts[0], parts[1]);

                let column_type = schema
                    .get(column)
                    .ok_or_else(|| TblCliError::Error(format!("Column '{}' not found", column)))?;

                let old_expr = raw_str_to_lit(column, old_value, column_type)?;
                let new_expr = raw_str_to_lit(column, new_value, column_type)?;
                new_lf = new_lf.with_column(col(column).replace(old_expr, new_expr));
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
