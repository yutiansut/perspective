/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
use std::fmt;
use std::str;
use std::io::Cursor;
use std::collections::HashMap;
use arrow::array;
use arrow::datatypes::{DataType, Schema};
use arrow::ipc::reader::{StreamReader};
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    // FIXME: remove log redefinition/find a way to pass log down to submodules
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnData {
    DTYPE_INT32(Vec<i32>),
    DTYPE_INT64(Vec<i64>),
    DTYPE_FLOAT64(Vec<f64>),
    DTYPE_TIME(Vec<i64>),
    DTYPE_DATE(Vec<i32>),
    DTYPE_BOOL(Vec<bool>),
    DTYPE_STR(Vec<Option<String>>),
}

pub struct ArrowColumn {
    name: String,
    dtype: DataType,
    data: ColumnData
}

impl ArrowColumn {
    pub fn new(name: &str, dtype: DataType, data: ColumnData) -> Self {
        ArrowColumn{
            name: String::from(name),
            dtype,
            data
        }
    }
}


impl fmt::Display for ArrowColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {:?}\n", self.name, self.data)
    }
}


pub struct ArrowAccessor {
    schema: Schema,
    data: HashMap<String, ArrowColumn>,
}

impl ArrowAccessor {

    pub fn new(batch: Box<RecordBatch>, schema: Schema) -> Self {
        let num_columns = batch.num_columns();
        let mut data: HashMap<String, ArrowColumn> = HashMap::new();

        for cidx in 0..num_columns {
            let field = schema.field(cidx);
            let name = field.name().clone();
            let dtype = field.data_type().clone();
            let col = batch.column(cidx);
            let col_any = col.as_any();

            // Construct our wrapper structures
            let mut column_data: Option<ColumnData> = None;

            if let Some(result) = col_any.downcast_ref::<array::Float64Array>() {
                let values_ptr: *const f64 = result.raw_values();
    
                let slice = unsafe {
                    std::slice::from_raw_parts(values_ptr, result.len())
                };

                column_data = Some(ColumnData::DTYPE_FLOAT64(slice.to_vec()));
            } else if let Some(result) = col_any.downcast_ref::<array::Int64Array>() {
                let values_ptr: *const i64 = result.raw_values();
    
                let slice = unsafe {
                    std::slice::from_raw_parts(values_ptr, result.len())
                };

                column_data = Some(ColumnData::DTYPE_INT64(slice.to_vec()));
            } else if let Some(result) = col_any.downcast_ref::<array::Int32Array>() {
                let values_ptr: *const i32 = result.raw_values();
    
                let slice = unsafe {
                    std::slice::from_raw_parts(values_ptr, result.len())
                };

                column_data = Some(ColumnData::DTYPE_INT32(slice.to_vec()));
            } else if let Some(result) = col_any.downcast_ref::<array::Date32Array>() {
                let values_ptr: *const i32 = result.raw_values();
    
                let slice = unsafe {
                    std::slice::from_raw_parts(values_ptr, result.len())
                };

                column_data = Some(ColumnData::DTYPE_DATE(slice.to_vec()));
            } else if let Some(result) = col_any.downcast_ref::<array::TimestampMillisecondArray>() {
                let values_ptr: *const i64 = result.raw_values();
    
                let slice = unsafe {
                    std::slice::from_raw_parts(values_ptr, result.len())
                };

                column_data = Some(ColumnData::DTYPE_TIME(slice.to_vec()));
            } else if let Some(result) = col_any.downcast_ref::<array::BooleanArray>() {
                let values_ptr: *const bool = result.raw_values();
    
                let slice = unsafe {
                    std::slice::from_raw_parts(values_ptr, result.len())
                };

                column_data = Some(ColumnData::DTYPE_BOOL(slice.to_vec()));
            } else if let Some(result) = col_any.downcast_ref::<array::Int32DictionaryArray>() {
                // Materialize the strings into one Vector of heap-owned Strings
                if let Some(strings) = result.values().as_any().downcast_ref::<array::StringArray>() {
                    let mut string_values: Vec<Option<String>> = Vec::new();
                    for key in result.keys() {
                        match key {
                            Some(k) => string_values.push(Some(String::from(strings.value(k as usize)))),
                            None => string_values.push(None)
                        }
                    }
                    column_data = Some(ColumnData::DTYPE_STR(string_values));
                }
            }

            let converted = ArrowColumn::new(name.as_str(), dtype, column_data.unwrap());
            data.insert(name, converted);
        }

        ArrowAccessor {
            schema,
            data
        }
    }

    // TODO: use the schema from Perspective for column names, or have the
    // arrow accessor hold its own schema from rust?
    pub fn contains_column(&self, column_name: &str) -> bool {
        match self.schema.field_with_name(column_name) {
            Ok(_) => true,
            Err(_) => false
        }
    }

    pub fn get(&self, column_name: &str, ridx: usize) {
        unimplemented!("get() not implemented!")
    }

    pub fn get_dtype(&self, column_name: &str) {
        unimplemented!("get_dtype() not implemented!")
    }

    // FIXME: need to return something that we can convert to a JSVal
    // pub fn get_dtype(&self, column_name: &str) -> Option<serde_json::value::Value> {
    //     match self.schema.field_with_name(column_name) {
    //         Ok(field) => {
    //             Some(field.data_type().to_json())
    //         },
    //         Err(err) => {
    //             log(format!("Could not get dtype for column `{}`: {}", column_name, err).as_str());
    //             None
    //         }
    //     }
    // }
}

impl fmt::Display for ArrowAccessor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ArrowAccessor\n");
        write!(f, "--------------\n");
        for column in self.data.values() {
            write!(f, "{}", column);
        }
        write!(f, "")
    }
}

/// Load an arrow binary in stream format.
pub fn load_arrow_stream(buffer: Box<[u8]>) -> Box<ArrowAccessor> {
    let cursor = Cursor::new(buffer);
    let reader = StreamReader::try_new(cursor).unwrap();

    // FIXME: probably no need to clone, as we are never mutating it
    let schema = (*reader.schema()).clone();

    log(format!("[Rust] arrow schema: {}", schema).as_str());

    // Iterate over record batches, and collect them into a vector of
    // heap-allocated batches. Do not use the `reader` after this line,
    // as it is consumed by `.map()` below.
    let batches = reader.map(|batch| {
        // Panic if it can't read the batch
        Box::new(batch.unwrap())
    }).collect::<Vec<Box<RecordBatch>>>();

    if let [batch] = &batches[..] {
        log(format!("[Rust] {} record batches loaded", batches.len()).as_str());
        Box::new(ArrowAccessor::new(batch.clone(), schema))
    } else {
        panic!("Should only be 1 record batch to load.")
    }
}
