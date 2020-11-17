/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
use std::any::Any;
use std::sync::Arc;
use std::str;
use std::io::Cursor;
use std::collections::HashMap;
use arrow::error::{ArrowError, Result};
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::ipc::reader::{StreamReader};
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    // FIXME: remove log redefinition/find a way to pass log down to submodules
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

pub struct ArrowAccessor<'a> {
    data: HashMap<String, Box<&'a dyn Array>>
}

/// Load an arrow binary in stream format.
pub fn load_arrow_stream(buffer: Box<[u8]>) {
    let cursor = Cursor::new(buffer);
    let reader = StreamReader::try_new(cursor).unwrap();
    let mut accessors: Vec<Box<ArrowAccessor>> = Vec::new();

    reader.for_each(|batch| {
        match batch {
            Ok(val) => {
                accessors.push(convert_record_batch(val))
            }, Err(err) => log(format!("{}", err).as_str())
        }
    });
}

pub fn convert_record_batch<'a>(batch: RecordBatch) -> Box<ArrowAccessor<'a>> {
    let schema = batch.schema();
    let num_columns: usize = batch.num_columns();
    let num_rows: usize = batch.num_rows();

    log(format!("{} x {}", num_columns, num_rows).as_str());

    let mut converted_data: HashMap<String, Box<&dyn Array>> = HashMap::new();

    for i in 0..num_columns {
        let col = batch.column(i);
        let col_any = col.as_any();

        let name: String = schema.field(i).name().clone();

        if let Some(result) = col_any.downcast_ref::<Float64Array>() {
            converted_data.insert(name, Box::new(result));
            for j in 0..result.len() {
                log(format!("Float64Array {}", result.value(j)).as_str());
            }
        } else if let Some(result) = col_any.downcast_ref::<Int64Array>() {
            for j in 0..result.len() {
                log(format!("Int64Array {}", result.value(j)).as_str());
            }
        } else if let Some(result) = col_any.downcast_ref::<Int32Array>() {
            for j in 0..result.len() {
                log(format!("Int32Array {}", result.value(j)).as_str());
            }
        } else if let Some(result) = col_any.downcast_ref::<Date32Array>() {
            for j in 0..result.len() {
                log(format!("Date32Array {}", result.value(j)).as_str());
            }
        } else if let Some(result) = col_any.downcast_ref::<TimestampMillisecondArray>() {
            for j in 0..result.len() {
                log(format!("TimestampMillisecondArray {}", result.value(j)).as_str());
            }
        } else if let Some(result) = col_any.downcast_ref::<BooleanArray>() {
            for j in 0..result.len() {
                log(format!("BooleanArray {}", result.value(j)).as_str());
            }
        } else if let Some(result) = col_any.downcast_ref::<Int32DictionaryArray>() {
            if let Some(strings) = result.values().as_any().downcast_ref::<StringArray>() {
                for key in result.keys() {
                    let k = key.unwrap();
                    log(format!("int32dict {}", strings.value(k as usize)).as_str());
                }
            }
        }
    }

    let accessor = ArrowAccessor {
        data: converted_data
    };

    return Box::new(accessor);
}

#[wasm_bindgen]
pub fn get_from_arrow(column_name: &str, ridx: usize) -> JsValue {
    println!("{}[{}]", column_name, ridx);
    println!("Returning JS null");
    JsValue::NULL
}