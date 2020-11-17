/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
use std::str;
use std::io::Cursor;
use std::collections::HashMap;
use arrow::array::*;
use arrow::ipc::reader::{StreamReader};
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    // FIXME: remove log redefinition/find a way to pass log down to submodules
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

pub struct ArrowAccessor {
    data: HashMap<String, Box<Vec<f64>>>
}

/// Load an arrow binary in stream format.
pub fn load_arrow_stream(buffer: Box<[u8]>) {
    let cursor = Cursor::new(buffer);
    let reader = StreamReader::try_new(cursor).unwrap();
    let schema = reader.schema();
    let mut accessors: Vec<Box<ArrowAccessor>> = Vec::new();

    reader.for_each(|batch| {
        match batch {
            Ok(val) => {
                accessors.push(convert_record_batch(val))
            }, Err(err) => log(format!("{}", err).as_str())
        }
    });

    let fields = schema.fields();

    for accessor in accessors {
        for field in fields {
            let name = field.name();
            let column = &accessor.data.get(name);
            log(format!("{}: {:?}", name, column).as_str());
        }
    }
}

pub fn convert_record_batch(batch: RecordBatch) -> Box<ArrowAccessor> {
    let schema = batch.schema();
    let num_columns: usize = batch.num_columns();
    let num_rows: usize = batch.num_rows();

    log(format!("{} x {}", num_columns, num_rows).as_str());

    // let mut converted_data: HashMap<String, Box<&dyn Array>> = HashMap::new();
    let mut converted: HashMap<String, Box<Vec<f64>>> = HashMap::new();

    for i in 0..num_columns {
        let col = batch.column(i);
        let col_any = col.as_any();
        let name: String = schema.field(i).name().clone();

        if let Some(result) = col_any.downcast_ref::<Float64Array>() {
            let values_ptr: *const f64 = result.raw_values();

            unsafe {
                let slice = std::slice::from_raw_parts(values_ptr, result.len());
                let v = slice.to_vec();
                converted.insert(name, Box::new(v));
            };
        } else if let Some(result) = col_any.downcast_ref::<Int64Array>() {
            let values_ptr: *const i64 = result.raw_values();

            unsafe {
                let slice = std::slice::from_raw_parts(values_ptr, result.len());
                let v = slice.to_vec();
                log(format!("i64 copied into vec: {:?}", v).as_str());
            };
        } else if let Some(result) = col_any.downcast_ref::<Int32Array>() {
            let values_ptr: *const i32 = result.raw_values();

            unsafe {
                let slice = std::slice::from_raw_parts(values_ptr, result.len());
                let v = slice.to_vec();
                log(format!("i32 copied into vec: {:?}", v).as_str());
            };
        } else if let Some(result) = col_any.downcast_ref::<Date32Array>() {
            let values_ptr: *const i32 = result.raw_values();

            unsafe {
                let slice = std::slice::from_raw_parts(values_ptr, result.len());
                let v = slice.to_vec();
                log(format!("date32 copied into vec: {:?}", v).as_str());
            };
        } else if let Some(result) = col_any.downcast_ref::<TimestampMillisecondArray>() {
            let values_ptr: *const i64 = result.raw_values();

            unsafe {
                let slice = std::slice::from_raw_parts(values_ptr, result.len());
                let v = slice.to_vec();
                log(format!("timestamp copied into vec: {:?}", v).as_str());
            };
        } else if let Some(result) = col_any.downcast_ref::<BooleanArray>() {
            let values_ptr: *const bool = result.raw_values();

            unsafe {
                let slice = std::slice::from_raw_parts(values_ptr, result.len());
                let v = slice.to_vec();
                log(format!("boolean copied into vec: {:?}", v).as_str());
            };
        } else if let Some(result) = col_any.downcast_ref::<Int32DictionaryArray>() {
            if let Some(strings) = result.values().as_any().downcast_ref::<StringArray>() {
                let mut string_vec: Vec<String> = Vec::new();
                for key in result.keys() {
                    let k = key.unwrap();
                    string_vec.push(String::from(strings.value(k as usize)));
                }
                log(format!("String copied into vec: {:?}", string_vec).as_str());
            }
        }
    }

    let accessor = ArrowAccessor {
        data: converted
    };

    return Box::new(accessor);
}

#[wasm_bindgen]
pub fn get_from_arrow(column_name: &str, ridx: usize) -> JsValue {
    println!("{}[{}]", column_name, ridx);
    println!("Returning JS null");
    JsValue::NULL
}