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
// use std::collections::HashMap;
// use arrow::array::*;
use arrow::datatypes::{Schema};
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
    // TODO: figure out the way to access underlying data
    batches: Vec<Box<RecordBatch>>,
    schema: Schema
}

impl ArrowAccessor {

    pub fn num_batches(&self) -> usize {
        log(format!("[Rust] {} record batches in accessor", self.batches.len()).as_str());    
        self.batches.len()
    }

    // TODO: use the schema from Perspective for column names, or have the
    // arrow accessor hold its own schema from rust?
    pub fn contains_column(&self, name: &str) -> bool {
        match self.schema.field_with_name(name) {
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

    log(format!("[Rust] {} record batches loaded", batches.len()).as_str());

    let accessor = ArrowAccessor {
        batches,
        schema
    };

    return Box::new(accessor);
}

// pub fn convert_record_batch(batch: RecordBatch) -> Box<ArrowAccessor> {
//     let schema = batch.schema();
//     let num_columns: usize = batch.num_columns();
//     let num_rows: usize = batch.num_rows();

//     log(format!("{} x {}", num_columns, num_rows).as_str());

//     // let mut converted_data: HashMap<String, Box<&dyn Array>> = HashMap::new();
//     let mut converted: HashMap<String, Box<Vec<f64>>> = HashMap::new();

//     for i in 0..num_columns {
//         let col = batch.column(i);
//         let col_any = col.as_any();
//         let name: String = schema.field(i).name().clone();

//         if let Some(result) = col_any.downcast_ref::<Float64Array>() {
//             let values_ptr: *const f64 = result.raw_values();

//             unsafe {
//                 let slice = std::slice::from_raw_parts(values_ptr, result.len());
//                 let v = slice.to_vec();
//                 converted.insert(name, Box::new(v));
//             };
//         } else if let Some(result) = col_any.downcast_ref::<Int64Array>() {
//             let values_ptr: *const i64 = result.raw_values();

//             unsafe {
//                 let slice = std::slice::from_raw_parts(values_ptr, result.len());
//                 let v = slice.to_vec();
//                 log(format!("i64 copied into vec: {:?}", v).as_str());
//             };
//         } else if let Some(result) = col_any.downcast_ref::<Int32Array>() {
//             let values_ptr: *const i32 = result.raw_values();

//             unsafe {
//                 let slice = std::slice::from_raw_parts(values_ptr, result.len());
//                 let v = slice.to_vec();
//                 log(format!("i32 copied into vec: {:?}", v).as_str());
//             };
//         } else if let Some(result) = col_any.downcast_ref::<Date32Array>() {
//             let values_ptr: *const i32 = result.raw_values();

//             unsafe {
//                 let slice = std::slice::from_raw_parts(values_ptr, result.len());
//                 let v = slice.to_vec();
//                 log(format!("date32 copied into vec: {:?}", v).as_str());
//             };
//         } else if let Some(result) = col_any.downcast_ref::<TimestampMillisecondArray>() {
//             let values_ptr: *const i64 = result.raw_values();

//             unsafe {
//                 let slice = std::slice::from_raw_parts(values_ptr, result.len());
//                 let v = slice.to_vec();
//                 log(format!("timestamp copied into vec: {:?}", v).as_str());
//             };
//         } else if let Some(result) = col_any.downcast_ref::<BooleanArray>() {
//             let values_ptr: *const bool = result.raw_values();

//             unsafe {
//                 let slice = std::slice::from_raw_parts(values_ptr, result.len());
//                 let v = slice.to_vec();
//                 log(format!("boolean copied into vec: {:?}", v).as_str());
//             };
//         } else if let Some(result) = col_any.downcast_ref::<Int32DictionaryArray>() {
//             if let Some(strings) = result.values().as_any().downcast_ref::<StringArray>() {
//                 let mut string_vec: Vec<String> = Vec::new();
//                 for key in result.keys() {
//                     let k = key.unwrap();
//                     string_vec.push(String::from(strings.value(k as usize)));
//                 }
//                 log(format!("String copied into vec: {:?}", string_vec).as_str());
//             }
//         }
//     }

//     let accessor = ArrowAccessor {
//         data: converted
//     };

//     return Box::new(accessor);
// }
