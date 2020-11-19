/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
mod api;
mod arrow;
mod utils;

use wasm_bindgen::prelude::*;

use crate::arrow::ArrowAccessor;
use crate::api::load_arrow_stream;
use crate::utils::set_panic_hook;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen]
pub fn load_arrow(buffer: Box<[u8]>) -> *const ArrowAccessor {
    set_panic_hook();
    let accessor = load_arrow_stream(buffer);
    Box::into_raw(accessor)
}

#[wasm_bindgen]
pub fn accessor_pprint(accessor: *const ArrowAccessor) {
    unsafe {
        log(format!("{}", accessor.as_ref().unwrap()).as_str())
    }
}

#[wasm_bindgen]
pub fn accessor_get(accessor: *const ArrowAccessor, column_name: &str, ridx: usize) -> JsValue {
    let accessor = unsafe { accessor.as_ref().unwrap() };
    if !accessor.is_valid(column_name, ridx) {
        JsValue::NULL
    } else {
        let schema = &accessor.schema;
        let dtype = &schema[column_name];
        match &dtype[..] {
            "i32" => JsValue::from(accessor.get_i32(column_name, ridx)),
            "i64" => {
                match accessor.get_i64(column_name, ridx) {
                    Some(num) => JsValue::from(num as i32),
                    None => JsValue::NULL
                }
            },
            "f64" => JsValue::from(accessor.get_f64(column_name, ridx)),
            "date" => JsValue::NULL,
            "datetime" => JsValue::NULL,
            "bool" => JsValue::from(accessor.get_bool(column_name, ridx).unwrap()),
            "string" => JsValue::from(accessor.get_string(column_name, ridx).unwrap()),
            _ => panic!("Unexpected dtype: {}", dtype)
        }
    }
}

#[wasm_bindgen]
pub fn accessor_print_schema(accessor: *const ArrowAccessor) {
    unsafe {
        let schema = accessor.as_ref().unwrap().schema.clone();
        log(format!("{:?}", schema).as_str())
    }
}

#[wasm_bindgen]
pub fn accessor_print_column_names(accessor: *const ArrowAccessor) {
    unsafe {
        let column_names = accessor.as_ref().unwrap().column_names.clone();
        log(format!("{:?}", column_names).as_str())
    }
}