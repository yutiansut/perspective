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

use js_sys::*;
use chrono::Datelike;
use wasm_bindgen::prelude::*;

use crate::arrow::ArrowAccessor;
use crate::api::load_arrow_stream;
// se crate::utils::set_panic_hook;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen]
pub fn make_rust_accessor(buffer: Box<[u8]>) -> *const ArrowAccessor {
    // set_panic_hook();
    let accessor = load_arrow_stream(buffer);
    Box::into_raw(accessor)
}

#[wasm_bindgen]
pub fn get_from_rust_accessor(accessor: *const ArrowAccessor, column_name: &str, ridx: usize) -> JsValue {
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
            "date" => {
                match accessor.get_date(column_name, ridx) {
                    Some(value) => JsValue::from(Date::new_with_year_month_day(value.year() as u32, value.month0() as i32, value.day() as i32)),
                    None => JsValue::NULL
                }  
            },
            "datetime" => {
                match accessor.get_datetime(column_name, ridx) {
                    Some(value) => {
                        let timestamp = JsValue::from(value as f64);
                        JsValue::from(Date::new(&timestamp))
                    }
                    None => JsValue::NULL
                }
            }
            "bool" => JsValue::from(accessor.get_bool(column_name, ridx).unwrap()),
            "string" => JsValue::from(accessor.get_string(column_name, ridx).unwrap()),
            _ => panic!("Unexpected dtype: {}", dtype)
        }
    }
}
