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
use std::collections::HashMap;
use std::sync::Arc;
use chrono::naive::NaiveDate;
use chrono::naive::NaiveDateTime;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    // FIXME: remove log redefinition/find a way to pass log down to submodules
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

pub struct ArrowAccessor {
    pub schema: HashMap<String, String>,
    pub column_names: Vec<String>,
    data: HashMap<String, ArrayRef>
}

impl ArrowAccessor {
    pub fn new(batch: Box<RecordBatch>, batch_schema: Schema) -> Self {
        let num_columns = batch.num_columns();
        let mut schema: HashMap<String, String> = HashMap::new();
        let mut column_names: Vec<String> = Vec::new();
        let mut data: HashMap<String, ArrayRef> = HashMap::new();

        for cidx in 0..num_columns {
            let col = batch.column(cidx);
            let field = batch_schema.field(cidx);
            let name = field.name();
            let dtype = col.data_type();
            let column_data = col.data();

            let new_array = match dtype {
                DataType::Boolean => {
                    schema.insert(name.clone(), "bool".to_string());
                    Arc::new(BooleanArray::from(column_data)) as ArrayRef
                },
                DataType::Int32 => {
                    schema.insert(name.clone(), "i32".to_string());
                    Arc::new(Int32Array::from(column_data)) as ArrayRef
                },
                DataType::Int64 => {
                    schema.insert(name.clone(), "i64".to_string());
                    Arc::new(Int64Array::from(column_data)) as ArrayRef
                },
                DataType::Float64 => {
                    schema.insert(name.clone(), "f64".to_string());
                    Arc::new(Float64Array::from(column_data)) as ArrayRef
                },
                DataType::Date32(DateUnit::Day) => {
                    schema.insert(name.clone(), "date".to_string());
                    Arc::new(Date32Array::from(column_data)) as ArrayRef
                },
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    schema.insert(name.clone(), "datetime".to_string());
                    Arc::new(TimestampMillisecondArray::from(column_data)) as ArrayRef
                },
                DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
                    DataType::Int32 => {
                        schema.insert(name.clone(), "string".to_string());
                        Arc::new(DictionaryArray::<Int32Type>::from(column_data)) as ArrayRef
                    },
                    dtype => panic!("Unexpected dictionary key type {:?}", dtype),
                },
                dtype => panic!("Unexpected data type {:?}", dtype),
            };

            column_names.push(name.clone());
            data.insert(name.clone(), new_array);
        }

        ArrowAccessor {
            schema,
            column_names,
            data
        }
    }

    pub fn get_i32(&self, column_name: &str, ridx: usize) -> Option<i32> {
        let col = &self.data[column_name].as_any().downcast_ref::<Int32Array>().take().unwrap();
        Some(col.value(ridx))
    }

    pub fn get_i64(&self, column_name: &str, ridx: usize) -> Option<i64> {
        let col = &self.data[column_name].as_any().downcast_ref::<Int64Array>().take().unwrap();
        Some(col.value(ridx))
    }

    pub fn get_f64(&self, column_name: &str, ridx: usize) -> Option<f64> {
        let col = &self.data[column_name].as_any().downcast_ref::<Float64Array>().take().unwrap();
        Some(col.value(ridx))
    }

    pub fn get_date(&self, column_name: &str, ridx: usize) -> Option<NaiveDate> {
        let col = &self.data[column_name].as_any().downcast_ref::<Date32Array>().take().unwrap();
        Some(col.value_as_date(ridx).unwrap())
    }

    pub fn get_time(&self, column_name: &str, ridx: usize) -> Option<NaiveDateTime> {
        let col = &self.data[column_name].as_any().downcast_ref::<TimestampMillisecondArray>().take().unwrap();
        Some(col.value_as_datetime(ridx).unwrap())
    }

    pub fn get_bool(&self, column_name: &str, ridx: usize) -> Option<bool> {
        let col = &self.data[column_name].as_any().downcast_ref::<BooleanArray>().take().unwrap();
        Some(col.value(ridx))
    }

    pub fn get_string(&self, column_name: &str, ridx: usize) -> Option<String> {
        let dict_array = &self.data[column_name].as_any().downcast_ref::<Int32DictionaryArray>().take().unwrap();
        let values = dict_array.values();
        let strings = values.as_any().downcast_ref::<StringArray>().take().unwrap();
        let key = dict_array.keys_array().value(ridx) as usize;
        Some(String::from(strings.value(key)))
    }

    pub fn is_valid(&self, column_name: &str, ridx: usize) -> bool {
        match self.data.get(column_name) {
            Some(col) => ridx < self.data[column_name].len() && col.is_valid(ridx) && !col.is_null(ridx),
            None => false
        }
    }
}

impl fmt::Display for ArrowAccessor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (name, column) in &self.data {
            let dtype = column.data_type();
            write!(f, "{}: {}\n", name, dtype.to_json());
        }
        write!(f, "\n")
    }
}