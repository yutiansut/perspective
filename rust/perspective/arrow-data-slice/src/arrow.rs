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
use std::sync::Arc;
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
    schema: Schema,
    data: Vec<ArrayRef>
}

impl ArrowAccessor {
    pub fn new(batch: Box<RecordBatch>, schema: Schema) -> Self {
        let num_columns = batch.num_columns();
        let mut data: Vec<ArrayRef> = Vec::new();

        for cidx in 0..num_columns {
            let col = batch.column(cidx);
            let column_data = col.data();

            let new_array = match col.data_type() {
                DataType::Boolean => Arc::new(BooleanArray::from(column_data)) as ArrayRef,
                DataType::Int32 => Arc::new(Int32Array::from(column_data)) as ArrayRef,
                DataType::Int64 => Arc::new(Int64Array::from(column_data)) as ArrayRef,
                DataType::Float64 => Arc::new(Float64Array::from(column_data)) as ArrayRef,
                DataType::Date32(DateUnit::Day) => Arc::new(Date32Array::from(column_data)) as ArrayRef,
                DataType::Timestamp(TimeUnit::Millisecond, _) =>
                    Arc::new(TimestampMillisecondArray::from(column_data)) as ArrayRef,
                DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
                    DataType::Int32 => 
                        Arc::new(DictionaryArray::<Int32Type>::from(column_data)) as ArrayRef,
                    dt => panic!("Unexpected dictionary key type {:?}", dt),
                },
                DataType::Null => Arc::new(NullArray::from(column_data)) as ArrayRef,
                dt => panic!("Unexpected data type {:?}", dt),
            };

            data.push(new_array);
        }

        ArrowAccessor {
            schema,
            data
        }
    }

    pub fn get<T>(&self, column_name: &str, ridx: usize) -> Option<T> {
        None
    }

    pub fn get<String>(&self, column_name: &str, ridx: usize) -> Option<String> {
        None
    }
}

impl fmt::Display for ArrowAccessor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in 0..self.data.len() {
            let field = self.schema.field(i);
            let name = field.name().clone();
            let col = &self.data[i];
            let dtype = col.data_type();

            write!(f, "{}: {}\n", name, dtype.to_json());
        }
        write!(f, "\n")
    }
}