/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/util/decimal.h>

/**
 * @brief Given a pointer to an arrow buffer somewhere on the heap, convert
 * it to an Arrow Table on the heap.
 */
arrow::Table* load_buffer(const uintptr_t ptr, std::int32_t buffer_length);

/**
 * @brief Load a buffer of the Arrow stream format.
 * 
 * @param ptr 
 * @param buffer_length 
 * @return arrow::Table* 
 */
arrow::Table* load_buffer_stream(const uintptr_t ptr, std::int32_t buffer_length);

/**
 * @brief Load a buffer of the arrow file format.
 * 
 * @param ptr 
 * @param buffer_length 
 * @return arrow::Table* 
 */
arrow::Table* load_buffer_file(const uintptr_t ptr, std::int32_t buffer_length);

/**
 * @brief Return the schema from an arrow table.
 * 
 * @param table 
 * @return arrow::Schema* 
 */
arrow::Schema* get_schema(const uintptr_t table);
