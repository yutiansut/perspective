/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <perspective/expression_tables.h>

namespace perspective {

t_expression_tables::t_expression_tables(
    const std::vector<std::shared_ptr<t_computed_expression>>& expressions) {
    t_schema schema;
    t_schema transitions_schema;

    for (const auto& expr : expressions) {
        const std::string& alias = expr->get_expression_alias();
        schema.add_column(alias, expr->get_dtype());
        transitions_schema.add_column(alias, DTYPE_UINT8);
    }

    m_master = std::make_shared<t_data_table>(
        "", "", schema, DEFAULT_EMPTY_CAPACITY, BACKING_STORE_MEMORY);
    m_flattened = std::make_shared<t_data_table>(
        "", "", schema, DEFAULT_EMPTY_CAPACITY, BACKING_STORE_MEMORY);
    m_prev = std::make_shared<t_data_table>(
        "", "", schema, DEFAULT_EMPTY_CAPACITY, BACKING_STORE_MEMORY);
    m_current = std::make_shared<t_data_table>(
        "", "", schema, DEFAULT_EMPTY_CAPACITY, BACKING_STORE_MEMORY);
    m_delta = std::make_shared<t_data_table>(
        "", "", schema, DEFAULT_EMPTY_CAPACITY, BACKING_STORE_MEMORY);
    m_transitions = std::make_shared<t_data_table>("", "", transitions_schema,
        DEFAULT_EMPTY_CAPACITY, BACKING_STORE_MEMORY);

    m_master->init();
    m_flattened->init();
    m_prev->init();
    m_current->init();
    m_delta->init();
    m_transitions->init();
}

void
t_expression_tables::calculate_transitions(
    std::shared_ptr<t_data_table> existed) {
    const t_schema& schema = m_transitions->get_schema();
    const std::vector<std::string>& column_names = schema.m_columns;
    const t_column& existed_column
        = *(existed->get_const_column("psp_existed"));

    auto num_cols = column_names.size();

#ifdef PSP_PARALLEL_FOR
    tbb::parallel_for(0, int(num_cols), 1,
        [&column_names, &existed_column, this](int cidx)
#else
    for (t_uindex cidx = 0; cidx < num_cols; ++cidx)
#endif
        {
            const std::string& cname = column_names[cidx];
            const t_column& prev_column = *(m_prev->get_const_column(cname));
            const t_column& current_column
                = *(m_current->get_const_column(cname));
            std::shared_ptr<t_column> transition_column
                = m_transitions->get_column(cname);

            for (t_uindex ridx = 0; ridx < transition_column->size(); ++ridx) {
                bool row_existed = existed_column.get_nth<bool>(ridx);

                t_tscalar prev_value = prev_column.get_scalar(ridx);
                t_tscalar curr_value = current_column.get_scalar(ridx);

                bool prev_valid = prev_column.is_valid(ridx);
                bool curr_valid = current_column.is_valid(ridx);
                bool prev_curr_eq = prev_valid && curr_valid && (prev_value == curr_value);

                t_value_transition transition;

                // Use a small subset of `t_value_transitions` that are
                // relevant - I have not implemented the code paths in
                // `calc_transitions` that are not referenced elsewhere, i.e.
                // by a context or by a tree implementation.
                if (row_existed) {
                    if (prev_curr_eq) {
                        // Row existed before, and the current value is
                        // the same as the previous value.
                        transition = VALUE_TRANSITION_EQ_TT;
                    } else {
                        if (!prev_valid && curr_valid) {
                            // Previous value was a null, new value is valid.
                            transition = VALUE_TRANSITION_NEQ_FT;
                        } else {
                            // Previous value was not null, new value is
                            // not null, and previous value != new value
                            transition = VALUE_TRANSITION_NEQ_TT;
                        }
                    }
                } else {
                    // Row did not exist before and was added
                    transition = VALUE_TRANSITION_NEQ_FT;
                }

                transition_column->set_nth<std::uint8_t>(ridx, transition);
            }
        }
#ifdef PSP_PARALLEL_FOR
    );
#endif
}

void
t_expression_tables::reserve_transitional_table_size(t_uindex size) {
    m_flattened->reserve(size);
    m_prev->reserve(size);
    m_current->reserve(size);
    m_delta->reserve(size);
    m_transitions->reserve(size);
}

void
t_expression_tables::set_transitional_table_size(t_uindex size) {
    m_flattened->set_size(size);
    m_prev->set_size(size);
    m_current->set_size(size);
    m_delta->set_size(size);
    m_transitions->set_size(size);
}

void
t_expression_tables::clear_transitional_tables() {
    m_flattened->clear();
    m_prev->clear();
    m_current->clear();
    m_delta->clear();
    m_transitions->clear();
}

void
t_expression_tables::reset() {
    m_master->reset();
    m_flattened->reset();
    m_prev->reset();
    m_current->reset();
    m_delta->reset();
    m_transitions->reset();
}

} // end namespace perspective
