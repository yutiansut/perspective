/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

import {registerPlugin} from "@finos/perspective-viewer/dist/esm/utils.js";

import "regular-table";
import {configureRegularTable, formatters} from "regular-table/dist/examples/perspective.js";
import MATERIAL_STYLE from "../less/regular_table.less";

import {configureRowSelectable, deselect} from "./row_selection.js";
import {configureEditable} from "./editing.js";
import {configureSortable} from "./sorting.js";

import("../../../../rust/perspective/arrow_accessor/pkg/arrow_accessor").then(mod => {
    window.arrow_accessor = mod;
});

const VIEWER_MAP = new WeakMap();
const INSTALLED = new WeakMap();

function lock(body) {
    let lock;
    return async function(...args) {
        while (lock) {
            await lock;
        }

        let resolve;
        try {
            lock = new Promise(x => (resolve = x));
            await body.apply(this, args);
        } catch (e) {
            throw e;
        } finally {
            lock = undefined;
            resolve();
        }
    };
}

const FORMATTERS = {
    datetime: Intl.DateTimeFormat("en-us", {
        week: "numeric",
        year: "numeric",
        month: "numeric",
        day: "numeric",
        hour: "numeric",
        minute: "numeric",
        second: "numeric"
    }),
    date: Intl.DateTimeFormat("en-us"),
    integer: Intl.NumberFormat("en-us"),
    float: new Intl.NumberFormat("en-us", {
        style: "decimal",
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    })
};

function _format(parts, val, use_table_schema = false) {
    if (val === null) {
        return "-";
    }
    const title = parts[parts.length - 1];
    const type = (use_table_schema && this._table_schema[title]) || this._schema[title] || "string";
    return FORMATTERS[type] ? FORMATTERS[type].format(val) : val;
}

function* _tree_header(paths = [], row_headers) {
    for (let path of paths) {
        path = ["TOTAL", ...path];
        const last = path[path.length - 1];
        path = path.slice(0, path.length - 1).fill("");
        const formatted = _format.call(this, [row_headers[path.length - 1]], last, true);
        path = path.concat({toString: () => formatted});
        path.length = row_headers.length + 1;
        yield path;
    }
}

async function dataListener(x0, y0, x1, y1) {
    let columns = {};
    let arrow_data, accessor;

    if (x1 - x0 > 0 && y1 - y0 > 0) {
        let start1 = performance.now();
        if (this._config.row_pivots.length == 0 && this._config.column_pivots.length == 0) {
            let arrow = await this._view.to_arrow({
                start_row: y0,
                start_col: x0,
                end_row: y1,
                end_col: x1,
                id: true
            });

            accessor = window.arrow_accessor.accessor_make(new Uint8Array(arrow));
            console.log(`arrow accessor took ${performance.now() - start1} ms`);
            arrow_data = window.arrow_accessor.accessor_get_data(accessor);
            window.arrow_accessor.accessor_drop(accessor);
        } else {
            let start1 = performance.now();
            columns = await this._view.to_columns({
                start_row: y0,
                start_col: x0,
                end_row: y1,
                end_col: x1,
                id: true
            });
            console.log(`to_columns took ${performance.now() - start1} ms`);
        }
        this._ids = columns.__ID__;
    }

    const data = [];
    const column_headers = [];

    for (const [cidx, path] of this._column_paths.slice(x0, x1).entries()) {
        const path_parts = path.split("|");
        // const column = columns[path] || new Array(y1 - y0).fill(null);
        const column = arrow_data[cidx];
        data.push(column.map(x => _format.call(this, path_parts, x)));
        column_headers.push(path_parts);
    }

    return {
        num_rows: this._num_rows,
        num_columns: this._column_paths.length,
        row_headers: Array.from(_tree_header.call(this, columns.__ROW_PATH__, this._config.row_pivots)),
        column_headers,
        data
    };
}

async function createModel(regular, table, view, extend = {}) {
    const config = await view.get_config();
    const [table_schema, table_computed_schema, num_rows, schema, computed_schema, column_paths] = await Promise.all([
        table.schema(),
        table.computed_schema(config.computed_columns),
        view.num_rows(),
        view.schema(),
        view.computed_schema(),
        view.column_paths()
    ]);

    const model = Object.assign(extend, {
        _view: view,
        _table: table,
        _table_schema: {...table_schema, ...table_computed_schema},
        _config: config,
        _num_rows: num_rows,
        _schema: {...schema, ...computed_schema},
        _ids: [],
        _column_paths: column_paths.filter(path => {
            return path !== "__ROW_PATH__" && path !== "__ID__";
        })
    });

    regular.setDataListener(dataListener.bind(model));
    return model;
}

const datagridPlugin = lock(async function(regular, viewer, view) {
    const is_installed = INSTALLED.has(regular);
    const table = viewer.table;
    let model;
    if (!is_installed) {
        model = await createModel(regular, table, view);
        configureRegularTable(regular, model);
        await configureRowSelectable.call(model, regular, viewer);
        await configureEditable.call(model, regular, viewer);
        await configureSortable.call(model, regular, viewer);
        INSTALLED.set(regular, model);
    } else {
        model = INSTALLED.get(regular);
        await createModel(regular, table, view, model);
    }

    try {
        const draw = regular.draw({swap: true});
        if (!model._preserve_focus_state) {
            regular.scrollTop = 0;
            regular.scrollLeft = 0;
            deselect(regular, viewer);
            regular._resetAutoSize();
        } else {
            model._preserve_focus_state = false;
        }

        await draw;
    } catch (e) {
        console.error(e);
    }
});

/**
 * Initializes a new datagrid renderer if needed, or returns an existing one
 * associated with a rendering `<div>` from a cache.
 *
 * @param {*} element
 * @param {*} div
 * @returns
 */
function get_or_create_datagrid(element, div) {
    let datagrid;
    if (!VIEWER_MAP.has(div)) {
        datagrid = document.createElement("regular-table");
        datagrid.formatters = formatters;
        div.innerHTML = "";
        div.appendChild(document.createElement("slot"));
        element.appendChild(datagrid);
        VIEWER_MAP.set(div, datagrid);
    } else {
        datagrid = VIEWER_MAP.get(div);
        if (!datagrid.isConnected) {
            div.innerHTML = "";
            div.appendChild(document.createElement("slot"));
            datagrid.clear();
            element.appendChild(datagrid);
        }
    }

    return datagrid;
}

/**
 * <perspective-viewer> plugin.
 *
 * @class DatagridPlugin
 */
class DatagridPlugin {
    static name = "Datagrid";
    static selectMode = "toggle";
    static deselectMode = "pivots";

    static async update(div) {
        try {
            const datagrid = VIEWER_MAP.get(div);
            const model = INSTALLED.get(datagrid);
            model._num_rows = await model._view.num_rows();
            await datagrid.draw();
        } catch (e) {
            return;
        }
    }

    static async create(div, view) {
        const datagrid = get_or_create_datagrid(this, div);
        try {
            await datagridPlugin(datagrid, this, view);
        } catch (e) {
            return;
        }
    }

    static async resize() {
        if (this.view && VIEWER_MAP.has(this._datavis)) {
            const datagrid = VIEWER_MAP.get(this._datavis);
            try {
                await datagrid.draw();
            } catch (e) {
                return;
            }
        }
    }

    static delete() {
        if (this.view && VIEWER_MAP.has(this._datavis)) {
            const datagrid = VIEWER_MAP.get(this._datavis);
            datagrid.clear();
        }
    }

    static save() {}

    static restore() {}
}

/**
 * Appends the default tbale CSS to `<head>`, should be run once on module
 * import.
 *
 */
function _register_global_styles() {
    const style = document.createElement("style");
    style.textContent = MATERIAL_STYLE;
    document.head.appendChild(style);
}

/******************************************************************************
 *
 * Main
 *
 */

registerPlugin("datagrid", DatagridPlugin);

_register_global_styles();
