// const pgpType = require('pg-promise');
// const pgp = pgpType();
// pgp.pg.types.setTypeParser(20, BigInt); // This is for serialization bug of BigInts as strings.

//TODO:
// Write tests performance
// Write tests unit
// Write tests end to end
// Sanitize Sql inputs 
// Make it TypeScript for interface atleast
// Add peer dependency for PG
// Add readme


module.exports = class PartionPg {
    constructor(pgpReaderConnection, pgpWriterConnection, schemaName, name) {
        //Private functions and mebers
        this._checkForSimilarConnection = this._checkForSimilarConnection.bind(this);
        this._generateSqlTableColumns = this._generateSqlTableColumns.bind(this);
        this._generateSqlIndexColumns = this._generateSqlIndexColumns.bind(this);
        this._generateBulkInsert = this._generateBulkInsert.bind(this);
        this._generatePrimaryKeyConstraintColumns = this._generatePrimaryKeyConstraintColumns.bind(this);
        this.filterOperators = new Map();
        this.filterOperators.set("=", function (operands) { return operands[0] });
        this.filterOperators.set("IN", function (operands) { return `VALUES (${operands.reduce((a, e) => a + `"${e}",`, "").slice(0, -1)})` });

        //Public functiond and members
        this.load = this.load.bind(this);
        this.define = this.define.bind(this);
        this.upsert = this.upsert.bind(this);
        this.readRange = this.readRange.bind(this);
        this.readIn = this.readIn.bind(this);

        //Constructor code
        //this._pgp = pgp;
        this._dbReader = pgpReaderConnection;//this._pgp(readerConnectionstring);
        this._dbWriter = pgpWriterConnection;//this._checkForSimilarConnection(readerConnectionstring, writerConnectionstring, this._dbReader);
        this._schemaName = schemaName;
        this._name = name;
        this._columnsNames = [];
        this._partitionKey = undefined;
    }

    load(definition) {
        let index = definition.findIndex(c => c.key != undefined);
        this._partitionKey = { "index": index, "range": parseInt(definition[index].key.range) };
        this._columnsNames = definition.map(e => e.name);

    }

    async define(definition) {

        this.load(definition);
        let tableColumns = definition.reduce(this._generateSqlTableColumns, "");
        let indexColumns = definition.reduce(this._generateSqlIndexColumns, "");
        let primaryKeyColumns = definition.reduce(this._generatePrimaryKeyConstraintColumns, "");
        primaryKeyColumns = primaryKeyColumns.slice(0, -1);
        tableColumns = tableColumns.slice(0, -1);
        indexColumns = indexColumns.slice(0, -1);


        let unsafeSql = `CREATE FUNCTION "${this._schemaName}"."${"auto_part_" + this._name}"(IN name TEXT) RETURNS VOID
        LANGUAGE 'plpgsql'
        AS $$
        DECLARE
        table_name TEXT := '${this._name}_'|| name;
        index_name TEXT := '${this._name}_'|| name||'_idx';
        primarykey_name TEXT := '${this._name}_'|| name||'_pk';
        dsql TEXT;
        BEGIN
        dsql:= 'CREATE TABLE IF NOT EXISTS "${this._schemaName}".'|| quote_ident(table_name) || '(${tableColumns} ,CONSTRAINT '|| quote_ident(primarykey_name)||' PRIMARY KEY (${primaryKeyColumns}))';
        EXECUTE dsql;
        dsql:= 'CREATE INDEX IF NOT EXISTS '|| quote_ident(index_name) ||' ON "${this._schemaName}".' || quote_ident(table_name) || ' (${indexColumns})';
        EXECUTE dsql;
        END$$;`;

        await this._dbWriter.none(unsafeSql);


    }

    async upsert(payload) {
        let groups = payload.reduce(this._groupByPartionKey, new Map());
        let groupKeyIterator = groups.key();
        let groupKeyCursor = groupKeyIterator.next();
        while (!groupKeyCursor.done) {
            let sql = this._generateBulkInsert(groupKeyCursor.value, groups.get(groupKeyCursor.value));
            await this._dbWriter.none(sql);
            groupKeyCursor = groupKeyIterator.next();
        }
    }

    async readRange(from, to, columnIndexes = [], filters = []) {

        // {
        //     "name": "quality",
        //     "operator": "=",
        //     "values": [1],
        //     "combine": {
        //         "condition-index": 1,
        //         "using": "and"
        //     }
        // }

        let width = this._partitionKey.range;
        if (from > to) [from, to] = [to, from];
        let CalculatedFrom = this._calculateFrameStart(from, width);
        let CalculatedTo = this._calculateFrameEnd(to, width);

        let columns = columnIndexes.reduce((acc, i) => acc + `"${this._columnsNames[i]}",`, "");
        columns = columns === "" ? "*" : columns.slice(0, -1);

        let whereCondition = this._generateWhereClause(filters);

        let sql = "";
        for (CalculatedFrom < CalculatedTo; CalculatedFrom += width;) {
            let tableName = `${CalculatedFrom}_${this._calculateFrameEndFromStart(CalculatedFrom, width)}"`;
            sql = `
            SELECT ${columns} 
            FROM "${this._schemaName}"."${this._name}_${tableName}"
            WHERE "${ this._columnsNames[this._partitionKey.index]}" BETWEEN ${CalculatedFrom} AND ${CalculatedTo} ${whereCondition.length > 0 ? (" AND " + whereCondition) : ""}
            UNION ALL`;
        }
        sql = sql.slice(0, -9) + ";";

        return await this._dbReader.any(sql);

    }

    async readIn(values, columnIndexes = [], filters = []) {
        let width = this._partitionKey.range;
        // if (from > to) [from, to] = [to, from];
        // let CalculatedFrom = this._calculateFrameStart(from, width);
        // let CalculatedTo = this._calculateFrameEnd(to, width);

        let columns = columnIndexes.reduce((acc, i) => acc + `"${this._columnsNames[i]}",`, "");
        columns = columns === "" ? "*" : columns.slice(0, -1);

        let whereCondition = this._generateWhereClause(filters);

        let combinedValues = values.reduce((acc, value) => {
            let tableName = `${this._calculateFrameStart(value, width)}_${this._calculateFrameEnd(value, width)}"`;
            if (acc.has(tableName)) {
                acc.get(tableName).push(value);
            }
            else {
                acc.set(tableName, [value]);
            }
            return acc;
        }, new Map());

        let sql = "";
        combinedValues.forEach((tableValues, tableName) => {
            sql = `
            SELECT ${columns} 
            FROM "${this._schemaName}"."${this._name}_${tableName}"
            WHERE "${ this._columnsNames[this._partitionKey.index]}" IN VALUES (${tableValues.join(",").slice(0, -1)}) AND ${CalculatedTo} ${whereCondition.length > 0 ? (" AND " + whereCondition) : ""}
            UNION ALL`;
        });
        sql = sql.slice(0, -9) + ";";

        return await this._dbReader.any(sql);
    }

    _generateWhereClause(filters) {
        let whereClauses = filters.map(e => `"${e.name}" ${e.operator} ${this.filterOperators.get(e.operator)(e)}`);
        let firstCondition = filters.findIndex(e => e.combine == undefined);
        filters.splice(firstCondition, 1);
        filters = filters.sort((cA, cB) => parseInt(cA.combine["condition-index"]) - parseInt(cB.combine["condition-index"]));
        return filters.reduce((acc, condition) => {
            if (condition.combine["condition-index"] != undefined) {
                acc += ` ${condition.combine.using}  ${whereClauses[condition.combine["condition-index"]]} `;
            }
            return acc;
        }, whereClauses[firstCondition]);
    }

    _generateBulkInsert(tableName, payload) {
        let valuesString = payload.reduce((valuesString, element) => {
            valuesString += `(${element.join(",")}),`;
        }, "");
        let columnNames = this._columnsNames.reduce((acc, e) => acc + `,"${e}",`, "");
        columnNames = columnNames.slice(0, -1);
        let updateColumns = this._columnsNames.reduce((acc, e) => acc + `,"${e}"=EXCLUDED."${e}",`, "");
        updateColumns = updateColumns.slice(0, -1);

        return `SELECT "${this._schemaName}"."auto_part_${this._name}"(${tableName});
        INSERT INTO "${this._schemaName}"."${this._name}_${tableName}" (${columnNames}) VALUES ${valuesString.slice(0, -1)}
        ON CONFLICT ON CONSTRAINT "${this._name}_${tableName}_pk"
	    DO UPDATE SET ${updateColumns};`;
    }

    _groupByPartionKey(groups, row) {
        let currentValue = row[this._partitionKey.index];
        let width = this._partitionKey.range;
        let frameStart = this._calculateFrameStart(currentValue, width);
        let frameEnd = this._calculateFrameEndFromStart(frameStart, width)
        let tableName = `${frameStart}_${frameEnd}"`;
        if (groups.has(tableName)) {
            groups.get(tableName).push(row);
        }
        else {
            groups.set(tableName, [row]);
        }

        return groups;
    }

    _calculateFrameStart(value, range) {
        return value - (value % range);
    }

    _calculateFrameEndFromStart(frameStart, range) {
        return frameStart + (range - 1);
    }

    _calculateFrameEnd(value, range) {
        return this._calculateFrameEndFromStart(this._calculateFrameStart(value, range), range);
    }

    _generateSqlTableColumns(completeSql, schema) {
        // let schema = {
        //     "name": "value",
        //     "datatype": "bigint|integer|float|json|byte[]",
        //     "filterable": { "sorted": "asc | desc" },
        //     "primary":true,
        //     "key": {
        //         "range":10
        //     }
        // } "TagId" INTEGER NOT NULL,
        completeSql += ` "${schema.name}" ${schema.datatype},`;
        return completeSql;
    }

    _generatePrimaryKeyConstraintColumns(completeSql, schema) {

        if (schema.primary === true) {
            completeSql += `"${schema.name}",`;
        }
        return completeSql;
    }

    _generateSqlIndexColumns(completeSql, schema) {

        if (schema.filterable != undefined) {
            completeSql += ` "${schema.name}" ${schema.filterable.sorted === 'asc' ? "ASC" : "DESC"},`;
        }
        return completeSql;
    }
}
const defaultConectionString = "postgres://postgres:@localhost:5432/postgres";
let x = new PartionPg(defaultConectionString, defaultConectionString, "public", "laukik");
x.define([
    {

        "name": "value",
        "datatype": "bigint",
        "filterable": { "sorted": "asc | desc" },
        "key": {
            "range": 10
        }
    }
])