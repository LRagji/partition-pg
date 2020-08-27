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
// Add validation for non negative reads and widths This lib only supports positive numbers for partition.
const pgp = require('pg-promise');

module.exports = class PartionPg {
    constructor(pgpReaderConnection, pgpWriterConnection, schemaName, name) {
        //Private functions and mebers
        this._generateWhereClause = this._generateWhereClause.bind(this);
        this._generateBulkInsert = this._generateBulkInsert.bind(this);
        this._groupByPartionKey = this._groupByPartionKey.bind(this);
        this._calculateFrameStart = this._calculateFrameStart.bind(this);
        this._calculateFrameEndFromStart = this._calculateFrameEndFromStart.bind(this);
        this._calculateFrameEnd = this._calculateFrameEnd.bind(this);
        this._generateSqlTableColumns = this._generateSqlTableColumns.bind(this);
        this._generatePrimaryKeyConstraintColumns = this._generatePrimaryKeyConstraintColumns.bind(this);
        this._generateSqlIndexColumns = this._generateSqlIndexColumns.bind(this);
        this._calculateFrameStart = this._calculateFrameStart.bind(this);
        this._calculateFrameStart = this._calculateFrameStart.bind(this);
        this._calculateFrameStart = this._calculateFrameStart.bind(this);
        this._combineResults = this._combineResults.bind(this);
        this._tableDoesnotExists = this._tableDoesnotExists.bind(this);

        this.filterOperators = new Map();
        this.filterOperators.set("=", function (operands) { return operands[0] });
        this.filterOperators.set("IN", function (operands) { return `VALUES (${operands.reduce((a, e) => a + `"${e}",`, "").slice(0, -1)})` });

        //Public functiond and members
        this.load = this.load.bind(this);
        this.define = this.define.bind(this);
        this.upsert = this.upsert.bind(this);
        this.readRange = this.readRange.bind(this);
        //this.readIn = this.readIn.bind(this);

        //Constructor code
        //this._pgp = pgp;
        this._dbReader = pgpReaderConnection;//this._pgp(readerConnectionstring);
        this._dbWriter = pgpWriterConnection;//this._checkForSimilarConnection(readerConnectionstring, writerConnectionstring, this._dbReader);
        this.schemaName = schemaName;
        this.tableName = name;
        this.columnsNames = [];
        this._partitionKey = undefined;
    }

    load(definition) {
        let index = definition.findIndex(c => c.key != undefined);
        this._partitionKey = { "index": index, "range": parseInt(definition[index].key.range) };
        this.columnsNames = definition.map(e => e.name);

    }

    async define(definition) {

        this.load(definition);
        let tableColumns = definition.reduce(this._generateSqlTableColumns, "");
        let indexColumns = definition.reduce(this._generateSqlIndexColumns, "");
        let primaryKeyColumns = definition.reduce(this._generatePrimaryKeyConstraintColumns, "");
        primaryKeyColumns = primaryKeyColumns.slice(0, -1);
        tableColumns = tableColumns.slice(0, -1);
        indexColumns = indexColumns.slice(0, -1);


        let unsafeSql = `CREATE FUNCTION $[schema_name:name].$[function_name:name] (IN name TEXT) RETURNS VOID
        LANGUAGE 'plpgsql'
        AS $$
        DECLARE
        table_name TEXT := $[table_name] || '_' || name;
        index_name TEXT := table_name ||'_idx';
        primarykey_name TEXT := table_name ||'_pk';
        dsql TEXT;
        BEGIN
        dsql:= 'SELECT pg_advisory_lock(hashtext($1)); ';
        dsql:= dsql ||'CREATE TABLE IF NOT EXISTS $[schema_name:name].'|| quote_ident(table_name) || '(${tableColumns} ,CONSTRAINT '|| quote_ident(primarykey_name)||' PRIMARY KEY (${primaryKeyColumns})); ';
        dsql:= dsql ||'CREATE INDEX IF NOT EXISTS '|| quote_ident(index_name) ||' ON $[schema_name:name].' || quote_ident(table_name) || ' (${indexColumns});';
        EXECUTE dsql USING table_name;
        END$$;`;
        await this._dbWriter.none(pgp.as.format(unsafeSql, {
            "schema_name": this.schemaName,
            "function_name": ("auto_part_" + this.tableName),
            "table_name": this.tableName
        }));

    }

    async upsert(payload) {
        let groups = payload.reduce(this._groupByPartionKey, new Map());
        let groupKeyIterator = groups.keys();
        let groupKeyCursor = groupKeyIterator.next();
        while (!groupKeyCursor.done) {
            let sql = this._generateBulkInsert(groupKeyCursor.value, groups.get(groupKeyCursor.value));
            //console.log(sql);
            await this._dbWriter.none(sql);// This is sequential cause the write speed is shared even if run it in parallel it will take same time.
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
        let calculatedFrom = this._calculateFrameStart(from, width);
        let calculatedTo = this._calculateFrameEnd(to, width);

        let columns = columnIndexes.reduce((acc, i) => acc + `"${this.columnsNames[i]}",`, "");
        columns = columns === "" ? this.columnsNames.reduce((acc, e) => acc + `"${e}",`, "").slice(0, -1) : columns.slice(0, -1);

        let whereCondition = this._generateWhereClause(filters);

        let sqlStatementsExecutions = [];
        while (calculatedFrom < calculatedTo) {
            let whereClause = "";
            if (calculatedFrom >= from && this._calculateFrameEndFromStart(calculatedFrom, width) <= to) {
                whereClause = "";
            }
            else {
                whereClause = `WHERE "${this.columnsNames[this._partitionKey.index]}" BETWEEN ${from} AND ${to} `;
            }
            if (whereClause === "" && whereCondition != "") {//0 1
                whereClause += `WHERE ${whereCondition} `;
            }
            // else if (whereClause === "" && whereCondition === "") {//0 0
            //     whereClause = "";
            // }
            else if (whereClause != "" && whereCondition != "") {//1 1
                whereClause += " AND " + whereCondition;
            }
            // else if (whereClause != "" && whereCondition === "") {//1 0

            // }

            let tableName = `${calculatedFrom}_${this._calculateFrameEndFromStart(calculatedFrom, width)}`;
            sqlStatementsExecutions.push(
                this._dbReader.any(`
            SELECT ${columns} 
            FROM "${this.schemaName}"."${this.tableName}_${tableName}"
            ${whereClause} ;`));

            calculatedFrom += width;
        }

        let resultsArray = await Promise.allSettled(sqlStatementsExecutions)
        return resultsArray.reduce(this._combineResults, []);
    }

    // async readIn(values, columnIndexes = [], filters = []) {
    //     let width = this._partitionKey.range;
    //     // if (from > to) [from, to] = [to, from];
    //     // let CalculatedFrom = this._calculateFrameStart(from, width);
    //     // let CalculatedTo = this._calculateFrameEnd(to, width);

    //     let columns = columnIndexes.reduce((acc, i) => acc + `"${this.columnsNames[i]}",`, "");
    //     columns = columns === "" ? "*" : columns.slice(0, -1);

    //     let whereCondition = this._generateWhereClause(filters);

    //     let combinedValues = values.reduce((acc, value) => {
    //         let tableName = `${this._calculateFrameStart(value, width)}_${this._calculateFrameEnd(value, width)}"`;
    //         if (acc.has(tableName)) {
    //             acc.get(tableName).push(value);
    //         }
    //         else {
    //             acc.set(tableName, [value]);
    //         }
    //         return acc;
    //     }, new Map());

    //     let sql = "";
    //     combinedValues.forEach((tableValues, tableName) => {
    //         sql = `
    //         SELECT ${columns} 
    //         FROM "${this.schemaName}"."${this.tableName}_${tableName}"
    //         WHERE "${ this.columnsNames[this._partitionKey.index]}" IN VALUES (${tableValues.join(",").slice(0, -1)}) AND ${CalculatedTo} ${whereCondition.length > 0 ? (" AND " + whereCondition) : ""}
    //         UNION ALL`;
    //     });
    //     sql = sql.slice(0, -9) + ";";

    //     return await this._dbReader.any(sql);
    // }

    _combineResults(accumulatedResults, singleResult) {
        if (Array.isArray(accumulatedResults)) {
            if (singleResult.status === 'fulfilled') {
                return accumulatedResults.concat(singleResult.value);
            }
            else if (this._tableDoesnotExists(singleResult)) {
                return accumulatedResults;
            }
            else {
                return Promise.reject(singleResult.reason);
            }
        }
    }

    _tableDoesnotExists(queryResult) {
        //This is a valid scenario from design as the requested table doesnot exists
        return queryResult.status === 'rejected' && queryResult.reason.code === "42P01";
    }

    _generateWhereClause(filters) {
        let firstCondition = filters.findIndex(e => e.combine == undefined);
        if (firstCondition < 0) return "";
        let nonDependentCondition = filters.splice(firstCondition, 1);
        filters = filters.sort((cA, cB) => parseInt(cA.combine["condition-index"]) - parseInt(cB.combine["condition-index"]));
        filters.unshift(nonDependentCondition[0]);
        return filters.reduce((acc, condition) => {
            if (condition.combine != undefined) {
                acc += ` ${condition.combine.using} "${condition.name}" ${condition.operator} ${this.filterOperators.get(condition.operator)(condition.values)}`;
            }
            else {
                acc += ` "${condition.name}" ${condition.operator} ${this.filterOperators.get(condition.operator)(condition.values)}`;
            }
            return acc;
        }, "");
    }

    _generateBulkInsert(tableName, payload) {
        let valuesString = payload.reduce((valuesString, element) => valuesString += `(${element.join(",")}),`, "");
        let columnNames = this.columnsNames.reduce((acc, e) => acc + `"${e}",`, "");
        columnNames = columnNames.slice(0, -1);
        let updateColumns = this.columnsNames.reduce((acc, e) => acc + ` "${e}"=EXCLUDED."${e}",`, "");
        updateColumns = updateColumns.slice(0, -1);

        return `SELECT "${this.schemaName}"."auto_part_${this.tableName}"('${tableName}');
        INSERT INTO "${this.schemaName}"."${this.tableName}_${tableName}" (${columnNames}) VALUES ${valuesString.slice(0, -1)}
        ON CONFLICT ON CONSTRAINT "${this.tableName}_${tableName}_pk"
        DO UPDATE SET ${updateColumns};`;
    }

    _groupByPartionKey(groups, row) {
        let currentValue = row[this._partitionKey.index];
        let width = this._partitionKey.range;
        let frameStart = this._calculateFrameStart(currentValue, width);
        let frameEnd = this._calculateFrameEndFromStart(frameStart, width)
        let tableName = `${frameStart}_${frameEnd}`;
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
// const defaultConectionString = "postgres://postgres:@localhost:5432/postgres";
// let x = new PartionPg(defaultConectionString, defaultConectionString, "public", "laukik");
// x.define([
//     {

//         "name": "value",
//         "datatype": "bigint",
//         "filterable": { "sorted": "asc | desc" },
//         "key": {
//             "range": 10
//         }
//     }
// ])

//Bloom sort actually we need count min sketch for sorting as there can be duplicates test show fast sorting is equal faster.
// let BloomFilter = require('bloomfilter').BloomFilter;

// let hulkArray = []
// let length = 20000000;
// let min = 1598140800021;
// let max = min + length
// let maxActual = Number.MIN_VALUE, minActual = Number.MAX_VALUE;
// console.time("Fill");
// var bloom = new BloomFilter(
//     length, // number of bits to allocate.
//     16        // number of hash functions.
// );
// for (let index = 0; index < length; index++) {
//     let timestamp = (Math.random() * (max - min) + min);
//     if (maxActual < timestamp) maxActual = timestamp;
//     if (minActual > timestamp) minActual = timestamp;
//     // hulkArray.push({
//     //     "TagId": (Math.random() * (1000000 - 1) + 1),
//     //     "Timestamp": timestamp,
//     //     "Value": max,
//     //     "Quality": max
//     // });
//     hulkArray.push(timestamp);
//     bloom.add(timestamp);
// }
// console.timeEnd("Fill");
// console.log(`Min: ${minActual} Max: ${maxActual}`);
// console.time("Sort");
// let sortedHulkArray = [];
// for (let index = minActual; index <= maxActual; index++) {
//     if (bloom.test(index)) sortedHulkArray.push(index);
// }
// console.timeEnd("Sort");
// console.log("Sorted Array Length: " + sortedHulkArray.length);
// let s = require('fast-sort');

// console.time("Fast Sort");
// let actual = s(hulkArray).asc();
// console.timeEnd("Fast Sort");
// console.log(`Are they Equal in length actual: ${actual.length} bloom: ${sortedHulkArray.length} equal: ${actual.length === sortedHulkArray.length}`);
// let areEqual = true;
// for (let index = 0; index < actual.length; index++) {
//     areEqual = actual[index] === sortedHulkArray[index] && areEqual;
//     if (!areEqual) {
//         console.log(`You screwed at ${index} actual: ${actual[index]} bloom: ${sortedHulkArray[index]}`);
//         break;
//     }
// }
// console.log("Are they Equal: " + areEqual);