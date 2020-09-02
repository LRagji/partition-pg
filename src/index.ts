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
import { default as MapPolyFill } from 'ts-map'; //MapPolyFill

interface internalePartitionKey {
    range: number,
    index: number
}

export interface condition {
    "condition-index": number,
    using: "and" | "or"
}
export interface filter {
    name: string,
    operator: string,
    values: Array<any>,
    combine: condition
}

export interface partitionKey {
    range: number
}
export interface filterable {
    sorted: string
}
export interface definition {
    name: string,
    datatype: string,
    filterable: filterable,
    primary: boolean,
    key: partitionKey
}

export default class PartionPg {

    //Public Props
    readonly filterOperators: MapPolyFill<string, (operands: Array<any>) => any>;
    readonly datatypes: MapPolyFill<string, string>;
    readonly schemaName: string;
    readonly tableName: string;
    columnsNames: Array<string>;

    //Private Fields
    #partitionKey: internalePartitionKey;
    #dbReader: any;
    #dbWriter: any;

    constructor(pgpReaderConnection: any, pgpWriterConnection: any, schemaName: string, name: string) {

        this.filterOperators = new MapPolyFill<string, (operands: Array<any>) => any>();
        this.filterOperators.set("=", function (operands: Array<any>) { return operands[0] });
        this.filterOperators.set("IN", function (operands: Array<any>) { return `VALUES (${operands.reduce((a, e) => a + `"${e}",`, "").slice(0, -1)})` });

        this.datatypes = new MapPolyFill<string, string>();
        this.datatypes.set("bigint", "bigint");
        this.datatypes.set("integer", "integer");
        this.datatypes.set("text", "text");
        this.datatypes.set("double", "double precision");

        //Public functions
        this.load = this.load.bind(this);
        this.define = this.define.bind(this);
        this.upsert = this.upsert.bind(this);
        this.readRange = this.readRange.bind(this);
        //this.readIn = this.readIn.bind(this);

        //Constructor code
        //this._pgp = pgp;
        this.#dbReader = pgpReaderConnection;//this._pgp(readerConnectionstring);
        this.#dbWriter = pgpWriterConnection;//this._checkForSimilarConnection(readerConnectionstring, writerConnectionstring, this.#dbReader);
        this.schemaName = schemaName;
        this.tableName = name;
        this.columnsNames = [];

        this.#partitionKey = { "index": -1, "range": -1 };
    }

    load(definition: Array<definition>) {
        let index = definition.findIndex(c => c.key != undefined);
        this.#partitionKey = { "index": index, "range": definition[index].key.range };
        this.columnsNames = definition.map(e => pgp.as.format('$1:alias', [e.name]));

    }

    async define(definition: Array<definition>) {

        this.load(definition);
        let tableColumns = definition.reduce(this.#generateSqlTableColumns, "");
        let indexColumns = definition.reduce(this.#generateSqlIndexColumns, "");
        let primaryKeyColumns = definition.reduce(this.#generatePrimaryKeyConstraintColumns, "");
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
        dsql:= dsql ||'CREATE TABLE IF NOT EXISTS $[schema_name:name].'|| quote_ident(table_name) || '($[columns:raw] ,CONSTRAINT '|| quote_ident(primarykey_name)||' PRIMARY KEY ($[primaryKeyColumns:raw])); ';
        dsql:= dsql ||'CREATE INDEX IF NOT EXISTS '|| quote_ident(index_name) ||' ON $[schema_name:name].' || quote_ident(table_name) || ' ($[indexColumns:raw]);';
        EXECUTE dsql USING table_name;
        END$$;`;
        await this.#dbWriter.none(pgp.as.format(unsafeSql, {
            "schema_name": this.schemaName,
            "function_name": ("auto_part_" + this.tableName),
            "table_name": this.tableName,
            "columns": tableColumns,
            "primaryKeyColumns": primaryKeyColumns,
            "indexColumns": indexColumns
        }));

    }

    async upsert(payload: Array<Array<any>>) {
        let groups = payload.reduce(this.#groupByPartionKey, new MapPolyFill<string, Array<any>>());
        let keys = groups.keys();
        for (let index = 0; index < keys.length; index++) {
            const key = keys[index];
            let groupRows = groups.get(key);
            if (groupRows !== undefined) {
                let sql = this.#generateBulkInsert(key, groupRows);
                await this.#dbWriter.none(sql);// This is sequential cause the write speed is shared even if run it in parallel it will take same time.
            }
        }
    }

    async readRange(from: number, to: number, columnIndexes: Array<number> = [], filters: Array<filter> = []): Promise<Array<any>> {

        // {
        //     "name": "quality",
        //     "operator": "=",
        //     "values": [1],
        //     "combine": {
        //         "condition-index": 1,
        //         "using": "and"
        //     }
        // }

        let width = this.#partitionKey.range;
        if (from > to) [from, to] = [to, from];
        let calculatedFrom = this.#calculateFrameStart(from, width);
        let calculatedTo = this.#calculateFrameEnd(to, width);

        let columns = columnIndexes.reduce((acc, i) => acc + `"${this.columnsNames[i]}",`, "");
        columns = columns === "" ? this.columnsNames.reduce((acc, e) => acc + `"${e}",`, "").slice(0, -1) : columns.slice(0, -1);

        let whereCondition = this.#generateWhereClause(filters);

        let sqlStatementsExecutions = [];
        while (calculatedFrom < calculatedTo) {
            let whereClause = "";
            if (calculatedFrom >= from && this.#calculateFrameEndFromStart(calculatedFrom, width) <= to) {
                whereClause = "";
            }
            else {
                whereClause = `WHERE "${this.columnsNames[this.#partitionKey.index]}" BETWEEN ${from} AND ${to} `;
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

            let tableName = `${calculatedFrom}_${this.#calculateFrameEndFromStart(calculatedFrom, width)}`;
            sqlStatementsExecutions.push(
                this.#dbReader.any(`
            SELECT ${columns} 
            FROM "${this.schemaName}"."${this.tableName}_${tableName}"
            ${whereClause} ;`));

            calculatedFrom += width;
        }

        let resultsArray = await Promise.allSettled(sqlStatementsExecutions)
        return resultsArray.reduce(this.#combineResults, []);
    }

    // async readIn(values, columnIndexes = [], filters = []) {
    //     let width = this.#partitionKey.range;
    //     // if (from > to) [from, to] = [to, from];
    //     // let CalculatedFrom = this.#calculateFrameStart(from, width);
    //     // let CalculatedTo = this.#calculateFrameEnd(to, width);

    //     let columns = columnIndexes.reduce((acc, i) => acc + `"${this.columnsNames[i]}",`, "");
    //     columns = columns === "" ? "*" : columns.slice(0, -1);

    //     let whereCondition = this.#generateWhereClause(filters);

    //     let combinedValues = values.reduce((acc, value) => {
    //         let tableName = `${this.#calculateFrameStart(value, width)}_${this.#calculateFrameEnd(value, width)}"`;
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
    //         WHERE "${ this.columnsNames[this.#partitionKey.index]}" IN VALUES (${tableValues.join(",").slice(0, -1)}) AND ${CalculatedTo} ${whereCondition.length > 0 ? (" AND " + whereCondition) : ""}
    //         UNION ALL`;
    //     });
    //     sql = sql.slice(0, -9) + ";";

    //     return await this.#dbReader.any(sql);
    // }

    #combineResults = (accumulatedResults: Array<any>, singleResult: PromiseSettledResult<any>): Array<any> => {
        if (Array.isArray(accumulatedResults)) {
            if (singleResult.status === 'fulfilled') {
                return accumulatedResults.concat(singleResult.value);
            }
            else if (this.#tableDoesnotExists(singleResult)) {
                return accumulatedResults;
            }
            else {
                throw singleResult.reason;
            }
        }
        else {
            return accumulatedResults;
        }
    }

    #tableDoesnotExists = (queryResult: any) => {
        //This is a valid scenario from design as the requested table doesnot exists
        return queryResult.status === 'rejected' && queryResult.reason.code === "42P01";
    }

    #generateWhereClause = (filters: Array<filter>): string => {
        let firstCondition = filters.findIndex(e => e.combine == undefined);
        if (firstCondition < 0) return "";
        let nonDependentCondition = filters.splice(firstCondition, 1);
        filters = filters.sort((cA, cB) => cA.combine["condition-index"] - cB.combine["condition-index"]);
        filters.unshift(nonDependentCondition[0]);
        return filters.reduce((acc, condition) => {
            let transformFunction = this.filterOperators.get(condition.operator);
            if (transformFunction == undefined) throw new Error("Operator not supported:" + condition.operator);
            if (condition.combine != undefined) {
                acc += ` ${condition.combine.using} "${condition.name}" ${condition.operator} ${transformFunction(condition.values)}`;
            }
            else {
                acc += ` "${condition.name}" ${condition.operator} ${transformFunction(condition.values)}`;
            }
            return acc;
        }, "");
    }

    #generateBulkInsert = (tableName: string, payload: Array<any>): string => {
        let valuesString = payload.reduce((valuesString: string, element) => valuesString += `(${element.join(",")}),`, "");
        let columnNames = this.columnsNames.reduce((acc, e) => acc + `"${e}",`, "");
        columnNames = columnNames.slice(0, -1);
        let updateColumns = this.columnsNames.reduce((acc, e) => acc + ` "${e}"=EXCLUDED."${e}",`, "");
        updateColumns = updateColumns.slice(0, -1);

        return `SELECT "${this.schemaName}"."auto_part_${this.tableName}"('${tableName}');
        INSERT INTO "${this.schemaName}"."${this.tableName}_${tableName}" (${columnNames}) VALUES ${valuesString.slice(0, -1)}
        ON CONFLICT ON CONSTRAINT "${this.tableName}_${tableName}_pk"
        DO UPDATE SET ${updateColumns};`;
    }

    #groupByPartionKey = (groups: MapPolyFill<string, Array<any>>, row: Array<any>): MapPolyFill<string, Array<any>> => {
        let currentValue = row[this.#partitionKey.index];
        let width = this.#partitionKey.range;
        let frameStart = this.#calculateFrameStart(currentValue, width);
        let frameEnd = this.#calculateFrameEndFromStart(frameStart, width)
        let tableName = `${frameStart}_${frameEnd}`;
        let existingResult = groups.get(tableName);
        if (existingResult !== undefined) {
            existingResult.push(row);
        }
        else {
            groups.set(tableName, [row]);
        }

        return groups;
    }

    #calculateFrameStart = (value: number, range: number): number => {
        return value - (value % range);
    }

    #calculateFrameEndFromStart = (frameStart: number, range: number): number => {
        return frameStart + (range - 1);
    }

    #calculateFrameEnd = (value: number, range: number): number => {
        return this.#calculateFrameEndFromStart(this.#calculateFrameStart(value, range), range);
    }

    #generateSqlTableColumns = (completeSql: string, schema: definition): string => {
        // let schema = {
        //     "name": "value",
        //     "datatype": "bigint|integer|float|json|byte[]",
        //     "filterable": { "sorted": "asc | desc" },
        //     "primary":true,
        //     "key": {
        //         "range":10
        //     }
        // } "TagId" INTEGER NOT NULL,
        if (this.datatypes.has(schema.datatype)) {
            completeSql += pgp.as.format(" $[name:alias] $[datatype:raw],", { "name": schema.name, "datatype": this.datatypes.get(schema.datatype) });
        }
        else {
            throw new Error(`${schema.datatype} is not supported data type.`);
        }
        return completeSql;
    }

    #generatePrimaryKeyConstraintColumns = (completeSql: string, schema: definition): string => {

        if (schema.primary === true) {
            completeSql += pgp.as.format("$[name:alias],", schema);
        }
        return completeSql;
    }

    #generateSqlIndexColumns = (completeSql: string, schema: definition) => {

        if (schema.filterable != undefined) {
            completeSql += pgp.as.format(" $[name:alias] $[sort:raw],", { "name": schema.name, "sort": schema.filterable.sorted === 'asc' ? "ASC" : "DESC" });
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