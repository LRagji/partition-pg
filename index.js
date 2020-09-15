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

        this.datatypes = new Map();
        this.datatypes.set("bigint", "bigint");
        this.datatypes.set("integer", "integer");
        this.datatypes.set("text", "text");
        this.datatypes.set("double", "double precision");

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
        this.columnsNames = definition.map(e => pgp.as.format('$1:alias', [e.name]));
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
        dsql:= dsql ||'CREATE TABLE IF NOT EXISTS $[schema_name:name].'|| quote_ident(table_name) || '($[columns:raw] ,CONSTRAINT '|| quote_ident(primarykey_name)||' PRIMARY KEY ($[primaryKeyColumns:raw])); ';
        dsql:= dsql ||'CREATE INDEX IF NOT EXISTS '|| quote_ident(index_name) ||' ON $[schema_name:name].' || quote_ident(table_name) || ' ($[indexColumns:raw]);';
        EXECUTE dsql USING table_name;
        END$$;`;
        await this._dbWriter.none(pgp.as.format(unsafeSql, {
            "schema_name": this.schemaName,
            "function_name": ("auto_part_" + this.tableName),
            "table_name": this.tableName,
            "columns": tableColumns,
            "primaryKeyColumns": primaryKeyColumns,
            "indexColumns": indexColumns
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
        if (this.datatypes.has(schema.datatype)) {
            completeSql += pgp.as.format(" $[name:alias] $[datatype:raw],", { "name": schema.name, "datatype": this.datatypes.get(schema.datatype) });
        }
        else {
            throw new Error(`${schema.datatype} is not supported data type.`);
        }
        return completeSql;
    }

    _generatePrimaryKeyConstraintColumns(completeSql, schema) {

        if (schema.primary === true) {
            completeSql += pgp.as.format("$[name:alias],", schema);
        }
        return completeSql;
    }

    _generateSqlIndexColumns(completeSql, schema) {

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
//Min TS, Min Value, Min Quality, BloomPayload, TS-Histogram, Value-Histogram, Quality-Histogram,Max TS, Max Value, Max Quality, CRC.

// let BloomFilter = require('bloomfilter').BloomFilter;
// let createCountMinSketch = require("count-min-sketch");
// let histTS = createCountMinSketch(0.00001,0.00001), histValue = createCountMinSketch(0.00001,0.00001), histQuality = createCountMinSketch(0.00001,0.00001);
// let length = 200;
// let bloom = new BloomFilter(
//     length*2, // number of bits to allocate.
//     16*2        // number of hash functions.
// );
// let inputArray = []
// let timestampMin = 1598140800021;
// let timestampMax = timestampMin + length
// let maxTS = Number.MIN_VALUE, maxValue = Number.MIN_VALUE, maxQuality = Number.MIN_VALUE, minTS = Number.MAX_VALUE, minValue = Number.MAX_VALUE, minQuality = Number.MAX_VALUE;

// console.time("Fill");
// for (let index = 0; index < (length / 2); index++) {
//     let timestamp = Math.floor((Math.random() * (timestampMax - timestampMin) + timestampMin));
//     let value = Math.floor((Math.random() * (100 - 1) + 1));;
//     let quality = Math.floor((Math.random() * (10 - 1) + 1));
//     if (maxTS < timestamp) maxTS = timestamp;
//     if (minTS > timestamp) minTS = timestamp;
//     if (maxValue < value) maxValue = value;
//     if (minValue > value) minValue = value;
//     if (maxQuality < quality) maxQuality = quality;
//     if (minQuality > quality) minQuality = quality;
//     histTS.update(timestamp, 1);
//     histValue.update(value, 1);
//     histQuality.update(quality, 1);
//     let sample = `T:${timestamp}V:${value}Q:${quality}`;
//     inputArray.push(sample);
//     bloom.add(sample);
// }
// console.timeEnd("Fill");

// let outputArray = []
// console.time("Decypher");
// let allTS = new Map(), allValues = new Map(), allQualities = new Map();
// for (let timestamp = minTS; timestamp <= maxTS; timestamp++) {
//     let repeatTS = histTS.query(timestamp);
//     if (repeatTS > 0) allTS.set(timestamp, repeatTS);
// }

// for (let value = minValue; value <= maxValue; value++) {
//     let repeatValue = histValue.query(value);
//     if (repeatValue > 0) allValues.set(value, repeatValue);
// }

// for (let quality = minQuality; quality <= maxQuality; quality++) {
//     let repeatQuality = histQuality.query(quality);
//     if (repeatQuality > 0) allQualities.set(quality, repeatQuality);
// }

// do {
//     allTS.forEach((tsCount, timestamp) => {
//         allValues.forEach((valueCount, value) => {
//             allQualities.forEach((qualityCount, quality) => {
//                 let sample = `T:${timestamp}V:${value}Q:${quality}`;
//                 if (bloom.test(sample)) {
//                     outputArray.push(sample);
//                     tsCount--;
//                     valueCount--;
//                     qualityCount--;
//                     if (tsCount <= 0) allTS.delete(timestamp);
//                     if (valueCount <= 0) allValues.delete(timestamp);
//                     if (qualityCount <= 0) allQualities.delete(timestamp);
//                 }
//             });
//         });
//     });
// }
// while (allTS.size > 0 && allValues.size > 0 && allQualities.size > 0)

// console.log("D");

// // for (let timestamp = minTS; timestamp <= maxTS; timestamp++) {
// //     let repeatTS = histTS.query(timestamp);
// //     while (repeatTS > 0) {
// //         for (let value = minValue; value <= maxValue; value++) {
// //             let repeatValue = histValue.query(value);
// //             while (repeatValue > 0) {
// //                 for (let quality = minQuality; quality <= maxQuality; quality++) {
// //                     let repeatQuality = histQuality.query(quality);
// //                     //while (repeatQuality > 0) {
// //                     let sample = `T:${timestamp}V:${value}Q:${quality}`;
// //                     if (bloom.test(sample)) outputArray.push(sample);
// //                     repeatQuality--;
// //                 }
// //             }
// //             repeatValue--;
// //         }
// //     }
// //     repeatTS--;
// // }



// console.timeEnd("Decypher");

// if (outputArray.length != inputArray.length) {
//     console.log(`Failed i:${inputArray.length} o:${outputArray.length}`);
// }
// else {
//     console.log("Passed");
// }


// var Scripto = require('redis-scripto');
// const fs = require('fs');
// var path = require('path');
// const redisType = require("ioredis");
//  const defaultRedisConnectionString = "redis://127.0.0.1:6379/";
// let redisClient = new redisType(defaultRedisConnectionString);
// var scriptManager = new Scripto(redisClient);
// scriptManager.loadFromDir(path.resolve(path.dirname(__filename), 'lua'));
// const payloadType = 'Type1';
// const payloadMaxKey = payloadType + "-Max";
// const payloadMinKey = payloadType + "-Min";

// let getIdentity = (range) => new Promise((resolve, reject) => scriptManager.run('identity', [payloadType, 'Inventory'], [range], function (err, result) {
//     if (err != undefined) {
//         reject(err);
//         return;
//     }
//     resolve(result)
// }))

// let main = async () => {
//     let ctr = 400000;
//     let payload = [];
//     console.time("Payload Generation");
//     while (ctr > 0) {
//         payload.push({ "time": ctr, "AlertId": ctr })
//         ctr--;
//     }
//     console.timeEnd("Payload Generation");

//     console.time("Acquiring Identity");
//     let identity = await getIdentity(payload.length)
//     if (identity[0] == -1) { //Partial failures will be forced into the last table its better than to fail the call.
//         console.error("Error:(" + identity[0] + ") " + identity[1])
//         return;
//     }
//     identity.splice(0, 1);
//     console.timeEnd("Acquiring Identity");

//     console.time("Transforming");
//     let lastChange = null;
//     let groupedSql = payload.reduceRight((groups, value, idx) => {
//         idx = idx + 1;
//         let changeIdx = identity.findIndex(e => e[0] == idx);
//         if (changeIdx == -1 && lastChange == null) throw new Error("Did not find start index");
//         if (changeIdx != -1) {
//             let t = identity.splice(changeIdx, 1)[0];
//             lastChange = { "Element": t, "Name": `${payloadType}-${t[1]}-${t[2]}` };
//         }
//         value.Id = lastChange.Name + "-" + lastChange.Element[3];
//         lastChange.Element[3]++;
//         let group = groups.get(lastChange.Name);
//         if (group == undefined) {
//             groups.set(lastChange.Name, { "Min": value.time, "Max": value.time, "Elements": [value] });
//         } else {
//             group.Min = group.Min > value.time ? value.time : group.Min;
//             group.Max = group.Max < value.time ? value.time : group.Max;
//             group.Elements.push(value);
//         }
//         return groups;
//     }, new Map())
//     console.timeEnd("Transforming");

//     console.time("Indexing");
//     let indexer = (maxKey, minKey, max, min, tableName) => new Promise((accept, reject) => scriptManager.run('indexing', [maxKey, minKey], [max, min, tableName], function (err, result) {
//         if (err) { reject(err); return }
//         accept(result);
//     }));
//     let allPromisses = [];
//     groupedSql.forEach((def, tableName) => {
//         allPromisses.push(indexer(payloadMaxKey, payloadMinKey, def.Max, def.Min, tableName));
//     });
//     await Promise.allSettled(allPromisses);
//     console.timeEnd("Indexing");

//     console.log("Total Groups:" + groupedSql.size);

//     console.time("Query");
//     let queryTables = (maxKey, minKey, max, min) => new Promise((accept, reject) => scriptManager.run('query', [maxKey, minKey], [max, min], function (err, result) {
//         if (err) { reject(err); return }
//         accept(result);
//     }));
//     let tablesToQuery = await queryTables(payloadMaxKey, payloadMinKey, 300, 10000)
//     console.timeEnd("Query");
//     console.table(tablesToQuery);

//     //fs.appendFileSync('log.csv', `Key,Min,Max,Time`);
//     //groupedSql.forEach(logMapElements);
// }

// function logMapElements(value, key) {
//     value.Elements.forEach((v) => {
//         //fs.appendFileSync('log.csv', `${key},${value.Min},${value.Max},${v.time}`);
//         //console.log(`Table:${key} Min:${value.Min} Max:${value.Max} Time:${v.time} Id:${v.Id}`)
//     })
// }
// main().then((r) => redisClient.disconnect());


//ZADD DBS 1 "1,10000,100"
// 400000 Rows Redis 0.7MB 
// Payload Generation: 64.402ms
// Acquiring Identity: 13289.518ms
// Transforming: 1630.864ms
// Indexing: 527.946ms
// Total Groups:3961
//Query: 31.613ms

//=>Partly Optimized

// Payload Generation: 63.092ms
// Acquiring Identity: 567.414ms
// Transforming: 1832.168ms
// Indexing: 665.969ms
// Total Groups:4000
// Query: 26.358ms

// 127.0.0.1:6379> KEYS *
// 1) "CDB"
// 2) "Type1-Max"
// 3) "MR"
// 4) "Type1"
// 5) "MT"
// 6) "Type1-Min"
// 127.0.0.1:6379> Memory usage CDB
// (integer) 45
// 127.0.0.1:6379> Memory usage Type1-Max
// (integer) 356668
// 127.0.0.1:6379> Memory usage Type1-Min
// (integer) 375681
// 127.0.0.1:6379> Memory usage Type1
// (integer) 76
// 127.0.0.1:6379> Memory usage MR
// (integer) 44
// 127.0.0.1:6379> Memory usage MT
// (integer) 44

//const defaultConectionString = "postgres://postgres:mysecretpassword@localhost:5432/pgpartition?application_name=perf-test";
const defaultConectionString = "postgres://postgres:@localhost:5432/Infinity-Index";
const defaultRedisConnectionString = "redis://127.0.0.1:6379/";
const readConfigParams = {
    connectionString: defaultConectionString,
    application_name: "e2e Test",
    max: 4 //4 readers
};
const writeConfigParams = {
    connectionString: defaultConectionString,
    application_name: "e2e Test",
    max: 2 //2 Writer
};
let Table1 = [{
    "name": "time",
    "datatype": "bigint",
    "filterable": { "sorted": "desc" }
},
{
    "name": "tagid",
    "datatype": "integer"
},
{
    "name": "value",
    "datatype": "double",
},
{
    "name": "quality",
    "datatype": "integer",
    "filterable": { "sorted": "asc" },
}];
const infTableType = require('./infinity-table');

const infTableFactory = new infTableType(defaultRedisConnectionString, readConfigParams, writeConfigParams)
const readConfigParamsDB1 = {
    connectionString: "postgres://postgres:@localhost:5432/Infinity-1",
    application_name: "e2e Test",
    max: 4 //4 readers
};
const writeConfigParamsDB1 = {
    connectionString: "postgres://postgres:@localhost:5432/Infinity-1",
    application_name: "e2e Test",
    max: 2 //2 Writer
};
let TypeId = 5;
let boundlessTable;
let main = async () => {
    if (TypeId == undefined) {
        let resourceId = await infTableFactory.registerResource(readConfigParamsDB1, writeConfigParamsDB1, 1000, 100);
        console.log("Resource Id:" + resourceId);
        boundlessTable = await infTableFactory.createTable(Table1);
        TypeId = boundlessTable.TableIdentifier;
        console.log("Type Created: " + TypeId);
        return;
    }
    else {
        boundlessTable = await infTableFactory.loadTable(TypeId);
    }

    let ctr = 40000;
    let payload = [];
    console.time("Payload Generation");
    while (ctr > 0) {
        payload.push([ctr, ctr, ctr, ctr])
        ctr--;
    }
    console.timeEnd("Payload Generation");

    console.time("Insertion");
    let result = await boundlessTable.bulkInsert(payload);
    console.timeEnd("Insertion");

    //console.log(result);
    boundlessTable.codeRed();
};

main().then((r) => {
  
    infTableFactory.codeRed();
});


// CREATE TABLE public."Resources"
// (
//     "Id" bigserial,
//     "Read" text NOT NULL,
//     "Write" text NOT NULL,
//     "MaxTables" integer NOT NULL,
//     "MaxRows" integer NOT NULL,
//     PRIMARY KEY ("Id")
// );

// CREATE TABLE public."Types"
// (
//     "Id" bigserial,
//     "Def" text[] NOT NULL,
//     PRIMARY KEY ("Id")
// );

//40K
// Payload Generation: 6.198ms
// Acquiring Identity: 62.97509765625ms
// Transforming: 48.083ms
// Inserting: 3284.655ms