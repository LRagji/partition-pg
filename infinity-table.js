
const redisType = require("ioredis");
const scripto = require('redis-scripto');
const path = require('path');
const pgp = require('pg-promise')();
const inventory_key = "Inventory";
const default_schema = "public";
pgp.pg.types.setTypeParser(20, BigInt); // This is for serialization bug of BigInts as strings.

module.exports = class InfinityTableFactory {
    #pgReadConfigParams
    #indexerRedisConnectionString
    #redisClient
    #scriptingEngine
    #configDBWriter
    #configDBReader

    constructor(indexerRedisConnectionString, pgReadConfigParams, pgWriteConfigParams) {
        this.#pgReadConfigParams = pgReadConfigParams;
        this.#indexerRedisConnectionString = indexerRedisConnectionString;
        this.#redisClient = new redisType(indexerRedisConnectionString);
        this.#scriptingEngine = new scripto(this.#redisClient);
        this.#scriptingEngine.loadFromDir(path.resolve(path.dirname(__filename), 'lua'));
        this.#configDBWriter = pgp(pgWriteConfigParams);
        this.#configDBReader = pgp(pgReadConfigParams);

        this.registerResource = this.registerResource.bind(this);
        this.createTable = this.createTable.bind(this);
        this.loadTable = this.loadTable.bind(this);

        this.codeRed = () => {
            this.#redisClient.disconnect();
            pgp.end();
        };//Not be exposed for PROD
    }

    async registerResource(readerConnectionParams, writerConnectionParams, maxTables, maxRowsPerTable) {
        return await this.#configDBWriter.tx(async (trans) => {
            // creating a sequence of transaction queries:
            let rIdentifier = await trans.one('INSERT INTO "Resources" ("Read","Write","MaxTables","MaxRows") values ($1,$2,$3,$4) RETURNING "Id";', [JSON.stringify(readerConnectionParams), JSON.stringify(writerConnectionParams), maxTables, maxRowsPerTable]);

            let redisRegister = (rId, countOfTables, rowsPerTable) => new Promise((resolve, reject) => this.#scriptingEngine.run('load-resources', [inventory_key], [rId, countOfTables, rowsPerTable], function (err, result) {
                if (err != undefined) {
                    reject(err);
                    return;
                }
                resolve(result)
            }));

            await redisRegister(rIdentifier.Id, maxTables, maxRowsPerTable);
            return rIdentifier.Id;
        });
    }

    async createTable(tableDefinition) {

        return await this.#configDBWriter.tx(async trans => {

            let defaultSystemIdColumn = {
                "name": "InfId",
                "datatype": "bigint",
                "filterable": { "sorted": "asc" },
                "primary": true
            };

            tableDefinition.unshift(defaultSystemIdColumn)

            let tIdentifier = await trans.one('INSERT INTO "Types" ("Def") values ($1) RETURNING "Id";', [tableDefinition]);

            return new InfinityTable(this.#pgReadConfigParams, this.#indexerRedisConnectionString, tIdentifier.Id, tableDefinition);
        });
    }

    async loadTable(TableIdentifier) {
        let tableDef = await this.#configDBReader.one('SELECT "Def" FROM "Types" WHERE "Id"=$1', [TableIdentifier]);
        if (tableDef == undefined) {
            throw new Error(`Table with id does ${TableIdentifier} not exists.`);
        }
        tableDef = tableDef.Def.map(e => JSON.parse(e));
        return new InfinityTable(this.#pgReadConfigParams, this.#indexerRedisConnectionString, TableIdentifier, tableDef);
    }
}

class InfinityTable {
    #configDBReader
    #def
    #connectionMap
    #redisClient
    #scriptingEngine
    #columnsNames

    constructor(configReaderConnectionParams, indexerRedisConnectionString, systemTableIdentifier, tableDefinition) {
        this.#configDBReader = pgp(configReaderConnectionParams);
        this.TableIdentifier = systemTableIdentifier;
        this.#def = tableDefinition;
        this.#connectionMap = new Map();
        this.#redisClient = new redisType(indexerRedisConnectionString);
        this.#scriptingEngine = new scripto(this.#redisClient);
        this.#scriptingEngine.loadFromDir(path.resolve(path.dirname(__filename), 'lua'));

        this.datatypes = new Map();
        this.datatypes.set("bigint", "bigint");
        this.datatypes.set("integer", "integer");
        this.datatypes.set("text", "text");
        this.datatypes.set("double", "double precision");
        this.#columnsNames = this.#def.map(e => e.name);

        this.#generateIdentity = this.#generateIdentity.bind(this);
        this.#teraformTableSpace = this.#teraformTableSpace.bind(this);
        this.#sqlTransform = this.#sqlTransform.bind(this);
        this.#generateSqlTableColumns = this.#generateSqlTableColumns.bind(this);
        this.#generateSqlIndexColumns = this.#generateSqlIndexColumns.bind(this);
        this.#generatePrimaryKeyConstraintColumns = this.#generatePrimaryKeyConstraintColumns.bind(this);

        this.bulkInsert = this.bulkInsert.bind(this);

        //Not be exposed for PROD
        this.codeRed = () => {
            this.#redisClient.disconnect();
            pgp.end();
        };
    }

    #generateIdentity = (range) => {
        return new Promise((resolve, reject) => this.#scriptingEngine.run('identity', [this.TableIdentifier, inventory_key], [range], function (err, result) {
            if (err != undefined) {
                reject(err);
                return;
            }
            if (result[0] == -1) { //Partial failures will be forced into the last table its better than to fail the call.
                reject(new Error("Error: (" + result[0] + ") => " + result[1]))
            }
            result.splice(0, 1);
            resolve(result)
        }))
    }

    #generateSqlTableColumns = (completeSql, schema) => {
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

    #generateSqlIndexColumns = (completeSql, schema) => {

        if (schema.filterable != undefined) {
            completeSql += pgp.as.format(" $[name:alias] $[sort:raw],", { "name": schema.name, "sort": schema.filterable.sorted === 'asc' ? "ASC" : "DESC" });
        }
        return completeSql;
    }

    #generatePrimaryKeyConstraintColumns = (completeSql, schema) => {
        if (schema.primary === true) {
            completeSql += pgp.as.format("$[name:alias],", schema);
        }
        return completeSql;
    }

    #teraformTableSpace = async (databaseId, tableId) => {

        let currentWriterConnection = undefined;
        if (this.#connectionMap.has(databaseId) == false) {
            let conDetails = await this.#configDBReader.one('SELECT "Read","Write" FROM "Resources" WHERE "Id"=$1', [databaseId]);
            let tableDBWriter = pgp(JSON.parse(conDetails["Write"]));
            let tableDBReader = pgp(JSON.parse(conDetails["Read"]));
            this.#connectionMap.set(databaseId, { "W": tableDBWriter, "R": tableDBReader });
            currentWriterConnection = tableDBWriter;
        }
        else {
            currentWriterConnection = this.#connectionMap.get(databaseId)["W"];
        }

        let tableColumns = this.#def.reduce(this.#generateSqlTableColumns, "");
        let indexColumns = this.#def.reduce(this.#generateSqlIndexColumns, "");
        let primaryKeyColumns = this.#def.reduce(this.#generatePrimaryKeyConstraintColumns, "");
        primaryKeyColumns = primaryKeyColumns.slice(0, -1);
        tableColumns = tableColumns.slice(0, -1);
        indexColumns = indexColumns.slice(0, -1);

        let functionSql = `CREATE OR REPLACE FUNCTION $[schema_name:name].$[function_name:name] (IN name TEXT) RETURNS VOID
        LANGUAGE 'plpgsql'
        AS $$
        DECLARE
        table_name TEXT := $[table_name] || '-' || name;
        index_name TEXT := table_name ||'_idx';
        primarykey_name TEXT := table_name ||'_pk';
        dsql TEXT;
        BEGIN
        dsql:= 'SELECT pg_advisory_lock(hashtext($1)); ';
        dsql:= dsql ||'CREATE TABLE IF NOT EXISTS $[schema_name:name].'|| quote_ident(table_name) || '($[columns:raw] ,CONSTRAINT '|| quote_ident(primarykey_name)||' PRIMARY KEY ($[primaryKeyColumns:raw])); ';
        dsql:= dsql ||'CREATE INDEX IF NOT EXISTS '|| quote_ident(index_name) ||' ON $[schema_name:name].' || quote_ident(table_name) || ' ($[indexColumns:raw]);';
        EXECUTE dsql USING table_name;
        END$$;`;
        await currentWriterConnection.none(pgp.as.format(functionSql, {
            "schema_name": default_schema,
            "function_name": ("infinity_part_" + this.TableIdentifier),
            "table_name": this.TableIdentifier + "-" + databaseId,
            "columns": tableColumns,
            "primaryKeyColumns": primaryKeyColumns,
            "indexColumns": indexColumns
        }));

        await currentWriterConnection.one(`SELECT "${default_schema}"."infinity_part_${this.TableIdentifier}"('${tableId}')`);
        return `${this.TableIdentifier}-${databaseId}-${tableId}`;
    }

    #sqlTransform = (tableName, payload) => {

        let valuesString = payload.reduce((valuesString, element) => valuesString += `(${element.V.join(",")}),`, "");
        let columns = this.#columnsNames.reduce((acc, e) => acc + `${pgp.as.format('$1:alias', [e])},`, "");
        columns = columns.slice(0, -1);
        valuesString = valuesString.slice(0, -1)
        return `INSERT INTO "${default_schema}"."${tableName}" (${columns}) VALUES ${valuesString} RETURNING *`;
    }

    async bulkInsert(payload) {
        console.time("Identity");
        let identities = await this.#generateIdentity(payload.length);
        console.timeEnd("Identity");

        console.time("Transform");
        let lastChange = null;
        let groupedPayloads = payload.reduceRight((groups, value, idx) => {
            idx = idx + 1;
            let changeIdx = identities.findIndex(e => e[0] == idx);
            if (changeIdx == -1 && lastChange == null) throw new Error("Did not find start index");
            if (changeIdx != -1) {
                lastChange = identities.splice(changeIdx, 1)[0];
            }
            let dbId = lastChange[1];
            let tableId = lastChange[2];
            let scopedRowId = lastChange[3];
            let rowId = `${this.TableIdentifier}-${dbId}-${tableId}-${scopedRowId}`;
            lastChange[3]++;
            value.unshift(scopedRowId);
            let item = { "Id": rowId, "V": value };
            let dbgroup = groups.get(dbId);
            if (dbgroup == undefined) {
                let temp = new Map();
                temp.set(tableId, [item]);
                groups.set(dbId, temp);
            }
            else {
                let tablegroup = dbgroup.get(tableId);
                if (tablegroup == undefined) {
                    dbgroup.set(tableId, [item]);
                }
                else {
                    tablegroup.push(item);
                }
            }
            return groups;
        }, new Map());
        console.timeEnd("Transform");

        console.time("PG");
        let results = { "failures": [], "success": [] };
        let DBIds = Array.from(groupedPayloads.keys());
        for (let dbIdx = 0; dbIdx < DBIds.length; dbIdx++) {
            const dbId = DBIds[dbIdx];
            const tables = groupedPayloads.get(dbId);
            const tableIds = Array.from(tables.keys());
            for (let tableIdx = 0; tableIdx < tableIds.length; tableIdx++) {
                const tableId = tableIds[tableIdx];
                const items = tables.get(tableId);
                try {
                    const tableName = await this.#teraformTableSpace(dbId, tableId);
                    const DBWritter = this.#connectionMap.get(dbId)["W"];
                    const insertedRows = await DBWritter.tx(async trans => {
                        let sql = this.#sqlTransform(tableName, items);
                        return trans.many(sql);
                    });
                    results.success.push(insertedRows.map(e => {
                        e.InfId = tableName + "-" + e.InfId;
                        return e;
                    }));
                }
                catch (err) {
                    results.failures.push({ "Error": err, "Items": items });
                    continue;
                    //TODO: Reclaim Lost Ids
                }
            }
        }
        console.timeEnd("PG");
        return results;
    }

    query() {

    }
    bulkUpdate() {

    }
    bulkDelete() {

    }
}