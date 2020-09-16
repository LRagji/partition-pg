
const redisType = require("ioredis");
const scripto = require('redis-scripto');
const path = require('path');
const pgp = require('pg-promise')();
const inventory_key = "Inventory";
const default_schema = "public";
pgp.pg.types.setTypeParser(20, BigInt); // This is for serialization bug of BigInts as strings.
const InfinityStampTag = "InfStamp";
const InfinityIdTag = "InfId";
const PrimaryTag = "primary";
module.exports = class InfinityTableFactory {
    #pgReadConfigParams
    #pgWriteConfigParams
    #indexerRedisConnectionString
    #redisClient
    #scriptingEngine
    #configDBWriter
    #configDBReader

    constructor(indexerRedisConnectionString, pgReadConfigParams, pgWriteConfigParams) {
        this.#pgReadConfigParams = pgReadConfigParams;
        this.#pgWriteConfigParams = pgWriteConfigParams;
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

        const infinityIdColumn = {
            "name": "InfId",
            "datatype": "bigint",
            "filterable": { "sorted": "asc" },
            "tag": InfinityIdTag
        };
        const infinityStampColumn = {
            "name": "InfStamp",
            "datatype": "bigint",
            "filterable": { "sorted": "asc" },
            "tag": InfinityStampTag
        };
        let userDefinedPK = false;
        let userDefinedPKDatatype;
        const totalPrimaryColumns = tableDefinition.reduce((acc, e) => acc + (e.tag === PrimaryTag ? 1 : 0), 0);
        if (totalPrimaryColumns > 1) throw new Error("Table cannot have multiple primary columns");
        if (totalPrimaryColumns === 0) tableDefinition.unshift(infinityIdColumn);
        if (totalPrimaryColumns === 1) {
            console.warn("It is recommended to use system generated infinity id for better scalling and performance.");
            userDefinedPK = true;
            userDefinedPKDatatype = tableDefinition.find(e => e.tag === PrimaryTag)["datatype"];
        }
        const totalInfinityStamoColumns = tableDefinition.reduce((acc, e) => acc + (e.tag === InfinityStampTag ? 1 : 0), 0);
        if (totalInfinityStamoColumns > 1) throw new Error("Table cannot have multiple InfitiyStamp columns");
        if (totalInfinityStamoColumns === 0) tableDefinition.push(infinityStampColumn);
        if (totalInfinityStamoColumns == 1) {
            const infinityStampColumn = tableDefinition.find(e => e.tag === InfinityStampTag);
            if (infinityStampColumn.datatype !== "bigint") throw new Error("InfitiyStamp columns should have datatype as bigint.");
        }

        const tableId = await this.#configDBWriter.tx(async trans => {

            let tIdentifier = await trans.one('INSERT INTO "Types" ("Def") values ($1) RETURNING "Id";', [tableDefinition]);
            if (userDefinedPK) {
                await trans.none(`CREATE TABLE public."${tIdentifier.Id}-PK"
                (
                    "UserPK" ${userDefinedPKDatatype} NOT NULL,
                    "CInfID" text NOT NULL,
                    PRIMARY KEY ("UserPK")
                );`); //THIS table should be partitioned with HASH for 20CR rows
            }
            await trans.none(`CREATE TABLE public."${tIdentifier.Id}-Min"
            (
                "InfStamp" bigint NOT NULL,
                "PInfID" text NOT NULL,
                CONSTRAINT "${tIdentifier.Id}-Min-PK" PRIMARY KEY ("PInfID")
            );`);
            await trans.none(`CREATE TABLE public."${tIdentifier.Id}-Max"
            (
                "InfStamp" bigint NOT NULL,
                "PInfID" text NOT NULL,
                CONSTRAINT "${tIdentifier.Id}-Max-PK" PRIMARY KEY ("PInfID")
            );`); //THIS table should be partitioned with HASH for 20CR rows
            return tIdentifier.Id;
        });

        return new InfinityTable(this.#pgReadConfigParams, this.#pgWriteConfigParams, this.#indexerRedisConnectionString, tableId, tableDefinition);
    }

    async loadTable(TableIdentifier) {
        let tableDef = await this.#configDBReader.one('SELECT "Def" FROM "Types" WHERE "Id"=$1', [TableIdentifier]);
        if (tableDef == undefined) {
            throw new Error(`Table with id does ${TableIdentifier} not exists.`);
        }
        tableDef = tableDef.Def.map(e => JSON.parse(e));
        return new InfinityTable(this.#pgReadConfigParams, this.#pgWriteConfigParams, this.#indexerRedisConnectionString, TableIdentifier, tableDef);
    }
}

class InfinityTable {
    #configDBReader
    #configDBWriter
    #def
    #connectionMap
    #redisClient
    #scriptingEngine
    #columnsNames

    constructor(configReaderConnectionParams, configWriterConnectionParams, indexerRedisConnectionString, systemTableIdentifier, tableDefinition) {
        this.#configDBReader = pgp(configReaderConnectionParams);
        this.#configDBWriter = pgp(configWriterConnectionParams);
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
        this.#indexPrimarykey = this.#indexPrimarykey.bind(this);
        this.#indexInfinityStampMin = this.#indexInfinityStampMin.bind(this);
        this.#indexInfinityStampMax = this.#indexInfinityStampMax.bind(this);

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
        if (schema.tag === InfinityIdTag || schema.tag === PrimaryTag) {
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

        let valuesString = payload.reduce((valuesString, element) => valuesString += `(${element.Values.join(",")}),`, "");
        let columns = this.#columnsNames.reduce((acc, e) => acc + `${pgp.as.format('$1:alias', [e])},`, "");
        columns = columns.slice(0, -1);
        valuesString = valuesString.slice(0, -1)
        return `INSERT INTO "${default_schema}"."${tableName}" (${columns}) VALUES ${valuesString} RETURNING *`;
    }

    #indexPrimarykey = (tableName, payload) => {

        let valuesString = payload.reduce((valuesString, element) => valuesString += pgp.as.format("($1,$2),", [element.UserPk, element.InfinityRowId]), "");
        valuesString = valuesString.slice(0, -1)
        return `INSERT INTO "${default_schema}"."${tableName}" ("UserPK","CInfID") VALUES ${valuesString};`;
    }

    #indexInfinityStampMin = (tableName, payload, PInfID) => {

        let min = payload.reduce((min, element) => Math.min(min, element.InfinityStamp), Number.MAX_VALUE);
        return `INSERT INTO "${default_schema}"."${tableName}" ("InfStamp","PInfID") VALUES (${min},'${PInfID}') ON CONFLICT ON CONSTRAINT "${tableName + "-PK"}"
        DO UPDATE SET "InfStamp" = LEAST(EXCLUDED."InfStamp","${tableName}"."InfStamp")`;
    }

    #indexInfinityStampMax = (tableName, payload, PInfID) => {

        let max = payload.reduce((max, element) => Math.max(max, element.InfinityStamp), Number.MIN_VALUE);
        return `INSERT INTO "${default_schema}"."${tableName}" ("InfStamp","PInfID") VALUES (${max},'${PInfID}') ON CONFLICT ON CONSTRAINT "${tableName + "-PK"}"
        DO UPDATE SET "InfStamp" = GREATEST(EXCLUDED."InfStamp","${tableName}"."InfStamp")`;
    }

    async bulkInsert(payload) {
        //This code has run away complexity dont trip on it ;)
        let userDefinedPk = false;
        if (payload.length > 10000) throw new Error("Currently ingestion rate of 10K/sec is only supported!");

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
            let completeRowId = `${this.TableIdentifier}-${dbId}-${tableId}-${scopedRowId}`;
            lastChange[3]++;
            let item = { "InfinityRowId": completeRowId, "Values": [], "InfinityStamp": Date.now() };

            this.#def.forEach(columnDef => {
                let colValue = value[columnDef.name];
                if (colValue == undefined) {
                    if (columnDef.tag === PrimaryTag) throw new Error("Primary field cannot be null:" + columnDef.name);//This should be done before to save identities
                    if (columnDef.tag === InfinityIdTag) colValue = scopedRowId;
                    if (columnDef.tag === InfinityStampTag) colValue = item.InfinityStamp;
                }
                else {
                    if (columnDef.tag === PrimaryTag) {
                        item["UserPk"] = colValue;
                        userDefinedPk = true;
                    }
                    if (columnDef.tag === InfinityStampTag) item.InfinityStamp = colValue;
                }
                item.Values.push(colValue);
            });

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
                    const insertedRows = await DBWritter.tx(async instanceTrans => {

                        await this.#configDBWriter.tx(async indexTran => {
                            if (userDefinedPk) {
                                let sql = this.#indexPrimarykey((this.TableIdentifier + "-PK"), items);
                                await indexTran.none(sql);
                            }
                            let sql = this.#indexInfinityStampMin((this.TableIdentifier + "-Min"), items, tableName);
                            await indexTran.none(sql);
                            sql = this.#indexInfinityStampMax((this.TableIdentifier + "-Max"), items, tableName);
                            await indexTran.none(sql);
                        });

                        let sql = this.#sqlTransform(tableName, items);
                        return instanceTrans.many(sql);
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