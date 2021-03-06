const assert = require('assert');
const sinon = require('sinon');
const pg = require('pg-promise');
const targetType = require('../dist/index').default;
let _target = {};
let _dbReaderObject = { "none": sinon.fake(), "any": sinon.fake() }, _dbWriterObject = { "none": sinon.fake(), "any": sinon.fake() };
let _staticSampleType = [{
    "name": "time",
    "datatype": "bigint",
    "filterable": { "sorted": "desc" },
    "primary": true,
    "key": {
        "range": 1000
    }
},
{
    "name": "tagid",
    "datatype": "integer",
    "primary": true
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


describe('PartionPg Unit Tests', function () {

    this.beforeEach(async function () {
        let tableName = "Raw", schemaName = "Anukram";
        _target = new targetType(_dbReaderObject, _dbWriterObject, schemaName, tableName,_staticSampleType);
    });

    this.afterEach(async function () {
        sinon.reset();
    });

    it('should initialize correctly with correct columns correct names and defualt operators', async function () {
        let tableName = "Raw", schemaName = "Anukram";
        let supportedOperators = ["=", "IN"];
        let target = new targetType("", "", schemaName, tableName,_staticSampleType);
        assert.deepEqual(target.tableName, tableName);
        assert.deepEqual(target.schemaName, schemaName);
        assert.deepEqual(target.columnsNames, ["time", "tagid", "value", "quality"], "Column names are not equal.");
        assert.deepEqual(Array.from(target.filterOperators.keys()), supportedOperators);
    });

    it('should execute correct sql when defining a table', async function () {

        let expectedSql = `CREATE FUNCTION "Anukram"."auto_part_Raw" (IN name TEXT) RETURNS VOID
        LANGUAGE 'plpgsql'
        AS $$
        DECLARE
        table_name TEXT := 'Raw' || '_' || name;
        index_name TEXT := table_name ||'_idx';
        primarykey_name TEXT := table_name ||'_pk';
        dsql TEXT;
        BEGIN
        dsql:= 'SELECT pg_advisory_lock(hashtext($1)); ';
        dsql:= dsql ||'CREATE TABLE IF NOT EXISTS "Anukram".'|| quote_ident(table_name) || '( time bigint, tagid integer, value double precision, quality integer ,CONSTRAINT '|| quote_ident(primarykey_name)||' PRIMARY KEY (time,tagid)); ';
        dsql:= dsql ||'CREATE INDEX IF NOT EXISTS '|| quote_ident(index_name) ||' ON "Anukram".' || quote_ident(table_name) || ' ( time DESC, quality ASC);';
        EXECUTE dsql USING table_name;
        END$$;`;

        await _target.create();
        assert.deepEqual(_dbReaderObject.none.notCalled, true, "Reader connection should not be used when writting");
        assert.deepEqual(_dbWriterObject.none.calledOnce, true);
        assert.deepEqual(_target.columnsNames, ["time", "tagid", "value", "quality"], "Column names are not equal.");
        assert.deepEqual(_dbWriterObject.none.firstCall.args[0], expectedSql);
    });

    it('should not execute any sql when loading a table', async function () {

        assert.deepEqual(_dbReaderObject.none.notCalled, true);
        assert.deepEqual(_dbWriterObject.none.notCalled, true);
        assert.deepEqual(_target.columnsNames, ["time", "tagid", "value", "quality"], "Column names are not equal.");
    });

    it('should execute correct sql when bulk inserting a table within a partition', async function () {

        let expectedSql = `SELECT "Anukram"."auto_part_Raw"('0_999');
        INSERT INTO "Anukram"."Raw_0_999" ("time","tagid","value","quality") VALUES (0,1,1.5,1),(999,2,2.5,2)
        ON CONFLICT ON CONSTRAINT "Raw_0_999_pk"
        DO UPDATE SET  "time"=EXCLUDED."time", "tagid"=EXCLUDED."tagid", "value"=EXCLUDED."value", "quality"=EXCLUDED."quality";`;

        let insertpayload = [
            [0, 1, 1.5, 1],
            [999, 2, 2.5, 2],
        ]

        await _target.upsert(insertpayload);
        assert.deepEqual(_dbReaderObject.none.notCalled, true, "Reader connection should not be used when writting");
        assert.deepEqual(_dbWriterObject.none.calledOnce, true);
        assert.deepEqual(_dbWriterObject.none.firstCall.args[0], expectedSql);
    });

    it('should execute correct sql when bulk inserting a table across a partition', async function () {

        let expectedSqlFirstCall = `SELECT "Anukram"."auto_part_Raw"('0_999');
        INSERT INTO "Anukram"."Raw_0_999" ("time","tagid","value","quality") VALUES (0,1,1.5,1)
        ON CONFLICT ON CONSTRAINT "Raw_0_999_pk"
        DO UPDATE SET  "time"=EXCLUDED."time", "tagid"=EXCLUDED."tagid", "value"=EXCLUDED."value", "quality"=EXCLUDED."quality";`;
        let expectedSqlSecondCall = `SELECT "Anukram"."auto_part_Raw"('1000_1999');
        INSERT INTO "Anukram"."Raw_1000_1999" ("time","tagid","value","quality") VALUES (1000,2,2.5,2)
        ON CONFLICT ON CONSTRAINT "Raw_1000_1999_pk"
        DO UPDATE SET  "time"=EXCLUDED."time", "tagid"=EXCLUDED."tagid", "value"=EXCLUDED."value", "quality"=EXCLUDED."quality";`;
        let insertpayload = [
            [0, 1, 1.5, 1],
            [1000, 2, 2.5, 2],
        ]

        await _target.upsert(insertpayload);
        assert.deepEqual(_dbReaderObject.none.notCalled, true, "Reader connection should not be used when writting");
        assert.deepEqual(_dbWriterObject.none.callCount, 2);
        assert.deepEqual(_dbWriterObject.none.firstCall.args[0], expectedSqlFirstCall);
        assert.deepEqual(_dbWriterObject.none.secondCall.args[0], expectedSqlSecondCall);
    });

    it('should execute the correct sql when no filters, no selective columns and within range value is passed for readRange', async function () {
        let expectedSql = `
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  ;`;

        _dbReaderObject.any = sinon.fake.returns(['a']);

        let result = await _target.readRange(0, 998);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql);
        assert.deepEqual(['a'], result);
    });

    it('should return empty result when table not exists exception is thrown', async function () {
        let expectedSql = `
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  ;`;
        let table404 = new Error("Faked Table not found error");

        table404.code = "42P01";
        _dbReaderObject.any = sinon.fake.rejects(table404);

        let result = await _target.readRange(0, 998);

        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql);
        assert.deepEqual([], result);
    });

    it('should execute the correct sql when no filters, no selective columns and outside range value is passed for readRange', async function () {
        let expectedSql = [`
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
             ;`,
            `
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_1000_1999"
            WHERE "time" BETWEEN 0 AND 1001  ;`];

        _dbReaderObject.any = sinon.fake(sql => {
            if (sql === expectedSql[0]) {
                return ['a'];
            }
            else if (sql === expectedSql[1]) {
                return ['b'];
            }
            else {
                throw new Error("Not expected Sql: " + sql);
            }
        });

        let result = await _target.readRange(0, 1001);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.callCount, 2);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql[0]);
        assert.deepEqual(_dbReaderObject.any.secondCall.args[0], expectedSql[1]);
        assert.deepEqual(['a', 'b'], result);
    });

    it('should execute the correct sql with "equal-to" and "in" filters combined, no selective columns and within range value is passed for readRange', async function () {
        let expectedSql = [`
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  AND  "quality" = 1 and "tagid" IN VALUES ("2","3","4") and "value" = 2 ;`];

        _dbReaderObject.any = sinon.fake(sql => {
            if (sql === expectedSql[0]) {
                return ['a'];
            }
            else {
                throw new Error("Not expected Sql: " + sql);
            }
        });

        let filters = [
            {
                "name": "quality",
                "operator": "=",
                "values": [1]
            },
            {
                "name": "tagid",
                "operator": "IN",
                "values": [2, 3, 4],
                "combine": {
                    "condition-index": 0,
                    "using": "and"
                }
            },
            {
                "name": "value",
                "operator": "=",
                "values": [2],
                "combine": {
                    "condition-index": 1,
                    "using": "and"
                }
            }
        ];
        let result = await _target.readRange(0, 998, [], filters);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql[0]);
        assert.deepEqual(['a'], result);
    });

    it('should execute the correct sql with "in" filter, no selective columns and within range value is passed for readRange', async function () {
        let expectedSql = [`
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  AND  "tagid" IN VALUES ("2","3","4") ;`];

        _dbReaderObject.any = sinon.fake(sql => {
            if (sql === expectedSql[0]) {
                return ['a'];
            }
            else {
                throw new Error("Not expected Sql: " + sql);
            }
        });

        let filters = [
            {
                "name": "tagid",
                "operator": "IN",
                "values": [2, 3, 4]
            }
        ];
        let result = await _target.readRange(0, 998, [], filters);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql[0]);
        assert.deepEqual(['a'], result);
    });

    it('should execute the correct sql with "equal-to" filter, no selective columns and within range value is passed for readRange', async function () {
        let expectedSql = [`
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  AND  "quality" = 1 ;`];

        _dbReaderObject.any = sinon.fake(sql => {
            if (sql === expectedSql[0]) {
                return ['a'];
            }
            else {
                throw new Error("Not expected Sql: " + sql);
            }
        });

        let filters = [
            {
                "name": "quality",
                "operator": "=",
                "values": [1]
            }
        ];
        let result = await _target.readRange(0, 998, [], filters);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql[0]);
        assert.deepEqual(['a'], result);
    });

    it('should execute the correct sql with "equal-to" and "in" filters combined with selective columns and within range values is passed for readRange', async function () {
        let expectedSql = [`
            SELECT "tagid" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  AND  "quality" = 1 and "tagid" IN VALUES ("2","3","4") ;`];

        let filters = [
            {
                "name": "quality",
                "operator": "=",
                "values": [1]
            },
            {
                "name": "tagid",
                "operator": "IN",
                "values": [2, 3, 4],
                "combine": {
                    "condition-index": 0,
                    "using": "and"
                }
            }
        ];

        _dbReaderObject.any = sinon.fake(sql => {
            if (sql === expectedSql[0]) {
                return ['a'];
            }
            else {
                throw new Error("Not expected Sql: " + sql);
            }
        });

        let selectiveColumns = [
            _target.columnsNames.findIndex(c => c === "tagid")
        ];
        let result = await _target.readRange(0, 998, selectiveColumns, filters);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql[0]);
        assert.deepEqual(['a'], result);
    });

    it('should execute the correct sql with "equal-to" and "in" filters combined with selective columns, outside range values for readRange', async function () {
        let expectedSql = [`
            SELECT "tagid" 
            FROM "Anukram"."Raw_0_999"
            WHERE  "quality" = 1 and "tagid" IN VALUES ("2","3","4")  ;`, `
            SELECT "tagid" 
            FROM "Anukram"."Raw_1000_1999"
            WHERE "time" BETWEEN 0 AND 1004  AND  "quality" = 1 and "tagid" IN VALUES ("2","3","4") ;`];

        let filters = [
            {
                "name": "quality",
                "operator": "=",
                "values": [1]
            },
            {
                "name": "tagid",
                "operator": "IN",
                "values": [2, 3, 4],
                "combine": {
                    "condition-index": 0,
                    "using": "and"
                }
            }
        ];

        _dbReaderObject.any = sinon.fake(sql => {
            if (sql === expectedSql[0]) {
                return ['a'];
            }
            else if (sql === expectedSql[1]) {
                return ['b'];
            }
            else {
                throw new Error("Not expected Sql: " + sql);
            }
        });

        let selectiveColumns = [
            _target.columnsNames.findIndex(c => c === "tagid")
        ];
        let result = await _target.readRange(0, 1004, selectiveColumns, filters);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.callCount, 2);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql[0]);
        assert.deepEqual(_dbReaderObject.any.secondCall.args[0], expectedSql[1]);
        assert.deepEqual(['a', 'b'], result);
    });

    it('should return empty result set when no records are found for readRange', async function () {
        let expectedSql = [`
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  AND  "quality" = 1 ;`];

        _dbReaderObject.any = sinon.fake(sql => {
            if (sql === expectedSql[0]) {
                return [];
            }
            else {
                throw new Error("Not expected Sql: " + sql);
            }
        });

        let filters = [
            {
                "name": "quality",
                "operator": "=",
                "values": [1]
            }
        ];
        let result = await _target.readRange(0, 998, [], filters);
        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql[0]);
        assert.deepEqual([], result);
    });

    it('should throw the exception any other than table not found', async function () {
        let expectedSql = `
            SELECT "time","tagid","value","quality" 
            FROM "Anukram"."Raw_0_999"
            WHERE "time" BETWEEN 0 AND 998  ;`;

        let unknownError = new Error("Faked unknown error");
        _dbReaderObject.any = sinon.fake.rejects(unknownError);

        let result;
        try {
            result = await _target.readRange(0, 998);
        }
        catch (exception) {
            if (exception.message != unknownError.message) {
                assert.fail("Unaccepted Exception: " + exception.message);
            }
        }

        assert.deepEqual(_dbWriterObject.any.notCalled, true);
        assert.deepEqual(_dbReaderObject.any.calledOnce, true);
        assert.deepEqual(_dbReaderObject.any.firstCall.args[0], expectedSql);
        assert.deepEqual(undefined, result);
    });
});