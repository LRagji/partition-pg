const assert = require('assert');
const targetType = require('../index');
const pgp = require('pg-promise')();
const defaultConectionString = "postgres://postgres:@localhost:5432/pgpartition?application_name=e2e";
let _target = {};
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
    "datatype": "double precision",
},
{
    "name": "quality",
    "datatype": "integer",
    "filterable": { "sorted": "asc" },
}];


describe('End to End Tests', function () {

    this.beforeEach(async function () {
        let tableName = "Raw", schemaName = "Anukram";
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
        const _dbRConnection = pgp(readConfigParams);
        let multiple = async (sqlStatements) => {
            let allExecutions = sqlStatements.reduce((promises, sql) => {
                promises.push(_dbRConnection.any(sql));
                return promises;
            }, []);
            return Promise.all(allExecutions);
        };
        const _dbWConnection = pgp(writeConfigParams);
        let _dbReaderObject = { "none": _dbRConnection.none, "multiple": multiple }, _dbWriterObject = { "none": _dbWConnection.none, "any": _dbWConnection.any };
        await _dbWConnection.any(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE; CREATE SCHEMA "${schemaName}";`);
        _target = new targetType(_dbReaderObject, _dbWriterObject, schemaName, tableName);
    });

    this.afterEach(async function () {
        pgp.end();
    });

    it('should be able to save and retrive static samples', async function () {

        await _target.define(_staticSampleType);
        let insertpayload = [
            [0, 1, 1.5, 1],
            [999, 2, 2.5, 2],
        ]
        await _target.upsert(insertpayload);
        let result = await _target.readRange(0, 998);
        assert.deepEqual(result, [{ "time": "0", "tagid": 1, "value": 1.5, "quality": 1 }]);
    });

    it('should be able ingest static samples in parallel', async function () {

        await _target.define(_staticSampleType);
        let insertpayload1 = [], insertpayload2 = [];
        let epoch = 0;
        for (let index = 0; index < 1000000; index++) {
            insertpayload1.push([(epoch + index), 1, index, 1]);
            insertpayload2.push([(epoch + index), 2, index, 1])
        }
        let p1 = _target.upsert(insertpayload1);
        let p2 = _target.upsert(insertpayload2);
        let elapsed = Date.now();
        await Promise.all([p1, p2])
        elapsed = Date.now() - elapsed;
        elapsed = elapsed / 1000;
        let speed = ((insertpayload1.length + insertpayload2.length) / elapsed);
        console.log("Ingestion speed is " + speed.toFixed(2));

        let result = await _target.readRange(0, 0);
        assert.deepEqual(result, [{ "time": "0", "tagid": 1, "value": 0, "quality": 1 },{ "time": "0", "tagid": 2, "value": 0, "quality": 1 }]);

    }).timeout(-1);

});