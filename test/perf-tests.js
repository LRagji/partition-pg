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


describe('Performance Tests', function () {

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
            max: 2 //1 Writer
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

    it('should be able ingest static samples greater than 40K/Sec', async function () {

        await _target.define(_staticSampleType);
        let insertpayload = []
        let epoch = Date.now();
        for (let index = 0; index < 1000000; index++) {
            insertpayload.push([(epoch + index), 1, index, 1])
        }
        let elapsed = Date.now();
        await _target.upsert(insertpayload);
        elapsed = Date.now() - elapsed;
        elapsed = elapsed / 1000;
        let speed = (insertpayload.length / elapsed);
        const expected = 40000;
        if (speed < expected) {
            assert.fail("Ingestion speed is " + speed.toFixed(2) + " expecting " + expected);
        } else {
            console.log("Ingestion speed is " + speed.toFixed(2));
        }

    }).timeout(-1);
});