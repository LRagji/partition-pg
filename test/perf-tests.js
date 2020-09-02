const assert = require('assert');
const targetType = require('../dist/index').default;
const pgp = require('pg-promise')();
const utils = require('./utilities');
const localUtils = new utils();
const defaultConectionString = "postgres://postgres:mysecretpassword@localhost:5432/pgpartition?application_name=perf-test";

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
let tableName = "Raw", schemaName = "Anukram";
let _dbRConnection, _dbWConnection;


describe('Performance Tests', function () {

    this.beforeEach(async function () {
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
        _dbRConnection = pgp(readConfigParams);
        _dbWConnection = pgp(writeConfigParams);
        await localUtils.cleanDBInChunks(_dbRConnection, schemaName);

    }).timeout(-1);

    this.afterEach(async function () {
        pgp.end();
    });

    it('should be able write static samples greater than 40K/Sec and read them @ 180K/Sec', async function () {

        //Write
        const _target = new targetType(_dbRConnection, _dbWConnection, schemaName, tableName, _staticSampleType);
        await _target.create();
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
        const writeSpeedExpected = 40000;
        if (speed < writeSpeedExpected) {
            assert.fail("Ingestion speed is " + speed.toFixed(2) + " expecting " + writeSpeedExpected);
        } else {
            console.log("Ingestion speed is " + speed.toFixed(2));
        }

        //Read
        elapsed = Date.now();
        let result = await _target.readRange(epoch, (epoch + insertpayload.length));
        elapsed = Date.now() - elapsed;
        elapsed = elapsed / 1000;
        speed = (result.length / elapsed);
        assert.deepEqual(result.length, insertpayload.length);
        const readSpeedExpected = 180000;
        if (speed < readSpeedExpected) {
            assert.fail("Reading speed is " + speed.toFixed(2) + " expecting " + readSpeedExpected);
        } else {
            console.log("Reading speed is " + speed.toFixed(2));
        }

    }).timeout(-1);

    it('should be able write static samples worth an entire table(20000000) with 10K tables full load', async function () {

        //Write
        const _target = new targetType(_dbRConnection, _dbWConnection, schemaName, tableName, _staticSampleType);
        await _target.create();
        let insertpayload = []
        let epoch = Date.now();
        for (let index = 0; index < 20000000; index++) {
            insertpayload.push([(epoch + index), 1, index, 1])
        }
        let elapsed = Date.now();
        await _target.upsert(insertpayload);
        elapsed = Date.now() - elapsed;
        elapsed = elapsed / 1000;
        let speed = (insertpayload.length / elapsed);
        const writeSpeedExpected = 40000;
        if (speed < writeSpeedExpected) {
            assert.fail("Ingestion speed is " + speed.toFixed(2) + " expecting " + writeSpeedExpected);
        } else {
            console.log("Ingestion speed is " + speed.toFixed(2));
        }

        //Read
        elapsed = Date.now();
        let result = await _target.readRange(epoch, (epoch + insertpayload.length));
        elapsed = Date.now() - elapsed;
        elapsed = elapsed / 1000;
        speed = (result.length / elapsed);
        assert.deepEqual(result.length, insertpayload.length);
        const readSpeedExpected = 180000;
        if (speed < readSpeedExpected) {
            assert.fail("Reading speed is " + speed.toFixed(2) + " expecting " + readSpeedExpected);
        } else {
            console.log("Reading speed is " + speed.toFixed(2));
        }

    }).timeout(-1);
});