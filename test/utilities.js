module.exports = class TestUtils {

    constructor() {
        this.cleanDBInChunks = this.cleanDBInChunks.bind(this);
    }

    async cleanDBInChunks(dbCon, schemaName) {
        let tables = [];
        do {
            tables = await dbCon.any(`SELECT table_name
                                        FROM
                                            information_schema.tables
                                        WHERE
                                            table_type = 'BASE TABLE'
                                        AND
                                            table_schema IN ('${schemaName}')
                                        ORDER BY table_name LIMIT 1000`);
            let sql = tables.reduce((acc, ele) => acc + `DROP TABLE  "${schemaName}"."${ele.table_name}";`, "");
            if (sql != "") {
                await dbCon.any(sql);
            }
        }
        while (tables.length > 0)
        await dbCon.any(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE; CREATE SCHEMA "${schemaName}";`);
    }
}