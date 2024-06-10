const { S3 } = require('aws-sdk')
const { Client } = require('pg')
const fs = require('fs')
const {
    ParquetSchema,
    ParquetWriter
} = require('parquetjs')
const {
    generateBackupPath,
    mapPostgresTypeToParquet
} = require('./utils')

// Initialize S3 client
const s3 = new S3()

// PostgreSQL connection details
const getDbConfig = (config) => {
    return {
        user: config.PGUSER,
        host: config.PGHOST,
        database: config.PGDATABASE,
        schema: config.PGSCHEMA,
        password: config.PGPASSWORD,
        timeout: 1000 * 1000
    }
}

// Tables to exclude
const excludeTables = ['core.spatial_ref_sys', 'core.gongsilclub_sigungu', 'core.gongsilclub_dong', 'core.gongsilclub_block', 'core.gongsilclub_sido']

async function getAllTables(config) {
    const client = new Client(getDbConfig(config))
    await client.connect()
    const query = `
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema = '${config.PGSCHEMA}'
  `
    const res = await client.query(query)
    await client.end()
    return res.rows.map(row => `${row.table_schema}.${row.table_name}`)
}

async function getColumnDetails(tableName, config) {
    const client = new Client(getDbConfig(config))
    await client.connect()
    const [schemaName, tableNameOnly] = tableName.split('.')
    const query = `
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
  `
    const res = await client.query(query, [schemaName, tableNameOnly])
    await client.end()
    return res.rows
}

async function fetchTableData(tableName, config) {
    const client = new Client(getDbConfig(config))
    await client.connect()
    const [schemaName, tableNameOnly] = tableName.split('.')
    const query = `SELECT * FROM ${schemaName}.${tableNameOnly}`
    const res = await client.query(query)
    await client.end()
    return { fields: res.fields, rows: res.rows }
}

async function saveToParquet(columnDetails, rows, tableNameOnly, config) {
    const s3Prefix = generateBackupPath(config.PGDATABASE, config.ROOT)
    const localFilePath = `/tmp/${tableNameOnly}.parquet`

    const schemaDefinition = columnDetails.reduce((acc, col) => {
        acc[col.column_name] = mapPostgresTypeToParquet(col.data_type, col.is_nullable === 'YES')
        return acc
    }, {})

    const schema = new ParquetSchema(schemaDefinition)

    const writer = await ParquetWriter.openFile(schema, localFilePath)
    // await Promise.all(rows.map(row => writer.appendRow(row)))
    for (const row of rows) {
        await writer.appendRow(row)
    }
    await writer.close()

    const s3Key = `${s3Prefix}/${tableNameOnly}.parquet`
    await s3.upload({
        Bucket: config.S3_BUCKET,
        Key: s3Key,
        Body: fs.createReadStream(localFilePath)
    }).promise()

    fs.unlinkSync(localFilePath)
    console.log(`Saved table ${tableNameOnly} to ${s3Key} in S3`)
}

exports.exportParquet = async (config) => {
    try {
        const allTables = await getAllTables(config)
        const tablesToExport = allTables.filter(table => !excludeTables.includes(table))

        for (const table of tablesToExport) {
            try {
                const columnDetails = await getColumnDetails(table, config)
                const { fields, rows } = await fetchTableData(table, config)
                const tableNameOnly = table.split('.')[1]
                await saveToParquet(columnDetails, rows, tableNameOnly, config)
            }
            catch (err) {
                console.error(`Error exporting table ${table}: ${err.message}`)
            }
        }
        return { statusCode: 200, body: 'Export completed successfully' }
    }
    catch (err) {
        console.error(`Error: ${err.message}`)
        return { statusCode: 500, body: 'Internal Server Error' }
    }
}
