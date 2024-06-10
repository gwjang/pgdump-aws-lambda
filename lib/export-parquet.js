const { S3 } = require('aws-sdk')
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

async function getAllTables(client) {
    const query = `
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema = '${process.env.PGSCHEMA}'
  `
    const res = await client.query(query)
    return res.rows.map(row => `${row.table_schema}.${row.table_name}`)
}

async function getColumnDetails(tableName, client) {
    const [schemaName, tableNameOnly] = tableName.split('.')
    const query = `
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
  `
    const res = await client.query(query, [schemaName, tableNameOnly])
    return res.rows
}

async function fetchTableData(tableName, client) {
    const [schemaName, tableNameOnly] = tableName.split('.')
    const query = `SELECT * FROM ${schemaName}.${tableNameOnly}`
    const res = await client.query(query)
    return { fields: res.fields, rows: res.rows }
}

async function saveToParquet(columnDetails, rows, tableNameOnly) {
    const s3Prefix = generateBackupPath(process.env.PGDATABASE, process.env.ROOT)
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
        Bucket: process.env.S3_BUCKET,
        Key: s3Key,
        Body: fs.createReadStream(localFilePath)
    }).promise()

    fs.unlinkSync(localFilePath)
    console.log(`Saved table ${tableNameOnly} to ${s3Key} in S3`)
}

exports.exportParquet = async (config, client) => {
    try {
        const allTables = await getAllTables(client)
        console.log(`Found ${allTables.length} tables`)
        const tablesToExport = allTables.filter(table => !excludeTables.includes(table))
        console.log(`Exporting ${tablesToExport.length} tables`)

        for (const table of tablesToExport) {
            try {
                const columnDetails = await getColumnDetails(table, client)
                const { fields, rows } = await fetchTableData(table, client)
                const tableNameOnly = table.split('.')[1]
                await saveToParquet(columnDetails, rows, tableNameOnly)
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
