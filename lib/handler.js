const pg = require('pg')

const DEFAULT_CONFIG = require('./config')
const { exportParquet } = require('./export-parquet')

async function handler(event) {
    const baseConfig = { ...DEFAULT_CONFIG, ...event }
    const config = baseConfig

    const pool = new pg.Pool({
        user: process.env.PGUSER,
        host: process.env.PGHOST,
        database: process.env.PGDATABASE,
        schema: process.env.PGSCHEMA,
        password: process.env.PGPASSWORD,
        timeout: 1000 * 1000
    })
    const client = await pool.connect()

    try {
        await exportParquet(config, client)
        client.end()
    }
    catch (err) {
        // log the error and rethrow for Lambda
        if (process.env.NODE_ENV !== 'test') {
            console.error(err)
        }
        client.end()
    }
}

module.exports = handler
