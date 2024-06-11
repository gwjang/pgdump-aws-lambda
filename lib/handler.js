const pg = require('pg')
const utils = require('./utils')
const uploadS3 = require('./upload-s3')
const pgdump = require('./pgdump')
const decorateWithIamToken = require('./iam')
const decorateWithSecretsManagerCredentials = require('./secrets-manager')
const encryption = require('./encryption')

const DEFAULT_CONFIG = require('./config')
const { exportParquet } = require('./export-parquet')

async function handler(event) {
    const baseConfig = { ...DEFAULT_CONFIG, ...event }
    let config

    if (event.USE_IAM_AUTH === true) {
        config = decorateWithIamToken(baseConfig)
    }
    else if (event.SECRETS_MANAGER_SECRET_ID) {
        config = await decorateWithSecretsManagerCredentials(baseConfig)
    }
    else {
        config = baseConfig
    }

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
