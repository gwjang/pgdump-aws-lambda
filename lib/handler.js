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

    try {
        return await exportParquet(config)
    }
    catch (error) {
        // log the error and rethrow for Lambda
        if (process.env.NODE_ENV !== 'test') {
            console.error(error)
        }
        throw error
    }
}

module.exports = handler
