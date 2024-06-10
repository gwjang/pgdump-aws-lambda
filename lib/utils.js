const moment = require('moment')
const path = require('path')

module.exports = {
    generateBackupPath(databaseName, rootPath, now = null) {
        now = now || moment().utc()
        const day = moment(now).format('YYYY/MM/DD')
        const key = path.join(rootPath || '', day)
        return key
    }
}

function mapPostgresTypeToParquet(type, isNullable) {
    let result
    switch (type) {
    case 'int2':
    case 'int4':
    case 'int8':
    case 'smallint':
    case 'integer':
    case 'bigint':
        result = { type: 'INT64' }
        break
    case 'float4':
    case 'float8':
    case 'double precision':
        result = { type: 'DOUBLE' }
        break
    case 'bool':
    case 'boolean':
        result = { type: 'BOOLEAN' }
        break
    case 'timestamp':
    case 'timestamptz':
    case 'date':
    case 'timestamp without time zone':
        result = { type: 'TIMESTAMP_MILLIS' }
        break
    case 'text':
    case 'varchar':
    case 'char':
    case 'uuid':
    case 'character varying':
        result = { type: 'UTF8' }
        break
    default:
        result = { type: 'UTF8' } // Default to string for any unrecognized types
    }
    if (isNullable) {
        result.optional = true
    }
    return result
}

module.exports.mapPostgresTypeToParquet = mapPostgresTypeToParquet
