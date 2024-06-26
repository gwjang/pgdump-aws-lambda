const AWS = require('aws-sdk')

// configure AWS to log to stdout
AWS.config.update({
    logger: process.stdout
})

async function uploadS3(stream, key) {
    const s3 = new AWS.S3({
        region: process.env.S3_REGION
    })
    const result = await s3.upload({
        Key: key,
        Bucket: process.env.S3_BUCKET,
        Body: stream,
        StorageClass: 'STANDARD'
    }).promise()

    console.log('Uploaded to', result.Location)
    return result.Location
}

module.exports = uploadS3
