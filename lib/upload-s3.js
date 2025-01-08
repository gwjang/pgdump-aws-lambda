const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3')

async function uploadS3(stream, key) {
    // S3 Client 생성
    const s3 = new S3Client({ region: process.env.S3_REGION })

    try {
        // S3 업로드 명령 실행
        await s3.send(
            new PutObjectCommand({
                Bucket: process.env.S3_BUCKET,
                Key: key,
                Body: stream,
                StorageClass: 'STANDARD'
            })
        )

        const location = `https://${process.env.S3_BUCKET}.s3.${process.env.S3_REGION}.amazonaws.com/${key}`
        console.log('Uploaded to', location)

        return location
    }
    catch (error) {
        console.error('Error uploading to S3:', error)
        throw error
    }
}

module.exports = uploadS3
