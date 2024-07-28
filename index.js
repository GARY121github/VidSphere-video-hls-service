import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import ffmpeg from "fluent-ffmpeg";
import fs from "node:fs/promises";
import path from "node:path";
import { createWriteStream } from "node:fs";

const RESOLUTIONS = [
    {
        name: "360p",
        width: 480,
        height: 360
    },
    {
        name: "480p",
        width: 858,
        height: 480
    },
    {
        name: "720p",
        width: 1280,
        height: 720
    }
];

const s3Client = new S3Client({
    region: "ap-south-1",
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
});

const BUCKET = process.env.BUCKET_NAME;
const KEY = process.env.KEY;

console.log("BUCKET", BUCKET);
console.log("KEY", KEY);
console.log("AWS_ACCESS_KEY_ID", process.env.AWS_ACCESS_KEY_ID);
console.log("AWS_SECRET_ACCESS_KEY", process.env.AWS_SECRET_ACCESS_KEY);

async function init() {
    const command = new GetObjectCommand({
        Bucket: BUCKET,
        Key: KEY
    });

    // Download the video
    const result = await s3Client.send(command);

    const originalFilePath = `original-video.mp4`;
    const writableStream = createWriteStream(originalFilePath);

    // Use a stream to handle the data
    result.Body.pipe(writableStream);

    // Wait for the file to be fully written
    await new Promise((resolve, reject) => {
        writableStream.on('finish', resolve);
        writableStream.on('error', reject);
    });

    const originalVideoPath = path.resolve(originalFilePath);

    // Start the transcoder
    const promises = RESOLUTIONS.map(resolution => {
        const output = `video-${resolution.name}.mp4`;

        return new Promise((resolve, reject) => {
            ffmpeg(originalVideoPath)
                .output(output)
                .videoCodec("libx264")
                .audioCodec("aac")
                .size(`${resolution.width}x${resolution.height}`)
                .on('end', async () => {
                    // Upload the video
                    const fileBuffer = await fs.readFile(output);
                    const putCommand = new PutObjectCommand({
                        Bucket: BUCKET,
                        Key: `${output}`,
                        Body: fileBuffer,
                        ContentType: 'video/mp4'
                    });
                    await s3Client.send(putCommand);
                    console.log(`Uploaded video-${resolution.name}`);
                    resolve();
                })
                .on('error', reject)
                .format("mp4")
                .run();
        });
    });

    await Promise.all(promises);
}

init()
.catch(error => console.error(error))
.finally(() => process.exit(0));
