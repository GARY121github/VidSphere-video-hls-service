import { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, S3Client } from "@aws-sdk/client-s3";
import ffmpeg from "fluent-ffmpeg";
import fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import axios from "axios";

const RESOLUTIONS = [
    { name: "360p", width: 480, height: 360 },
    { name: "480p", width: 858, height: 480 },
    { name: "720p", width: 1280, height: 720 },
    { name: "1080p", width: 1920, height: 1080 }
];

const s3Client = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
});

const BUCKET = process.env.BUCKET_NAME;
const KEY = process.env.KEY;
const VIDEO_STATUS_API = process.env.VIDEO_STATUS_API;


async function notifyStatus(status , videoId) {
    try {
        await axios.patch(VIDEO_STATUS_API, { videoId : videoId , status : status });
    } catch (error) {
        console.error("Error notifying status:", error);
    }
}

async function downloadVideo(bucket, key, filePath) {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const result = await s3Client.send(command);
    const writableStream = createWriteStream(filePath);
    result.Body.pipe(writableStream);

    return new Promise((resolve, reject) => {
        writableStream.on('finish', resolve);
        writableStream.on('error', reject);
    });
}

async function deleteVideo(bucket, key) {
    try {
        const deleteCommand = new DeleteObjectCommand({ Bucket: bucket, Key: key });
        await s3Client.send(deleteCommand);
    } catch (error) {
        console.error("Error deleting video from S3:", error);
    }
}

async function uploadVideo(bucket, key, filePath) {
    const fileBuffer = await fs.readFile(filePath);
    const putCommand = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: fileBuffer,
        ContentType: 'video/mp4'
    });
    await s3Client.send(putCommand);
}

async function transcodeVideo(inputPath, outputPath, width, height) {
    return new Promise((resolve, reject) => {
        ffmpeg(inputPath)
            .output(outputPath)
            .videoCodec("libx264")
            .audioCodec("aac")
            .size(`${width}x${height}`)
            .on('end', resolve)
            .on('error', reject)
            .format("mp4")
            .run();
    });
}

async function processVideo() {
    const videoPath = KEY.split('/');
    const originalFilePath = `original-video.mp4`;
    const basePath = `vidsphere/${videoPath[1]}/video/${videoPath[3]}`;

    try {
        console.log("Transcoding started");
        await notifyStatus("transcoding" , videoPath[3]);
        await downloadVideo(BUCKET, KEY, originalFilePath);
        await deleteVideo(BUCKET, KEY);

        for (const resolution of RESOLUTIONS) {
            const outputFilePath = `${resolution.name}.mp4`;
            const outputKey = `${basePath}/${resolution.name}.mp4`;

            await transcodeVideo(originalFilePath, outputFilePath, resolution.width, resolution.height);
            await uploadVideo(BUCKET, outputKey, outputFilePath);
            await fs.unlink(outputFilePath);
        }

        await notifyStatus("completed" , videoPath[3]);
    } catch (error) {
        console.error("Error during processing:", error);
        try {
            await uploadVideo(BUCKET, KEY, originalFilePath);
        } catch (uploadError) {
            console.error("Error uploading original video back to S3:", uploadError);
        }
    } finally {
        await fs.unlink(originalFilePath);
    }

    console.log("Transcoding complete");
}

processVideo()
    .catch(error => console.error("Unhandled error:", error))
    .finally(() => process.exit(0));
