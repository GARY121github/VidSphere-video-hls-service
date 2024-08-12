import { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, S3Client } from "@aws-sdk/client-s3";
import ffmpeg from "fluent-ffmpeg";
import fs from "node:fs/promises";
import { createWriteStream, mkdir } from "node:fs";
import axios from "axios";
import { finished } from "stream";

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

async function notifyStatus(status, videoId) {
    try {
        await axios.patch(VIDEO_STATUS_API, { videoId: videoId, status: status });
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
        finished(writableStream, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
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

async function transcodeVideo(inputPath, outputDir, width, height) {
    return new Promise((resolve, reject) => {
        const outputFileName = `${outputDir}/index.m3u8`; // HLS playlist file

        ffmpeg(inputPath)
            .outputOptions([
                '-vf', `scale=${width}:${height}`,      // Video scaling
                '-c:v', 'libx264',                     // Video codec
                '-c:a', 'aac',                         // Audio codec
                '-ar', '48000',                        // Audio sampling rate
                '-b:a', '128k',                        // Audio bitrate
                '-hls_time', '10',                     // Segment duration in seconds
                '-hls_playlist_type', 'vod',           // Playlist type
                '-hls_segment_filename', `${outputDir}/segment_%03d.ts` // Segment file pattern
            ])
            .output(outputFileName)
            .on('end', () => resolve(outputFileName))
            .on('error', reject)
            .run();
    });
}

async function processVideo() {
    const videoPath = KEY.split('/');
    const originalFilePath = `original-video.mp4`;
    const basePath = `vidsphere/${videoPath[1]}/video/${videoPath[3]}`;

    try {
        console.log("Transcoding started");
        await notifyStatus("transcoding", videoPath[3]);
        await downloadVideo(BUCKET, KEY, originalFilePath);
        await deleteVideo(BUCKET, KEY);

        for (const resolution of RESOLUTIONS) {
            const outputDir = `${resolution.name}`; // Directory for this resolution's output
            const outputKey = `${basePath}/${resolution.name}`; // S3 key prefix for this resolution

            await fs.mkdir(outputDir); // Create directory for output

            const outputFilePath = await transcodeVideo(originalFilePath, outputDir, resolution.width, resolution.height);

            // Upload each segment and the m3u8 file
            const files = await fs.readdir(outputDir);
            await Promise.all(files.map(async (file) => {
                const filePath = `${outputDir}/${file}`;
                await uploadVideo(BUCKET, `${outputKey}/${file}`, filePath);
                await fs.unlink(filePath); // Clean up local files
            }));

            await fs.rmdir(outputDir); // Remove directory after upload
        }

        await notifyStatus("completed", videoPath[3]);
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
