import MediaInfo from "mediainfo.js";
import fs from "fs";

/**
 * Fetches a media file from the provided URL and calculates its aspect ratio using MediaInfo.js.
 *
 * @async
 * @function getAspectRatio
 * @param {string} url - The URL of the media file to analyze.
 * @returns {Promise<string|false|undefined>} The aspect ratio of the video in the format "width:height".
 * Returns `undefined` if no video track is found in the media file.
 * @throws {Error} If the media file cannot be fetched or analyzed.
 *
 * @example
 * // Example usage
 * const url = 'https://example.com/video.m3u8';
 * getAspectRatio(url)
 *   .then(aspectRatio => {
 *     console.log('Aspect Ratio:', aspectRatio);
 *   })
 *   .catch(error => {
 *     console.error('Error:', error.message);
 *   });
 */
export async function getAspectRatio(url) {
  // MediaInfo.js'yi başlat
  const mediaInfo = await MediaInfo();

  // Playlist dosyasını indir
  const response = await fetch(url);
  if (!response.ok) {
    if (response.status === 404) {
      return false;
    }
    throw new Error(`Dosya alınırken hata oluştu: ${response.statusText}`);
  }

  const blob = await response.blob();

  // MediaInfo.js ile analiz için veri sağlayan bir fonksiyon
  const result = await mediaInfo.analyzeData(
    blob.size,
    async (chunkSize, offset) => {
      const buf = await blob.slice(offset, offset + chunkSize).arrayBuffer();
      return new Uint8Array(buf);
    },
  );

  const video = result.media.track.find((t) => t["@type"] === "Video");

  if (!video) {
    console.log("no video found");

    fs.writeFile("error.json", JSON.stringify(result), (err) => {
      if (err) {
        console.error(err);
      } else {
        // file written successfully
      }
    });

    return;
  }

  function gcd(a, b) {
    return b === 0 ? a : gcd(b, a % b);
  }

  const divisor = gcd(video.Width, video.Height);
  const simplifiedWidth = video.Width / divisor;
  const simplifiedHeight = video.Height / divisor;

  return `${simplifiedWidth}:${simplifiedHeight}`;
}

// Örnek kullanım
const url =
  "https://vz-cefd8cb9-508.b-cdn.net/98f8ee07-ed4b-4e46-a242-460c834e6cc8/original";
getAspectRatio(url).catch((err) => console.error("Hata:", err.message));
