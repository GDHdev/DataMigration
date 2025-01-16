import dotenv from "dotenv";
import ollama from "ollama";
import pg from "pg";
import Cursor from "pg-cursor";

import { JSONFilePreset } from "lowdb/node";

dotenv.config();

const defaultData = { ratios: [], news: [] };
const db = await JSONFilePreset("metadata.json", defaultData);

const { Client } = pg;

const oldDbConfigs = {
  host: process.env.OLD_DB_HOST,
  port: parseInt(process.env.OLD_DB_PORT),
  user: process.env.OLD_DB_USER,
  password: process.env.OLD_DB_PASSWORD,
  database: process.env.OLD_DB_DATABASE,
  // ssl: {
  //   rejectUnauthorized: false,
  //   requestCert: false,
  // },
};

const client = new Client(oldDbConfigs);

/**
 * Bir tablo kaydını temsil eden obje.
 *
 * @typedef {Object} Story
 * @property {number} id - Kaydın benzersiz kimliği (serial4).
 * @property {Object} video - Video bilgilerini içeren JSONB veri türü.
 * @property {Object} embed - Gömülü içerik bilgilerini içeren JSONB veri türü.
 * @property {string} title - İçeriğin başlığı.
 * @property {string} message - İçeriğin mesajı.
 * @property {string} content - İçeriğin ana metni.
 * @property {Object} content_data - İçeriğin ek verilerini içeren JSONB veri türü.
 * @property {boolean} pinned - Kaydın sabitlenmiş olup olmadığını belirtir.
 * @property {string} status - İçeriğin durumunu temsil eden enum türü.
 * @property {string} created_from - Kaydın oluşturulduğu yer bilgisi.
 * @property {string} updated_from - Kaydın güncellendiği yer bilgisi.
 * @property {string} updated_at - Kaydın son güncellenme zamanı (timestamp).
 * @property {string} created_at - Kaydın oluşturulma zamanı (timestamp).
 * @property {string|null} deleted_at - Kaydın silinme zamanı, silinmemişse `null`.
 * @property {number} created_by_id - Kaydı oluşturan kullanıcının kimliği (int4).
 * @property {number} updated_by_id - Kaydı güncelleyen kullanıcının kimliği (int4).
 * @property {string} chat - Chat bilgileri.
 * @property {Object} message_data - Mesaj verilerini içeren JSONB veri türü.
 * @property {number} chat_id - Chat kimliği (int4).
 * @property {Object} audio - Ses verilerini içeren JSONB veri türü.
 * @property {Object} images - Görselleri içeren JSONB veri türü.
 * @property {string} poll - Anket bilgisi.
 * @property {number} poll_id - Anket kimliği (int4).
 * @property {Object} references - Referans bilgilerini içeren JSONB veri türü.
 * @property {string} published_at - Yayınlanma zamanı (timestamp).
 * @property {number} assignee_id - Atanan kişinin kimliği (int4).
 * @property {number} brand_id - Markanın kimliği (int4).
 * @property {number} category_id - Kategorinin kimliği (int4).
 * @property {number} popularity - Popülerlik değeri (int4).
 * @property {number} author_id - Yazara ait kimlik (int4).
 * @property {string} slug - İçeriğin kısa adı.
 * @property {Object} seo - SEO bilgilerini içeren JSONB veri türü.
 * @property {boolean} global - Küresel bir içerik olup olmadığını belirtir.
 * @property {boolean} premium - Premium içerik olup olmadığını belirtir.
 * @property {number} stat_views - Görüntülenme sayısı (int4).
 * @property {number} stat_hits - Hit sayısı (int4).
 * @property {number} stat_comments - Yorum sayısı (int4).
 * @property {number} stat_bookmarks - Kaydetme sayısı (int4).
 * @property {number} stat_favorites - Favorilere eklenme sayısı (int4).
 * @property {string} vector - TSVector veri türü (metin araması için).
 * @property {boolean} breaking - Çarpıcı haber olup olmadığını belirtir.
 * @property {boolean} sidebar - Yan panelde gösterilip gösterilmeyeceğini belirtir.
 * @property {string} breaking_text - Çarpıcı haber metni.
 * @property {boolean} election - Seçim bilgisiyle ilişkili olup olmadığını belirtir.
 */

/**
 * @type { Cursor<Story> }
 */
const stories = await client.query(
  new Cursor(
    "SELECT * FROM story WHERE status='published' and author_id is not null and video::jsonb is null and content_data is not null",
  ),
);
const READ_COUNT = 50;

/**
 * @type {Array<Story>}
 */

await client.connect();

let readed = await stories.read(READ_COUNT);

console.log(readed.length);
do {
  for (const story of readed) {
    const exist = db.data.news.find((r) => r.id === story.id.toString());

    if (exist) {
      continue;
    }

    const title = story.title || story.message;

    if (title.length <= 60) {
      continue;
    }

    const response = await ollama.chat({
      model: "gemma2:latest",
      messages: [
        {
          role: "system",
          content:
            "Sen bir başlık kısaltma uzmanısın. Sana verilen başlığı 60 karakterin altında olacak şekilde yeniden yaz. Başlığın tarzını ve mesajını koru. Sadece kısaltılmış başlığı döndür, başka açıklama yapma.",
        },
        {
          role: "user",
          content: title,
        },
      ],
    });

    const list_title = response.message.content;

    await db.data.news.push({
      id: story.id.toString(),
      title: title,
      list_title,
    });

    await db.write();
  }

  console.log("executed", readed.length);

  readed = await stories.read(READ_COUNT);
  console.log("next batch", readed.length);
} while (readed.length === READ_COUNT);

console.log("bitti");
