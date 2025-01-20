import pg from "pg";
import Cursor from "pg-cursor";
import edjsParser from "editorjs-parser";
import { v4 } from "uuid";
import bcryptjs from "bcryptjs";
import fs from "fs/promises";
import dotenv from "dotenv";

import * as emoji from "node-emoji";
import OpenAI from "openai";

import orSlugify from "slugify";

// Read or create db.json

dotenv.config();

const { Pool } = pg;

export const slugify = (...args) => {
  if (!args[1]) {
    args[1] = {
      lower: true,
      strict: true,
    };
  }

  return orSlugify(...args);
};

/**
 * News tablosunu temsil eden bir obje.
 *
 * @typedef {Object} News
 * @property {string} id - Haberin benzersiz kimliği (varchar(255)).
 * @property {string} type - Haberin türü (public.enum_news_type).
 * @property {string} title - Haberin başlığı.
 * @property {string} description - Haberin açıklaması.
 * @property {string} slug - Haberin kısa adı (slug).
 * @property {string} content - Haberin içeriği.
 * @property {Object} thumbnails - Haberin küçük resim bilgileri (json).
 * @property {string} caption - Haberin başlığı veya alt metni.
 * @property {Object} seo - Haberin SEO bilgileri (json).
 * @property {string} status - Haberin durumu (public.enum_news_status).
 * @property {string} category_id - Kategori kimliği (varchar(255)).
 * @property {string} brand_id - Marka kimliği (varchar(255)).
 * @property {string} subcategory_id - Alt kategori kimliği (varchar(255)).
 * @property {number} number_of_view - Haberin görüntülenme sayısı (int4).
 * @property {boolean} is_premium - Premium içerik olup olmadığını belirtir.
 * @property {string} created_by - Haberi oluşturan kullanıcı (varchar(255)).
 * @property {string} approved_by - Haberi onaylayan kullanıcı (varchar(255)).
 * @property {string} published_at - Haberin yayınlanma zamanı (timestamptz).
 * @property {string[]} recommendation - Haberin önerilen içerikleri (_varchar).
 * @property {string} galleryId - Galeri kimliği (varchar(255)).
 * @property {string} created_at - Haberin oluşturulma zamanı (timestamptz).
 * @property {string} updated_at - Haberin güncellenme zamanı (timestamptz).
 * @property {string|null} deletedAt - Haberin silinme zamanı (timestamptz), eğer silinmemişse `null`.
 * @property {string} special_news_id - Özel haber kimliği (varchar(255)).
 * @property {string} audio - Haberin ses dosyası (varchar(255)).
 * @property {string} video - Haberin video dosyası (varchar(255)).
 * @property {string} list_title - Haberin liste başlığı (text).
 * @property {string} import_id - İthalat kimliği (varchar(255)).
 * @property {string} references - Haberin referans bilgileri (varchar(255)).
 */

/**
 * Columns tablosunu temsil eden bir obje.
 *
 * @typedef {Object} Columns
 * @property {string} id - Haberin benzersiz kimliği (varchar(255)).
 * @property {string} title - Haberin başlığı (varchar(255)).
 * @property {string} description - Haberin açıklaması (text).
 * @property {string} content - Haberin içeriği (text).
 * @property {boolean} is_active - Haber aktif mi, pasif mi olduğunu belirtir (bool).
 * @property {string} status - Haberin durumu (public.enum_columns_status).
 * @property {string} approved_by - Haberi onaylayan kullanıcının kimliği (varchar(255)).
 * @property {string} writer_id - Haberi yazan kişinin kimliği (varchar(255)).
 * @property {string} createdAt - Haberin oluşturulma zamanı (timestamptz).
 * @property {string} updatedAt - Haberin güncellenme zamanı (timestamptz).
 * @property {string|null} deletedAt - Haberin silinme zamanı, silinmediyse `null` (timestamptz(6)).
 * @property {number} number_of_view - Haberin görüntülenme sayısı (int4).
 * @property {Object} thumbnails - Haberin küçük resim bilgileri (json).
 * @property {string} brand_id - Haberin ait olduğu markanın kimliği (varchar(255)).
 * @property {string} category_id - Haberin kategorisinin kimliği (varchar(255)).
 * @property {string} subcategory_id - Haberin alt kategorisinin kimliği (varchar(255)).
 * @property {Object} seo - Haberin SEO bilgilerini içeren obje (json).
 * @property {string} caption - Haberin başlığı veya alt metni (varchar(255)).
 * @property {string} slug - Haberin kısa adı (varchar(255)).
 * @property {string} audio - Habere ait ses dosyası (varchar(255)).
 * @property {string} import_id - Haberin ithalat kimliği (varchar(255)).
 */

/**
 * Shorts tablosunu temsil eden bir obje.
 *
 * @typedef {Object} Shorts
 * @property {string} id - Short içeriğinin benzersiz kimliği (varchar(255)).
 * @property {string} title - Short içeriğinin başlığı (text).
 * @property {string} description - Short içeriğinin açıklaması (text).
 * @property {string} slug - Short içeriğinin kısa adı (slug, varchar(255)).
 * @property {string} url - Short içeriğinin URL'si (varchar(255)).
 * @property {Object} thumbnails - Short içeriğinin küçük resim bilgileri (json).
 * @property {string} status - Short içeriğinin durumu (public.enum_shorts_status).
 * @property {string} category_id - Short'un kategorisi için kimlik (varchar(255)).
 * @property {string} brand_id - Short'un markası için kimlik (varchar(255)).
 * @property {string} subcategory_id - Short'un alt kategorisi için kimlik (varchar(255)).
 * @property {number} number_of_views - Short içeriğinin görüntülenme sayısı (int4).
 * @property {string} created_by - Short içeriğini oluşturan kişinin kimliği (varchar(255)).
 * @property {string} approved_by - Short içeriğini onaylayan kişinin kimliği (varchar(255)).
 * @property {string} created_at - Short içeriğinin oluşturulma zamanı (timestamptz).
 * @property {string} updated_at - Short içeriğinin güncellenme zamanı (timestamptz).
 * @property {string|null} deletedAt - Short içeriğinin silinme zamanı (timestamptz(6)), eğer silinmediyse `null`.
 * @property {string} import_id - Short içeriği için ithalat kimliği (varchar(255)).
 */

const newDbConfigs = {
  host: process.env.NEW_DB_HOST,
  port: parseInt(process.env.NEW_DB_PORT),
  user: process.env.NEW_DB_USER,
  password: process.env.NEW_DB_PASSWORD,
  database: process.env.NEW_DB_DATABASE,
  ssl: {
    rejectUnauthorized: false,
    requestCert: false,
  },
};

const newClient = new Pool(newDbConfigs);
await newClient.connect();

/**
 * @param {string} title
 *
 * @returns {string}
 */
function cleanTitle(title) {
  const strippedEmojis = emoji.strip(title);
  const trimmed = strippedEmojis.trim();
  const dotless = trimmed.endsWith(".") ? trimmed.slice(0, -1) : trimmed;

  return dotless;
}

const run = async () => {
  const READ_COUNT = 1000; // slice length
  const BATCH_COUNT = 512;

  console.log("running");
  /**
   * @type { News[] }
   */
  const news = await newClient.query(
    "SELECT * FROM news ORDER BY published_at DESC",
  );
  console.log("fetched news", news.rows.length);

  const process = async () => {
    let promises = [];

    const newsSlice = news.rows.splice(0, READ_COUNT);
    console.log("news length", newsSlice.length);

    for (const newsRow of newsSlice) {
      try {
        const updateNewsProcess = async () => {
          const dotless = cleanTitle(newsRow.title);

          const dotlessList = cleanTitle(newsRow.list_title);

          const query = {
            // give the query a unique name
            name: "update-news" + Date.now().toString(),
            text: `UPDATE news SET "slug"=$1, "title"=$2, "list_title"=$3 WHERE "id"=$4 RETURNING *`,
            values: [slugify(newsRow.slug), dotless, dotlessList, newsRow.id],
          };

          const res = await newClient.query(query).catch(console.log);

          console.log(
            "UPDATED NEWS: ",
            slugify(newsRow.slug),
            dotless,
            dotlessList,
            newsRow.id,
          );
        };

        promises.push(updateNewsProcess());
      } catch (err) {
        console.error(err);
      }
    }

    if (promises.length > BATCH_COUNT - 1) {
      console.info(new Date().toISOString(), ": ", "executing " + BATCH_COUNT);
      await Promise.all(promises).catch(console.error);

      promises = [];
    }

    if (promises.length) {
      console.info(
        new Date().toISOString(),
        ": ",
        "executing " + promises.length,
      );
      await Promise.all(promises).catch(console.error);
      promises = [];
    }

    if (newsSlice.length === READ_COUNT) {
      return process();
    } else {
      return true;
    }
  };

  const res = await process();

  /**
   * @type { Shorts[] }
   */
  const shorts = await newClient.query("SELECT * FROM shorts");

  const shortsProcess = async () => {
    let promises = [];

    const shortsSlice = shorts.rows.splice(0, READ_COUNT);

    for (let shortsRow of shortsSlice) {
      try {
        const updateShortsProcess = async () => {
          const dotless = cleanTitle(shortsRow.title);

          const query = `UPDATE shorts SET slug=$1,title=$2 WHERE id = '${shortsRow.id}'`;
          const values = [slugify(shortsRow.slug), dotless];

          await newClient.query(query, values);
          console.log("UPDATED SHORTS: ", shortsRow.id, " ", shortsRow.title);
        };

        promises.push(updateShortsProcess());
      } catch (err) {
        console.error(err);
      }
    }

    if (promises.length > BATCH_COUNT - 1) {
      console.info(new Date().toISOString(), ": ", "executing " + BATCH_COUNT);
      await Promise.all(promises).catch(console.error);

      promises = [];
    }

    if (promises.length) {
      console.info(
        new Date().toISOString(),
        ": ",
        "executing " + promises.length,
      );
      await Promise.all(promises).catch(console.error);
      promises = [];
    }

    if (shortsSlice.length === READ_COUNT) {
      return shortsProcess();
    } else {
      return true;
    }
  };

  const res2 = await shortsProcess();

  console.info(new Date().toISOString(), ": ", "Migration done...");

  await newClient.end();
};

run();
