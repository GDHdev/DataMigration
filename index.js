import pg from "pg";
import Cursor from "pg-cursor";
import edjsParser from "editorjs-parser";
import { v4 } from "uuid";
import bcryptjs from "bcryptjs";
import fs from "fs/promises";
import dotenv from "dotenv";

import { JSONFilePreset } from "lowdb/node";

import { getAspectRatio } from "./media.js";

import OpenAI from "openai";

import orSlugify from "slugify";

import workerpool from "workerpool";
import * as emoji from "node-emoji";

const pool = workerpool.pool();

// Read or create db.json

/**
 * @type { {ratios: VideoMetadata[], news: Array<{id: string, title: string, list_title: string}>} }
 */
const defaultData = { ratios: [], news: [] };
const db = await JSONFilePreset("metadata.json", defaultData);

dotenv.config();

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const { Client } = pg;

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

/**
 * @typedef {Object} Infographics
 * @property {string} id - Unique identifier for the record (max length: 255).
 * @property {string} description - Detailed description of the record (text type).
 * @property {string} image_url - URL of the image (max length: 255).
 * @property {Object} seo - SEO configuration stored as JSON.
 * @property {boolean} isActive - Indicates if the record is active.
 * @property {number} order - Order of the record (integer).
 * @property {string} created_at - Timestamp when the record was created (with timezone).
 * @property {string} updated_at - Timestamp when the record was last updated (with timezone).
 * @property {string|null} deletedAt - Timestamp when the record was deleted (nullable, with 6 fractional seconds).
 * @property {string} title - Title of the record (max length: 255).
 * @property {string} slug - URL-friendly identifier (max length: 255).
 * @property {string} import_id - importId identifier (max length: 255).
 */

const oldDbConfigs = {
  host: process.env.OLD_DB_HOST,
  port: parseInt(process.env.OLD_DB_PORT),
  user: process.env.OLD_DB_USER,
  password: process.env.OLD_DB_PASSWORD,
  database: process.env.OLD_DB_DATABASE,
  ssl: {
    rejectUnauthorized: false,
    requestCert: false,
  },
};
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

const client = new Client(oldDbConfigs);

const newClient = new Client(newDbConfigs);

const parserConf = {
  image: {
    use: "figure",
    // use figure or img tag for images (figcaption will be used for caption of figure)
    // if you use figure, caption will be visible
    imgClass: "img", // used class for img tags
    figureClass: "image", // used class for figure tags
    figCapClass: "figcaption", // used class for figcaption tags
    path: "absolute",
    // if absolute is passed, the url property which is the absolute path to the image will be used
    // otherwise pass a relative path with the filename property in <> like so: '/img/<fileName>'
  },
  paragraph: {
    pClass: "paragraph", // used class for paragraph tags
  },
  code: {
    codeBlockClass: "code-block", // used class for code blocks
  },
  embed: {
    useProvidedLength: false,
    // set to true if you want the returned width and height of editorjs to be applied
    // NOTE: sometimes source site overrides the lengths so it does not work 100%
  },
  quote: {
    applyAlignment: false,
    // if set to true blockquote element will have text-align css property set
  },
};

const capitalizeWords = (str) =>
  str
    .split(" ")
    .map((i) => `${i[0].toUpperCase()}${i.slice(1).toLowerCase()}`)
    .join(" ");

const saveBrands = async () => {
  const brands = await client.query("SELECT * FROM brand");

  console.info(new Date().toISOString(), ": ", "brands", brands.rows);

  fs.writeFile("brands.json", JSON.stringify(brands.rows));
};

const saveCategories = async () => {
  const categories = await client.query("SELECT * FROM category");

  console.info(new Date().toISOString(), ": ", "categories", categories.rows);

  fs.writeFile("categories.json", JSON.stringify(categories.rows));
};

const getTags = async () => {
  const tags = await client.query("SELECT * FROM tag");
  return tags.rows;
};

const getTagRelations = async () => {
  const storyTags = await client.query("SELECT * FROM story_tag");

  return storyTags.rows;
};

const getTagRelationByStoryId = async (id) => {
  const storyTags = await client.query("SELECT * FROM story_tag");

  return storyTags.rows;
};

const createEditor = async (editor) => {
  console.info(new Date().toISOString(), ": ", "creating new editor..");
  const id = v4();
  const query = `INSERT INTO editors(id, email, fullname, password, is_active, role_id, is_email_verified, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`;
  const password = await bcryptjs.hash("123456", 10);
  const values = [
    id,
    editor.email || editor.user?.email,
    editor.full_name,
    password,
    true,
    "editr",
    true,
    new Date().toISOString(),
    new Date().toISOString(),
  ];

  await newClient.query(query, values);

  const { rows: inserted } = await newClient.query(
    `SELECT * FROM editors WHERE id='${id}'`,
  );

  return inserted[0];
};

const getAuthors = async () => {
  const { rows: authors } = await client.query(
    `SELECT author.* , to_json(u) "user" FROM author LEFT JOIN "user" as u ON author.user_id = u.id`,
  );

  const { rows: editors } = await newClient.query(`SELECT * from editors`);

  for (const author of authors) {
    const relatedEditor = editors.find(
      (i) =>
        i.email === author.email ||
        i.email === author.user?.email ||
        i.fullname === author.full_name,
    );
    if (relatedEditor) {
      author.mapped = relatedEditor;
    } else {
      author.mapped = await createEditor(author);
    }
  }

  return authors;
};

const createWriter = async (writer) => {
  console.info(new Date().toISOString(), ": ", "creating new writer..");
  const id = v4();
  const query = `INSERT INTO writers(id, email, fullname, password, is_active, slug, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`;
  const password = await bcryptjs.hash("123456", 10);
  const values = [
    id,
    writer.email || writer.user?.email,
    writer.fullname,
    password,
    true,
    writer.fullname.replace(" ", "-").toLocaleLowerCase("tr-TR") + "-" + id,
    new Date().toISOString(),
    new Date().toISOString(),
  ];

  await newClient.query(query, values);

  const { rows: inserted } = await newClient.query(
    `SELECT * FROM writers WHERE id='${id}'`,
  );

  console.info(new Date().toISOString(), ": ", inserted);

  return inserted[0];
};

const getWriters = async () => {
  const { rows: writers } = await newClient.query(`SELECT * from writers`);

  return writers;
};

const createNewBrand = async (brand) => {
  console.info(
    new Date().toISOString(),
    ": ",
    `creating new brand ${brand.mapped}`,
  );
  const { rows: existed } = await newClient.query(
    `SELECT * FROM brands WHERE slug='${brand.mapped}'`,
  );
  if (existed.length) {
    return existed[0];
  }
  const id = v4();
  const createdAt = new Date().toISOString();
  const query = `INSERT INTO public.brands(id, name, slug, description, icon_url, seo, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`;
  const values = [
    id,
    capitalizeWords(brand.name.trim(" ")),
    brand.mapped,
    brand.description || "",
    "",
    JSON.stringify(brand.seo),
    createdAt,
    createdAt,
  ];

  const res = await newClient.query(query, values);

  const { rows: inserted } = await newClient.query(
    `SELECT * FROM brands WHERE id='${id}'`,
  );

  console.info(new Date().toISOString(), ": ", "inserted", inserted[0]);

  return inserted[0];
};

/**
 * Haber oluşturma işlemini gerçekleştirir.
 *
 * @param {News} news - Haber bilgilerini içeren obje.
 * @param {Number} editorId - Editör id si
 * @returns {Promise<News|boolean>} - Yeni oluşturulan haber objesi veya zaten varsa `false` döner.
 */
const createNews = async (news, editorId) => {
  // console.info(new Date().toISOString(),": ",`creating news ${news.id}`);
  const query = `INSERT INTO news(${Object.keys(news).join(
    ",",
  )}) VALUES (${Object.keys(news).map((item, index) => `$${index + 1}`)})`;
  const values = Object.values(news);

  const exist = await newClient.query(
    `SELECT * FROM news WHERE import_id='${news.import_id}'`,
  );

  if (exist.rows[0]) {
    //console.info(new Date().toISOString(),": ","exist", exist.rows[0].id);
    return false;
  }

  const res = await newClient.query(query, values);

  await createEditorNewsRelation(news.id, editorId);

  return res.rows[0];
};

/**
 * Köşe yazısı oluşturma işlemini gerçekleştirir.
 *
 * @param {Columns} column - Köşe yazısı bilgilerini içeren obje.
 * @returns {Promise<void>} - Yeni oluşturulan Köşe yazısı objesi veya zaten varsa `false` döner.
 */
const createColumn = async (column) => {
  // console.info(new Date().toISOString(),": ",`creating column ${column.id}`);
  const query = `INSERT INTO columns(${Object.keys(column)
    .map((c) => `"${c}"`)
    .join(
      ",",
    )}) VALUES (${Object.keys(column).map((item, index) => `$${index + 1}`)})`;
  const values = Object.values(column);

  const exist = await newClient.query(
    `SELECT * FROM columns WHERE import_id='${column.import_id}'`,
  );

  if (exist.rows[0]) {
    //console.info(new Date().toISOString(),": ","exist", exist.rows[0].id);
    return false;
  }

  const res = await newClient.query(query, values);

  return res.rows[0];
};

const createEditorNewsRelation = async (newsId, editorId) => {
  // console.info(new Date().toISOString(),": ",`creating news (${newsId}) editor (${editorId}) relation`);
  const createdAt = new Date().toISOString();
  const query =
    'INSERT INTO news_writers_pivot(news_id, editor_id, "createdAt", "updatedAt") VALUES ($1,$2,$3,$4)';
  const values = [newsId, editorId, createdAt, createdAt];

  const res = await newClient.query(query, values);

  return res.rows[0];
};

/**
 * @param {Shorts} shorts
 * @returns {Promise<Shorts | false>}
 */
async function createShorts(shorts) {
  const query = `INSERT INTO shorts(${Object.keys(shorts)
    .map((c) => `"${c}"`)
    .join(
      ",",
    )}) VALUES (${Object.keys(shorts).map((item, index) => `$${index + 1}`)})`;
  const values = Object.values(shorts);

  const exist = await newClient.query(
    `SELECT * FROM shorts WHERE import_id='${shorts.import_id}'`,
  );

  if (exist.rows[0]) {
    //console.info(new Date().toISOString(),": ","exist", exist.rows[0].id);
    return false;
  }

  const res = await newClient.query(query, values);

  return res.rows[0];
}

/**
 * @param {Infographics} infographic
 * @returns {Promise<Infographics | false>}
 */
async function createInfographics(infographic) {
  const query = `INSERT INTO infographics(${Object.keys(infographic)
    .map((c) => `"${c}"`)
    .join(
      ",",
    )}) VALUES (${Object.keys(infographic).map((item, index) => `$${index + 1}`)})`;
  const values = Object.values(infographic);

  const exist = await newClient.query(
    `SELECT * FROM infographics WHERE import_id='${infographic.import_id}'`,
  );

  if (exist.rows[0]) {
    //console.info(new Date().toISOString(),": ","exist", exist.rows[0].id);
    return false;
  }

  const res = await newClient.query(query, values);

  return res.rows[0];
}

const run = async () => {
  const READ_COUNT = 1000; // slice length
  const BATCH_COUNT = 512;

  // connect news and old dbs
  await client.connect();
  await newClient.connect();

  const authors = await getAuthors();
  const writers = await getWriters();

  console.info(
    new Date().toISOString(),
    ": ",
    "deleted brands getting removed..",
  );
  // await newClient.query("DELETE FROM brands WHERE deleted_at is not null");
  // congiure parser
  const parser = new edjsParser(parserConf, {
    heading: function (data) {
      return `<h${data.level}>${data.text}</h${data.level}>`;
    },
  });

  // read json datas
  let [oldBrands, oldCategories] = (
    await Promise.all([
      fs.readFile("./brands.json"),
      fs.readFile("./categories.json"),
    ])
  ).map((i) => JSON.parse(i.toString("utf-8")));

  oldBrands = oldBrands.filter((i) => !!i.mapped);
  oldCategories = oldCategories.filter((i) => !!i.mapped);

  const { rows: newBrands } = await newClient.query(
    "SELECT * FROM brands WHERE deleted_at is null",
  );

  const newBrandMapping = {};
  const newCategoryMapping = {};
  for (let oldBrand of oldBrands) {
    const newBrand = newBrands.find((i) => i.slug === oldBrand.mapped);
    if (newBrand) {
      newBrandMapping[oldBrand.id] = newBrand;
    } else if (!newBrand && oldBrand.slug !== "infografik") {
      newBrandMapping[oldBrand.id] = await createNewBrand(oldBrand);
    } else {
      console.info(new Date().toISOString(), ": ", "infografik..");
    }
  }

  for (let oldCategory of oldCategories) {
    const newBrand = newBrands.find((i) => i.slug === oldCategory.mapped);
    if (newBrand) {
      newCategoryMapping[oldCategory.id] = newBrand;
    } else if (!newBrand && oldCategory.slug !== "infografik") {
      newCategoryMapping[oldCategory.id] = await createNewBrand(oldCategory);
    } else {
      console.info(new Date().toISOString(), ": ", "infografik..");
    }
  }

  /**
   * @type { Cursor<Story> }
   */
  const stories = await client.query(
    new Cursor(
      "SELECT * FROM story WHERE status='published' and author_id is not null ORDER BY published_at DESC",
    ),
  );

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

  const process = async () => {
    let promises = [];
    const slice = await stories.read(READ_COUNT);
    for (let row of slice) {
      /**
       * Video metadata bilgilerini temsil eden bir obje.
       *
       * @typedef {Object} VideoMetadata
       * @property {string} id - Videonun benzersiz kimliği.
       * @property {string} title - Videonun başlığı veya mesajı. Eğer başlık yoksa `row.message` kullanılır.
       * @property {string} source - Videonun orijinal kaynağının URL'si.
       * @property {string} thumbnail - Videonun küçük resim URL'si. Orijinal URL "original" yerine "thumbnail.jpg" ile değiştirilir.
       * @property {string} ratio - Videonun en-boy oranı (aspect ratio).
       */

      // if (!row.images || row.images.length < 1) {
      //   console.info(new Date().toISOString(),": ","no image..");
      //   continue;
      // }

      const relatedAuthor = authors.find((i) => i.id === row.author_id);

      if (!relatedAuthor) {
        console.info(
          new Date().toISOString(),
          ": ",
          `author ${row.author_id} is not found`,
        );
        continue;
      }
      const newDbEditor = relatedAuthor.mapped;

      if (
        !newDbEditor ||
        (!newBrandMapping[row.brand_id] && !newCategoryMapping[row.category_id])
      ) {
        console.info(
          new Date().toISOString(),
          ": ",
          "editor brand or category does not exist",
        );
        continue;
      }

      const brand = newBrandMapping[row.brand_id]
        ? newBrandMapping[row.brand_id]
        : newCategoryMapping[row.category_id];

      if (!brand) {
        continue;
      }

      if (
        brand.slug === "yasam" &&
        newDbEditor.fullname === "Abdullah Aydemir"
      ) {
        console.info(
          new Date().toISOString(),
          ": ",
          "Abdullah Aydemir skipped",
        );
        continue;
      }

      try {
        const creationProcess = async () => {
          /**
           * @type {VideoMetadata | undefined}
           */
          let videoMeta;

          if (row.video) {
            /**
             * @type {VideoMetadata | undefined}
             */
            const exist = db.data.ratios.find(
              (r) => r.id === row.id.toString(),
            );

            if (exist) {
              videoMeta = exist;
            } else {
              console.info(
                new Date().toISOString(),
                ": ",
                "started: extracting metadata",
              );
              const ratio = await getAspectRatio(row.video.source);
              console.info(
                new Date().toISOString(),
                ": ",
                "extracted news metadata",
              );

              if (ratio !== false) {
                /**
                 * @type {VideoMetadata}
                 */
                const metadata = {
                  id: row.id.toString(),
                  title: row.title || row.message,
                  source: row.video.source,
                  thumbnail: row.video.source.replace(
                    "original",
                    "thumbnail.jpg",
                  ),
                  ratio: ratio,
                };

                await db.data.ratios.push(metadata);

                await db.write();

                videoMeta = metadata;
              }
            }
          }

          const id = Math.random().toString(36).substring(2, 18);

          let newsMeta = db.data.news.find(
            (r) => r.id.toString() === row.id.toString(),
          );

          if (!newsMeta) {
            console.log(
              new Date().toISOString(),
              ": ",
              "generating list_title",
            );
            const list_title = (
              await openai.chat.completions.create({
                model: "gpt-4o-mini",
                store: true,
                messages: [
                  {
                    role: "system",
                    content:
                      "Sen bir başlık kısaltma uzmanısın. Sana verilen başlığı 60 karakterin altında olacak şekilde yeniden yaz. Başlığın tarzını ve mesajını koru. Sadece kısaltılmış başlığı döndür, başka açıklama yapma.",
                  },
                  {
                    role: "user",
                    content: row.title || row.message,
                  },
                ],
              })
            ).choices[0].message.content;

            console.log(new Date().toISOString(), ": ", "generated list_title");

            newsMeta = {
              id: row.id.toString(),
              title: row.title || row.message,
              list_title,
            };

            await db.data.news.push(newsMeta);

            await db.write();
          }

          const list_title =
            (row.title || row.message).length <= 60
              ? row.title || row.message
              : newsMeta.list_title;

          if (
            brand.slug === "infografik" &&
            row.images &&
            row.images.length > 0
          ) {
            console.info(
              new Date().toISOString(),
              ": ",
              "creating infographics",
            );

            await createInfographics({
              id,
              slug: `${slugify(row.slug || row.title || list_title)
                .toLowerCase()
                .slice(0, 239)}-${id}`,
              title: row.title || list_title,
              description: row.message,
              created_at: row.created_at,
              updated_at: row.updated_at,
              isActive: true,
              seo: row.seo,
              image_url: row.images[0]?.url,
              import_id: row.id,
              order: "0",
            });
          }

          if (row.content_data) {
            const parsedContent = parser.parse(row.content_data);

            /**
             * @type {News}
             */
            const newsBody = {
              id,
              slug: `${slugify(row.slug || row.title || list_title)
                .toLowerCase()
                .slice(0, 239)}-${id}`,
              title: cleanTitle(row.title || list_title),
              list_title,
              description: row.message,
              content: parsedContent,
              brand_id: brand.id,
              seo: row.seo,
              status: "published",
              is_premium: row.premium,
              thumbnails: {
                original: row.images[0]?.url,
                "3x2": row.images[0]?.url,
                "4x3": row.images[0]?.url,
                "9x16": row.images[0]?.url,
              },
              number_of_views: row.stat_views,
              import_id: row.id,
              published_at: row.published_at,
              created_by: newDbEditor.id,
              created_at: row.created_at,
              updated_at: row.updated_at,
              type: "news",
            };

            /**
             * @type { Columns | undefined }
             */
            let columnBody;

            /**
             * @type { Array<"Taceddin Kutay" | "Cüneyt Polat" | "Yusuf Alabarda" | "Mehmet Kancı"> }
             */
            const columnEditors = [
              "Taceddin Kutay",
              "Cüneyt Polat",
              "Yusuf Alabarda",
              "Mehmet Kancı",
            ];

            if (brand.slug === "yakin-plan") {
              if (columnEditors.includes(newDbEditor.fullname)) {
                let writerId;

                const writer = writers.find(
                  (wr) => wr.fullname === newDbEditor.fullname,
                );

                writerId = writer?.id;

                if (!writerId) {
                  const newWriter = await createWriter(newDbEditor);

                  writers.push(newWriter);

                  writerId = newWriter.id;
                }

                columnBody = {
                  id,
                  title: row.title || list_title,
                  description: row.message,
                  content: parsedContent,
                  slug: `${slugify(row.slug || row.title || list_title).toLowerCase()}-${id}`,
                  brand_id: brand.id,
                  seo: row.seo,
                  status: "published",
                  is_active: true,
                  thumbnails: {
                    original: row.images[0]?.url,
                    "3x2": row.images[0]?.url,
                    "4x3": row.images[0]?.url,
                    "9x16": row.images[0]?.url,
                  },
                  number_of_views: row.stat_views,
                  import_id: row.id,
                  writer_id: writerId,

                  createdAt: row.created_at,
                  updatedAt: row.updated_at,
                };
              }
            }

            if (columnBody) {
              console.info(new Date().toISOString(), ": ", "creating column");
              await createColumn(columnBody);
            } else {
              console.info(new Date().toISOString(), ": ", "creating news");
              await createNews(newsBody, newDbEditor.id);
            }

            return;
          }

          if (!videoMeta) {
            return;
          }

          if (videoMeta.ratio === "9:16") {
            console.info(new Date().toISOString(), ": ", "creating shorts");
            await createShorts({
              id,
              slug: `${slugify(row.slug || row.title || list_title).toLowerCase()}-${id}`,
              title: row.title || list_title,
              description: row.message,
              brand_id: brand.id,
              status: "published",
              url: row.video.playlist,
              thumbnails: {
                original: row.images[0]?.url || videoMeta.thumbnail,
                "3x2": row.images[0]?.url || videoMeta.thumbnail,
                "4x3": row.images[0]?.url || videoMeta.thumbnail,
                "9x16": row.images[0]?.url || videoMeta.thumbnail,
              },
              number_of_views: row.stat_views,
              import_id: row.id,
              created_by: newDbEditor.id,
              created_at: row.created_at,
              updated_at: row.updated_at,
            });

            return;
          }

          console.info(new Date().toISOString(), ": ", "creating video news");

          /**
           * Creating video news
           */
          const exist = await createNews(
            {
              id,
              slug: `${slugify(row.slug || row.title || list_title)
                ?.toLowerCase()
                .slice(0, 239)}-${id}`,
              title: cleanTitle(row.title || list_title),
              list_title,
              description: row.message,
              brand_id: brand.id,
              seo: row.seo,
              status: "published",
              is_premium: row.premium,
              video: row.video.playlist,
              thumbnails: {
                original: row.images[0]?.url || videoMeta.thumbnail,
                "3x2": row.images[0]?.url || videoMeta.thumbnail,
                "4x3": row.images[0]?.url || videoMeta.thumbnail,
                "9x16": row.images[0]?.url || videoMeta.thumbnail,
              },
              number_of_views: row.stat_views,
              import_id: row.id,
              published_at: row.published_at,
              created_by: newDbEditor.id,
              created_at: row.created_at,
              updated_at: row.updated_at,
              type: "video",
            },
            newDbEditor.id,
          );
        };

        promises.push(creationProcess());

        if (promises.length > BATCH_COUNT - 1) {
          console.info(
            new Date().toISOString(),
            ": ",
            "executing " + BATCH_COUNT,
          );
          await Promise.all(promises).catch(console.error);

          promises = [];
        }

        await Promise.all(promises).catch(console.error);
      } catch (err) {
        console.error(err);
      }
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
    if (slice.length === READ_COUNT) {
      return process();
    } else {
      return true;
    }
  };

  const res = await process();

  console.info(new Date().toISOString(), ": ", "Migration done...");

  await client.end();
};

run();
