import pg from "pg";
import Cursor from "pg-cursor";
import edjsParser from "editorjs-parser";
import { v4 } from "uuid";
import bcryptjs from "bcryptjs";
import fs from "fs/promises";
import dotenv from "dotenv";

import { JSONFilePreset } from "lowdb/node";

import { getAspectRatio } from "./media.js";

// Read or create db.json

/**
 * @type { Record<"ratios", Array< VideoMetadata >> }
 */
const defaultData = { ratios: [] };
const db = await JSONFilePreset("metadata.json", defaultData);

dotenv.config();

const { Client } = pg;

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
const newDbConfigs = {
  host: process.env.NEW_DB_HOST,
  port: parseInt(process.env.NEW_DB_PORT),
  user: process.env.NEW_DB_USER,
  password: process.env.NEW_DB_PASSWORD,
  database: process.env.NEW_DB_DATABASE,
  // ssl: {
  //   rejectUnauthorized: false,
  //   requestCert: false,
  // },
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

  console.log("brands", brands.rows);

  fs.writeFile("brands.json", JSON.stringify(brands.rows));
};

const saveCategories = async () => {
  const categories = await client.query("SELECT * FROM category");

  console.log("categories", categories.rows);

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
  console.log("creating new editor..");
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
  console.log("creating new writer..");
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

  console.log(inserted);

  return inserted[0];
};

const getWriters = async () => {
  const { rows: writers } = await newClient.query(`SELECT * from writers`);

  return writers;
};

const createNewBrand = async (brand) => {
  console.log(`creating new brand ${brand.mapped}`);
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

  console.log("inserted", inserted[0]);

  return inserted[0];
};

/**
 * Haber oluşturma işlemini gerçekleştirir.
 *
 * @param {News} news - Haber bilgilerini içeren obje.
 * @returns {Promise<News|boolean>} - Yeni oluşturulan haber objesi veya zaten varsa `false` döner.
 */
const createNews = async (news) => {
  // console.log(`creating news ${news.id}`);
  const query = `INSERT INTO news(${Object.keys(news).join(
    ",",
  )}) VALUES (${Object.keys(news).map((item, index) => `$${index + 1}`)})`;
  const values = Object.values(news);

  const exist = await newClient.query(
    `SELECT * FROM news WHERE import_id='${news.import_id}'`,
  );

  if (exist.rows[0]) {
    //console.log("exist", exist.rows[0].id);
    return false;
  }

  const res = await newClient.query(query, values);

  return res.rows[0];
};

/**
 * Köşe yazısı oluşturma işlemini gerçekleştirir.
 *
 * @param {Columns} column - Köşe yazısı bilgilerini içeren obje.
 * @returns {Promise<void>} - Yeni oluşturulan Köşe yazısı objesi veya zaten varsa `false` döner.
 */
const createColumn = async (column) => {
  // console.log(`creating column ${column.id}`);
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
    //console.log("exist", exist.rows[0].id);
    return false;
  }

  const res = await newClient.query(query, values);

  return res.rows[0];
};

const createEditorNewsRelation = async (newsId, editorId) => {
  // console.log(`creating news (${newsId}) editor (${editorId}) relation`);
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
    //console.log("exist", exist.rows[0].id);
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

  console.log("deleted brands getting removed..");
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
      console.log("infografik..");
    }
  }

  for (let oldCategory of oldCategories) {
    const newBrand = newBrands.find((i) => i.slug === oldCategory.mapped);
    if (newBrand) {
      newCategoryMapping[oldCategory.id] = newBrand;
    } else if (!newBrand && oldCategory.slug !== "infografik") {
      newCategoryMapping[oldCategory.id] = await createNewBrand(oldCategory);
    } else {
      console.log("infografik..");
    }
  }

  /**
   * @type { Cursor<Story> }
   */
  const stories = await client.query(
    new Cursor(
      "SELECT * FROM story where content_data is not null and status='published' and author_id is not null",
    ),
  );

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

      /**
       * @type {VideoMetadata | undefined}
       */
      let videoMeta;

      if (row.video) {
        /**
         * @type {VideoMetadata | undefined}
         */
        const exist = db.data.ratios.find((r) => r.id === row.id.toString());

        if (exist) {
          videoMeta = exist;
        } else {
          const ratio = await getAspectRatio(row.video.source);

          /**
           * @type {VideoMetadata}
           */
          const metadata = {
            id: row.id.toString(),
            title: row.title || row.message,
            source: row.video.source,
            thumbnail: row.video.source.replace("original", "thumbnail.jpg"),
            ratio: ratio,
          };

          await db.data.ratios.push(metadata);

          await db.write();

          videoMeta = metadata;
        }
      }

      if (!row.images || row.images.length < 1) {
        console.log("no image..");
        continue;
      }

      const parsedContent = parser.parse(row.content_data);
      const id = Math.random().toString(36).substring(2, 18);
      const relatedAuthor = authors.find((i) => i.id === row.author_id);

      if (!relatedAuthor) {
        console.log(`author ${row.author_id} is not found`);
        continue;
      }

      const newDbEditor = relatedAuthor.mapped;
      if (
        !newDbEditor ||
        (!newBrandMapping[row.brand_id] && !newCategoryMapping[row.category_id])
      ) {
        console.log("editor brand or category does not exist");
        continue;
      } else {
        // create news
        const brand = newBrandMapping[row.brand_id]
          ? newBrandMapping[row.brand_id]
          : newCategoryMapping[row.category_id];
        if (brand) {
          /**
           * @type {News}
           */
          const newsBody = {
            id,
            slug: `${row.slug?.slice(0, 239)}-${id}`,
            title: row.title,
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
           * @type { Array<"Taceddin Kutay" | "Cüneyt Polat" | "Yusuf Alabarda"> }
           */
          const columnEditors = [
            "Taceddin Kutay",
            "Cüneyt Polat",
            "Yusuf Alabarda",
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
                title: row.title,
                description: row.message,
                content: parsedContent,
                slug: `${row.slug}-${id}`,
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

          try {
            const creationProcess = async () => {
              if (videoMeta) {
                if (videoMeta.ratio === "9:16") {
                  console.log("creating shorts");
                  await createShorts({
                    id,
                    slug: `${row.slug}-${id}`,
                    title: row.title,
                    description: row.message,
                    brand_id: brand.id,
                    status: "published",
                    url: row.video.playlist,
                    thumbnails: {
                      original: row.images[0]?.url,
                      "3x2": row.images[0]?.url,
                      "4x3": row.images[0]?.url,
                      "9x16": row.images[0]?.url,
                    },
                    number_of_views: row.stat_views,
                    import_id: row.id,
                    created_by: newDbEditor.id,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                  });
                } else {
                  console.log("creating video news");
                  /**
                   * Creating video news
                   */
                  const exist = await createNews({
                    id,
                    slug: `${row.slug.slice(0, 239)}-${id}`,
                    title: row.title,
                    description: row.message,
                    brand_id: brand.id,
                    seo: row.seo,
                    status: "published",
                    is_premium: row.premium,
                    video: row.video.playlist,
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
                    type: "video",
                  });

                  // create news <> editor relations

                  if (exist !== false) {
                    await createEditorNewsRelation(id, newDbEditor.id);
                  }
                }
              } else if (columnBody) {
                await createColumn(columnBody);
              } else {
                const exist = await createNews(newsBody);

                // create news <> editor relations

                if (exist !== false) {
                  await createEditorNewsRelation(id, newDbEditor.id);
                }
              }
            };

            promises.push(creationProcess());

            if (promises.length > BATCH_COUNT - 1) {
              console.log("executing " + BATCH_COUNT);
              await Promise.all(promises).catch(console.error);

              promises = [];
            }
          } catch (err) {
            console.error(err);
          }
        }
      }
    }
    if (promises.length) {
      console.log("executing " + promises.length);
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

  console.log("Migration done...");

  await client.end();
};

run();
