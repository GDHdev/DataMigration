import pg from "pg";
import Cursor from "pg-cursor";
import edjsParser from "editorjs-parser";
import { v4 } from "uuid";
import bcryptjs from "bcryptjs";
import fs from "fs/promises";
import dotenv from "dotenv";

dotenv.config();

const { Client } = pg;

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

const run = async () => {
  const READ_COUNT = 1000; // slice length

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

  const stories = await client.query(
    new Cursor(
      "SELECT * FROM story where content_data is not null and status='published' and author_id is not null",
    ),
  );

  const process = async () => {
    let promises = [];
    const slice = await stories.read(READ_COUNT);
    for (let row of slice) {
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
          const newsBody = {
            id,
            slug: `${row.slug}-${id}`,
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
          };

          let columnBody;

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
              if (columnBody) {
                await createColumn(columnBody);
              } else {
                const exist = await createNews(newsBody);

                // create news <> editor relations

                if (exist) {
                  await createEditorNewsRelation(id, newDbEditor.id);
                }
              }
            };

            promises.push(creationProcess());

            if (promises.length > 19) {
              console.log("executing 20");
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
