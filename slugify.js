import orSlugify from "slugify";

const target = `Fenerbahçe'nin yeni transferi Diego Carlos kimdir?`;

export const slugify = (...args) => {
  if (!args[1]) {
    args[1] = {
      lower: true,
      strict: true,
    };
  }

  console.log(args);
  return orSlugify(...args);
};

console.log(slugify(target));
