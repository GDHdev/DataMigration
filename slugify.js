import orSlugify from "slugify";

const target = `'hucre'-operasyonunda-10-suc-orgutu-cokertildi-66-yakalandi.`;

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
