import { docValueFields } from "./codec.js";

export const FIELD_ROW_PIPELINE_FORMAT = "rffieldrows-v1";

export function createFieldRowPipeline(store, config, total) {
  const fields = docValueFields(config, store);
  const rows = {
    format: FIELD_ROW_PIPELINE_FORMAT,
    source: store?.format || "object",
    total,
    fields,
    fieldCount: fields.length,
    facetFields: fields.filter(field => field.kind === "facet").length,
    numericFields: fields.filter(field => field.kind === "number").length,
    booleanFields: fields.filter(field => field.kind === "boolean").length,
    dateFields: fields.filter(field => field.type === "date").length,
    get(name, doc) {
      if (store && typeof store.get === "function") return store.get(name, doc);
      return (store?.[name] || [])[doc];
    },
    chunk(name, start, count) {
      if (store && typeof store.chunk === "function") return store.chunk(name, start, count);
      return (store?.[name] || []).slice(start, start + count);
    },
    descriptor() {
      return {
        format: FIELD_ROW_PIPELINE_FORMAT,
        source: store?.format || "object",
        total,
        fields: fields.map(field => ({
          name: field.name,
          kind: field.kind,
          type: field.type,
          words: field.words || 0,
          bytesPerDoc: field.bytesPerDoc || 0
        }))
      };
    }
  };
  rows._fields = fields;
  rows._dicts = store?._dicts || {};
  return rows;
}
