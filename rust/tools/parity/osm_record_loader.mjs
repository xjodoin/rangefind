// Module resolve hook for osm_record_hooks.mjs.
const QUERY_SUFFIX = "/src/integrations/osm/query.js";
const WRAPPER = new URL("./osm_record_wrapper.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  const resolved = await nextResolve(specifier, context);
  const parent = context.parentURL || "";
  if (resolved.url.endsWith(QUERY_SUFFIX) && parent.includes("/test/")) {
    return { ...resolved, url: WRAPPER, shortCircuit: true };
  }
  return resolved;
}
