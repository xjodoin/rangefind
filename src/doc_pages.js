import { DOC_PAGE_POINTER_MAGIC } from "./binary.js";
import { buildDocPointerTable, decodeDocPointerRecord, parseDocPointerHeader, parseDocPointerPage } from "./doc_pointers.js";

export const DOC_PAGE_FORMAT = "rfdocpage-v1";
export const DOC_PAGE_POINTER_FORMAT = "rfdocpageptr-v1";

export function buildDocPagePointerTable(entries, packIndexes) {
  return buildDocPointerTable(entries, packIndexes, {
    format: DOC_PAGE_POINTER_FORMAT,
    magic: DOC_PAGE_POINTER_MAGIC
  });
}

export function parseDocPagePointerHeader(buffer) {
  return parseDocPointerHeader(buffer, {
    format: DOC_PAGE_POINTER_FORMAT,
    magic: DOC_PAGE_POINTER_MAGIC
  });
}

export function parseDocPagePointerPage(buffer, options = {}) {
  return parseDocPointerPage(buffer, {
    ...options,
    format: DOC_PAGE_POINTER_FORMAT,
    magic: DOC_PAGE_POINTER_MAGIC
  });
}

export function decodeDocPagePointerRecord(buffer, offset, meta, packTable = []) {
  return decodeDocPointerRecord(buffer, offset, meta, packTable);
}
