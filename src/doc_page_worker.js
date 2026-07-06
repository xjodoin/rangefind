import { closeSync, openSync, readSync } from "node:fs";
import { parentPort } from "node:worker_threads";
import { gunzipSync, gzipSync } from "node:zlib";
import { encodeDocPageColumns } from "./doc_pages.js";

const DOC_SPOOL_ENTRY_BYTES = 24;

let state = null;

function readEntry(fd, index) {
  const buffer = Buffer.allocUnsafe(DOC_SPOOL_ENTRY_BYTES);
  const bytesRead = readSync(fd, buffer, 0, buffer.length, index * DOC_SPOOL_ENTRY_BYTES);
  if (bytesRead !== buffer.length) throw new Error(`Rangefind doc page worker is missing spool entry ${index}.`);
  return {
    offset: Number(buffer.readBigUInt64LE(0)),
    length: Number(buffer.readBigUInt64LE(8)),
    logicalLength: Number(buffer.readBigUInt64LE(16))
  };
}

function readPayloadDoc(index) {
  const entry = readEntry(state.payloadEntryFd, index);
  const compressed = Buffer.allocUnsafe(entry.length);
  const bytesRead = readSync(state.payloadFd, compressed, 0, entry.length, entry.offset);
  if (bytesRead !== entry.length) throw new Error(`Rangefind doc page worker payload spool ended before document ${index}.`);
  return JSON.parse(gunzipSync(compressed).toString("utf8"));
}

function initState(message) {
  state = {
    payloadFd: message.payloadPath ? openSync(message.payloadPath, "r") : null,
    payloadEntryFd: message.payloadEntryPath ? openSync(message.payloadEntryPath, "r") : null,
    fields: message.fields || [],
    gzipLevel: Math.max(0, Math.floor(Number(message.gzipLevel ?? 6)))
  };
}

function gzipItems(message) {
  return {
    id: message.id,
    items: message.items.map(item => ({
      key: item.key,
      logicalLength: item.bytes.length,
      compressed: gzipSync(Buffer.isBuffer(item.bytes) ? item.bytes : Buffer.from(item.bytes.buffer, item.bytes.byteOffset, item.bytes.byteLength), { level: state.gzipLevel })
    }))
  };
}

function buildRankPages(message) {
  const pages = message.pages.map(page => {
    const docs = new Array(page.docIds.length);
    for (let i = 0; i < page.docIds.length; i++) docs[i] = readPayloadDoc(page.docIds[i]);
    const source = encodeDocPageColumns(docs, state.fields);
    return {
      pageIndex: page.pageIndex,
      logicalLength: source.length,
      compressed: gzipSync(Buffer.from(source.buffer, source.byteOffset, source.length), { level: state.gzipLevel })
    };
  });
  return { id: message.id, pages };
}

parentPort.on("message", (message) => {
  try {
    if (message.type === "init") {
      initState(message);
      parentPort.postMessage({ id: message.id });
      return;
    }
    if (message.type === "close") {
      if (state?.payloadFd != null) closeSync(state.payloadFd);
      if (state?.payloadEntryFd != null) closeSync(state.payloadEntryFd);
      parentPort.postMessage({ id: message.id });
      return;
    }
    parentPort.postMessage(message.kind === "rank-pages" ? buildRankPages(message) : gzipItems(message));
  } catch (error) {
    parentPort.postMessage({
      id: message.id,
      error: error?.stack || error?.message || String(error)
    });
  }
});
