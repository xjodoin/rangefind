const latin1Decoder = new TextDecoder("iso-8859-1");

function multipartBoundary(contentType) {
  const match = /(?:^|;)\s*boundary=(?:"([^"]+)"|([^;\s]+))/iu.exec(String(contentType || ""));
  return match?.[1] || match?.[2] || null;
}

export function parseMultipartByteRanges(buffer, contentType) {
  const boundary = multipartBoundary(contentType);
  if (!boundary) throw new Error("Multipart byte-range response is missing its boundary.");
  const bytes = new Uint8Array(buffer);
  const text = latin1Decoder.decode(bytes);
  const marker = `--${boundary}`;
  const parts = [];
  let cursor = 0;

  while (cursor < text.length) {
    const markerOffset = text.indexOf(marker, cursor);
    if (markerOffset < 0) break;
    let headerOffset = markerOffset + marker.length;
    if (text.startsWith("--", headerOffset)) break;
    if (text.startsWith("\r\n", headerOffset)) headerOffset += 2;
    else if (text.startsWith("\n", headerOffset)) headerOffset += 1;
    const crlfSeparator = text.indexOf("\r\n\r\n", headerOffset);
    const headerEnd = crlfSeparator >= 0 ? crlfSeparator : text.indexOf("\n\n", headerOffset);
    const separatorBytes = crlfSeparator >= 0 ? 4 : 2;
    if (headerEnd < 0) throw new Error("Multipart byte-range response has malformed headers.");
    const headers = text.slice(headerOffset, headerEnd);
    const range = /(?:^|\r?\n)content-range:\s*bytes\s+(\d+)-(\d+)\/(?:\d+|\*)/iu.exec(headers);
    if (!range) throw new Error("Multipart byte-range part is missing Content-Range.");
    const start = Number(range[1]);
    const inclusiveEnd = Number(range[2]);
    const bodyOffset = headerEnd + separatorBytes;
    const length = inclusiveEnd - start + 1;
    if (!Number.isSafeInteger(start) || length <= 0 || bodyOffset + length > bytes.byteLength) {
      throw new Error("Multipart byte-range part has an invalid Content-Range.");
    }
    parts.push({
      start,
      end: inclusiveEnd + 1,
      buffer: buffer.slice(bodyOffset, bodyOffset + length)
    });
    cursor = bodyOffset + length;
  }

  if (!parts.length) throw new Error("Multipart byte-range response contains no parts.");
  return parts;
}

export function selectMultipartByteRanges(parts, ranges) {
  return ranges.map(({ offset, length }) => {
    const end = offset + length;
    const part = parts.find(item => item.start <= offset && item.end >= end);
    if (!part) throw new Error(`Multipart byte-range response omitted bytes ${offset}-${end - 1}.`);
    return part.buffer.slice(offset - part.start, end - part.start);
  });
}
