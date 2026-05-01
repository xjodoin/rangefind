export const OBJECT_STORE_FORMAT = "rfobject-v1";
export const OBJECT_POINTER_FORMAT = "rfbp-v1";
export const OBJECT_CHECKSUM_ALGORITHM = "sha256";
export const OBJECT_NAME_HASH_LENGTH = 24;

function bytesView(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return new Uint8Array(value.buffer, value.byteOffset || 0, value.byteLength);
}

function hex(bytes) {
  return [...bytes].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

export async function sha256Hex(value) {
  if (!globalThis.crypto?.subtle) {
    throw new Error("Rangefind checksum verification requires crypto.subtle.");
  }
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytesView(value));
  return hex(new Uint8Array(digest));
}

export function pointerChecksum(pointer) {
  if (!pointer?.checksum?.value) return null;
  return {
    algorithm: pointer.checksum.algorithm || OBJECT_CHECKSUM_ALGORITHM,
    value: pointer.checksum.value
  };
}

export async function verifyBlockPointer(value, pointer, label = "range object") {
  const checksum = pointerChecksum(pointer);
  if (!checksum?.value) throw new Error(`Rangefind missing checksum for ${label}.`);
  if (checksum.algorithm !== OBJECT_CHECKSUM_ALGORITHM) {
    throw new Error(`Rangefind unsupported checksum ${checksum.algorithm} for ${label}.`);
  }
  const actual = await sha256Hex(value);
  if (actual !== checksum.value) {
    throw new Error(`Rangefind checksum mismatch for ${label}.`);
  }
}
