export function isPostingRowBuffer(rows) {
  return !!rows && rows.docs instanceof Int32Array && rows.scores instanceof Int32Array;
}

export function postingRowCount(rows) {
  if (!rows) return 0;
  if (isPostingRowBuffer(rows)) return Math.max(0, Math.floor(Number(rows.length) || 0));
  return rows.length || 0;
}

export function postingRowDoc(rows, index) {
  return isPostingRowBuffer(rows) ? rows.docs[index] : rows[index]?.[0];
}

export function postingRowScore(rows, index) {
  return isPostingRowBuffer(rows) ? rows.scores[index] : rows[index]?.[1];
}

export function createPostingRowBuffer(capacity = 0) {
  const size = Math.max(0, Math.floor(Number(capacity) || 0));
  return {
    docs: new Int32Array(size),
    scores: new Int32Array(size),
    length: 0
  };
}

export function ensurePostingRowCapacity(rows, capacity) {
  const needed = Math.max(0, Math.floor(Number(capacity) || 0));
  if (rows.docs.length >= needed) return rows;
  let next = Math.max(16, rows.docs.length || 0);
  while (next < needed) next *= 2;
  const docs = new Int32Array(next);
  const scores = new Int32Array(next);
  docs.set(rows.docs.subarray(0, rows.length));
  scores.set(rows.scores.subarray(0, rows.length));
  rows.docs = docs;
  rows.scores = scores;
  return rows;
}

export function resetPostingRows(rows) {
  rows.length = 0;
  return rows;
}

export function appendPostingRow(rows, doc, score) {
  ensurePostingRowCapacity(rows, rows.length + 1);
  rows.docs[rows.length] = doc;
  rows.scores[rows.length] = score;
  rows.length++;
  return rows;
}

export function copyPostingRows(rows) {
  const count = postingRowCount(rows);
  const docs = new Int32Array(count);
  const scores = new Int32Array(count);
  if (isPostingRowBuffer(rows)) {
    docs.set(rows.docs.subarray(0, count));
    scores.set(rows.scores.subarray(0, count));
  } else {
    for (let i = 0; i < count; i++) {
      docs[i] = postingRowDoc(rows, i);
      scores[i] = postingRowScore(rows, i);
    }
  }
  return { docs, scores, length: count };
}
