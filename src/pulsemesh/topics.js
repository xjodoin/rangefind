// PulseMesh topic grammar, sharding, and rotation (protocol §5.2).
//
//   /rangefind/pulsemesh/1/<epochPrefix16hex>/<zoneX>/<zoneY>/<window>/<shard>
//
// The non-numeric element `t` after the version is reserved for the thread
// channel's key-derived topics; a traffic-only implementation must not
// subscribe to it, and parseTopic reports it as { reserved: true }.

import { sha256 } from "./sha256.js";
import { utf8Bytes } from "./codec.js";

export const TOPIC_PREFIX = "/rangefind/pulsemesh/1";
export const SYNC_PROTOCOL = "/rangefind/pulsemesh/1/sync";
// §5.4: one framed PMA1 per stream, no response. Its own protocol rather
// than a sync message so a peer can present its bond before it has
// anything to say.
export const BOND_PROTOCOL = "/rangefind/pulsemesh/1/bond";

export function shardOfCell(cell) {
  return sha256(utf8Bytes(`${cell.x}/${cell.y}`))[0] % 16;
}

export function topicName({ epochPrefix16hex, zoneX, zoneY, window, shard }) {
  return `${TOPIC_PREFIX}/${epochPrefix16hex}/${zoneX}/${zoneY}/${window}/${shard}`;
}

export function topicForCell({ epochPrefix16hex, cell, window }) {
  return topicName({
    epochPrefix16hex,
    zoneX: cell.x >> 6,
    zoneY: cell.y >> 6,
    window,
    shard: shardOfCell(cell)
  });
}

export function parseTopic(topic) {
  const parts = String(topic).split("/");
  // ["", "rangefind", "pulsemesh", "1", epoch16, zoneX, zoneY, window, shard]
  if (parts.length < 5 || parts[0] !== "" || parts[1] !== "rangefind" || parts[2] !== "pulsemesh" || parts[3] !== "1") {
    return null;
  }
  if (parts[4] === "t") return { reserved: true };
  if (parts.length !== 9) return null;
  const [zoneX, zoneY, window, shard] = parts.slice(5).map(Number);
  if (![zoneX, zoneY, window, shard].every(n => Number.isInteger(n) && n >= 0)) return null;
  if (shard > 15) return null;
  if (!/^[0-9a-f]{16}$/.test(parts[4])) return null;
  return { reserved: false, epochPrefix16hex: parts[4], zoneX, zoneY, window, shard };
}

/**
 * The topic windows a peer should be subscribed to at `nowMillis`: the
 * current window always, plus the next one within TOPIC_OVERLAP seconds of
 * the upcoming rotation, plus the previous one within TOPIC_OVERLAP after
 * a rotation (messages on current-or-adjacent windows are acceptable, and
 * dual-subscribing across the boundary is what makes rotation seamless).
 */
export function activeWindows(nowMillis, overlapSeconds = 30) {
  const seconds = Math.floor(nowMillis / 1000);
  const window = Math.floor(seconds / 300);
  const into = seconds - window * 300;
  const windows = [window];
  if (300 - into <= overlapSeconds) windows.push(window + 1);
  if (into < overlapSeconds && window > 0) windows.push(window - 1);
  return windows;
}

/** Whether a message on `topicWindow` is acceptable at `nowMillis` (§5.2). */
export function windowAcceptable(topicWindow, nowMillis) {
  const current = Math.floor(nowMillis / 1000 / 300);
  return Math.abs(topicWindow - current) <= 1;
}

/**
 * Full topic set for a set of z9 zones at `nowMillis`. `shards` may narrow
 * to a subset when bandwidth-constrained (§11.4); defaults to all 16.
 */
export function topicsForZones({ epochPrefix16hex, zones, nowMillis, overlapSeconds = 30, shards = null }) {
  const shardList = shards || Array.from({ length: 16 }, (_, i) => i);
  const topics = [];
  for (const window of activeWindows(nowMillis, overlapSeconds)) {
    for (const zone of zones) {
      for (const shard of shardList) {
        topics.push(topicName({ epochPrefix16hex, zoneX: zone.x, zoneY: zone.y, window, shard }));
      }
    }
  }
  return topics;
}
