import type { MeshNetwork, MeshNode } from "./pulsemesh.js";

/**
 * Creates a libp2p host with the PulseMesh §5.1 profile: TCP transport,
 * Noise encryption, Yamux muxing, and GossipSub with message signing
 * disabled (records are self-validating) and message ids = the first 20
 * bytes of SHA-256 of the payload. Requires the optional libp2p peer
 * dependencies; throws if they are not installed.
 */
export declare function createPulseMeshHost(options?: {
  listen?: string[];
  bootstrapPeers?: string[];
  /**
   * Peers this host met last time and the *host* chose to remember
   * (threads §20.10). Dialled alongside `bootstrapPeers` — configured
   * first, deduplicated — with the same best-effort semantics. The
   * library persists nothing.
   */
  rememberedPeers?: string[];
  profile?: "browser" | "node";
  dht?: boolean | Record<string, unknown>;
}): Promise<Libp2pLike>;

/**
 * Wraps a libp2p host as a MeshNetwork for exactly one MeshNode:
 * GossipSub topics for gossip, one framed request/response per stream on
 * /rangefind/pulsemesh/1/sync. Call close() when done; stopping the host
 * itself stays the caller's job.
 */
export declare function createLibp2pNetwork(host: Libp2pLike): MeshNetwork & {
  stats: {
    gossipIn: number; gossipOut: number; requests: number; responses: number; served: number;
    bondsSent: number; bondsReceived: number;
  };
  /**
   * §5.4: mines this host's admission bond (chunked, background-safe) and
   * presents it to every current and future peer. Resolves true when
   * minted; false when the budget or signal ended the search.
   */
  mintBond(options?: { budgetMillis?: number | null; signal?: AbortSignal | null; chunkMillis?: number }): Promise<boolean>;
  /**
   * The multiaddrs this host is actually connected to, each ending in
   * `/p2p/<peerId>` so it can be dialled again as-is (threads §20.10).
   * The seam that lets a host stop depending on a seed after first
   * contact: the library reports, the host decides what to keep.
   */
  knownPeers(): string[];
  close(): Promise<void>;
};

/** Structural subset of a js-libp2p node this adapter relies on. */
export interface Libp2pLike {
  peerId: { toString(): string };
  getMultiaddrs(): Array<{ toString(): string }>;
  getPeers(): Array<{ toString(): string }>;
  getConnections(): Array<{
    remotePeer: { toString(): string };
    /** Present on a live connection; `knownPeers()` skips one without it. */
    remoteAddr?: { toString(): string };
    newStream(protocol: string): Promise<unknown>;
  }>;
  handle(protocol: string, handler: (context: unknown) => void): Promise<void> | void;
  unhandle(protocol: string): Promise<void>;
  dial(address: unknown): Promise<unknown>;
  stop(): Promise<void>;
  services: { pubsub: unknown };
}

export type { MeshNode };
