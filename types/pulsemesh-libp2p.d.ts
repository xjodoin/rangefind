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
}): Promise<Libp2pLike>;

/**
 * Wraps a libp2p host as a MeshNetwork for exactly one MeshNode:
 * GossipSub topics for gossip, one framed request/response per stream on
 * /rangefind/pulsemesh/1/sync. Call close() when done; stopping the host
 * itself stays the caller's job.
 */
export declare function createLibp2pNetwork(host: Libp2pLike): MeshNetwork & {
  stats: { gossipIn: number; gossipOut: number; requests: number; responses: number; served: number };
  close(): Promise<void>;
};

/** Structural subset of a js-libp2p node this adapter relies on. */
export interface Libp2pLike {
  peerId: { toString(): string };
  getMultiaddrs(): Array<{ toString(): string }>;
  getPeers(): Array<{ toString(): string }>;
  getConnections(): unknown[];
  handle(protocol: string, handler: (context: unknown) => void): Promise<void> | void;
  unhandle(protocol: string): Promise<void>;
  dial(address: unknown): Promise<unknown>;
  stop(): Promise<void>;
  services: { pubsub: unknown };
}

export type { MeshNode };
