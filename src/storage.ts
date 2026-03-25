/**
 * Abstract storage interface — bring your own backend.
 * Compatible with Cloudflare R2, S3, local filesystem, or in-memory.
 */
export interface StorageObject {
  arrayBuffer(): Promise<ArrayBuffer>;
}

export interface Storage {
  get(key: string, options?: { range?: { offset: number; length: number } }): Promise<StorageObject | null>;
}
