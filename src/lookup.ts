/**
 * Binary lookup engine.
 *
 * 1. Load .idx from storage (cached in memory across calls)
 * 2. Binary search prefix index → shard + byte range
 * 3. Range request to the correct shard file
 * 4. Linear scan within chunk for exact ID match
 */

import type { SourceConfig } from './sources.js';
import type { Storage } from './storage.js';

const MAGIC = new Uint8Array([0x4c, 0x49, 0x44, 0x58]); // "LIDX"
const HEADER_SIZE = 16;
const ENTRY_SIZE = 16;

interface IndexEntry {
  prefix: number;
  shard: number;
  offset: number;
  length: number;
}

const indexCache = new Map<string, IndexEntry[]>();
const indexLoadPromises = new Map<string, Promise<IndexEntry[]>>();

function parseIndex(buf: ArrayBuffer): IndexEntry[] {
  const view = new DataView(buf);

  for (let i = 0; i < 4; i++) {
    if (new Uint8Array(buf)[i] !== MAGIC[i]) throw new Error('Invalid index magic');
  }

  const entryCount = view.getUint32(4, true);
  const entries: IndexEntry[] = new Array(entryCount);

  for (let i = 0; i < entryCount; i++) {
    const off = HEADER_SIZE + i * ENTRY_SIZE;
    entries[i] = {
      prefix: view.getUint32(off, true),
      shard: view.getUint32(off + 4, true),
      offset: view.getUint32(off + 8, true),
      length: view.getUint32(off + 12, true),
    };
  }

  return entries;
}

export async function loadIndex(storage: Storage, source: SourceConfig): Promise<IndexEntry[]> {
  const cached = indexCache.get(source.baseName);
  if (cached) return cached;

  const existing = indexLoadPromises.get(source.baseName);
  if (existing) return existing;

  const promise = (async () => {
    const obj = await storage.get(`${source.baseName}.idx`);
    if (!obj) throw new Error(`Index file not found: ${source.baseName}.idx`);
    const entries = parseIndex(await obj.arrayBuffer());
    indexCache.set(source.baseName, entries);
    indexLoadPromises.delete(source.baseName);
    return entries;
  })();
  indexLoadPromises.set(source.baseName, promise);
  return promise;
}

/** Clear cached index (useful for tests or hot-reloading) */
export function clearIndexCache(baseName?: string): void {
  if (baseName) {
    indexCache.delete(baseName);
    indexLoadPromises.delete(baseName);
  } else {
    indexCache.clear();
    indexLoadPromises.clear();
  }
}

function findPrefix(index: IndexEntry[], prefix: number): IndexEntry | null {
  let lo = 0;
  let hi = index.length - 1;

  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    if (index[mid].prefix === prefix) return index[mid];
    if (index[mid].prefix < prefix) lo = mid + 1;
    else hi = mid - 1;
  }

  return null;
}

function decodeRecord(
  data: Uint8Array,
  offset: number,
  source: SourceConfig,
): { id: string; fields: Record<string, string>; size: number } | null {
  if (offset + 2 > data.length) return null;

  const view = new DataView(data.buffer, data.byteOffset + offset);
  const recordLen = view.getUint16(0, true);
  if (offset + recordLen > data.length) return null;

  const idLen = source.primaryId.length;
  const id = String.fromCharCode(...data.subarray(offset + 2, offset + 2 + idLen));

  const fields: Record<string, string> = {};
  const decoder = new TextDecoder();
  let pos = offset + 2 + idLen;

  for (const name of source.fieldNames) {
    if (pos >= offset + recordLen) break;
    const fieldLen = data[pos++];
    fields[name] = decoder.decode(data.subarray(pos, pos + fieldLen));
    pos += fieldLen;
  }

  return { id, fields, size: recordLen };
}

export async function lookupId(
  storage: Storage,
  source: SourceConfig,
  id: string,
): Promise<Record<string, string> | null> {
  const index = await loadIndex(storage, source);
  const prefixLen = source.primaryId.prefixLength;

  const prefix = parseInt(id.substring(0, prefixLen));
  const entry = findPrefix(index, prefix);
  if (!entry) return null;

  const shardKey = `${source.baseName}-${entry.shard}.bin`;
  const obj = await storage.get(shardKey, {
    range: { offset: entry.offset, length: entry.length },
  });
  if (!obj) return null;

  const data = new Uint8Array(await obj.arrayBuffer());

  // Linear scan (records sorted by ID, ~1-2K records per prefix chunk)
  let pos = 0;
  while (pos < data.length) {
    const result = decodeRecord(data, pos, source);
    if (!result) break;

    if (result.id === id) {
      return { [source.primaryId.name]: result.id, ...result.fields };
    }
    if (result.id > id) return null;

    pos += result.size;
  }

  return null;
}
