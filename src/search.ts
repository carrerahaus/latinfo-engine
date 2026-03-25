/**
 * Inverted-index search engine.
 *
 * V1 (.dat entries = uint64 ID, 8 bytes each):
 *   Requires secondary lookupId calls to fetch display fields.
 *
 * V2 (.dat entries = 110 bytes: ID + name + status, magic "LSRY"):
 *   Stores razon_social + estado inline — zero secondary lookups.
 *
 * Common path:
 *   1. Load search .idx from storage (cached globally)
 *   2. Tokenize query
 *   3. Resolve tokens: exact or prefix scan (last token = autocomplete)
 *   4. Fetch posting lists in parallel via range requests
 *   5. Union prefix-expanded lists, then merge-intersect smallest-first
 *   6. Score (IDF + phrase proximity + position + specificity)
 *   7. V1: lookupId for full records. V2: return inline fields directly.
 */

import { tokenize, isAllStopWords } from './tokenize.js';
import { lookupId } from './lookup.js';
import type { SourceConfig } from './sources.js';
import type { Storage } from './storage.js';

// V1 magic: "LSRX"
const MAGIC_V1 = new Uint8Array([0x4c, 0x53, 0x52, 0x58]);
// V2 magic: "LSRY" — inline stored fields
const MAGIC_V2 = new Uint8Array([0x4c, 0x53, 0x52, 0x59]);

const HEADER_SIZE = 16;
const ENTRY_SIZE = 32;
const TERM_LEN = 22;

const ENTRY_SIZE_V2 = 110;

const MAX_POSTINGS = 50_000;
const MAX_RESULTS = 20;
const CANDIDATE_POOL = 30;
const MAX_PREFIX_TERMS = 20;
const MAX_PREFIX_SCAN = 5000;
const PREFIX_IDF_WEIGHT = 0.8;
const DEFAULT_TOTAL_DOCS = 18_000_000;

const MAX_POSTING_CACHE_ENTRIES = 200;
interface CachedPosting { v1?: BigUint64Array; v2?: PostingEntryV2[]; ts: number }
const postingCache = new Map<string, CachedPosting>();

interface TermEntry {
  term: string;
  shard: number;
  offset: number;
  count: number;
}

interface ResolvedToken {
  entries: TermEntry[];
  isPrefix: boolean;
  totalDf: number;
}

interface SearchIndexCache {
  buf: ArrayBuffer;
  termCount: number;
  totalDocs: number;
  version: 1 | 2;
}

interface PostingEntryV2 {
  id: bigint;
  name: string;
  status: string;
}

const searchIndexCache = new Map<string, SearchIndexCache>();
const searchIndexLoadPromises = new Map<string, Promise<SearchIndexCache>>();

async function loadSearchIndex(storage: Storage, source: SourceConfig): Promise<SearchIndexCache> {
  const cached = searchIndexCache.get(source.baseName);
  if (cached) return cached;

  const existing = searchIndexLoadPromises.get(source.baseName);
  if (existing) return existing;

  const promise = (async () => {
    const key = `${source.baseName}-search.idx`;
    const obj = await storage.get(key);
    if (!obj) throw new Error(`Search index not found: ${key}`);
    const buf = await obj.arrayBuffer();

    const magic = new Uint8Array(buf, 0, 4);
    const isV2 = magic[0] === MAGIC_V2[0] && magic[1] === MAGIC_V2[1] &&
                 magic[2] === MAGIC_V2[2] && magic[3] === MAGIC_V2[3];
    const isV1 = !isV2 && magic[0] === MAGIC_V1[0] && magic[1] === MAGIC_V1[1] &&
                 magic[2] === MAGIC_V1[2] && magic[3] === MAGIC_V1[3];
    if (!isV1 && !isV2) throw new Error('Invalid search index magic');

    const view = new DataView(buf);
    const termCount = view.getUint32(4, true);
    const storedTotal = view.getUint32(12, true);
    const totalDocs = storedTotal > 0 ? storedTotal : DEFAULT_TOTAL_DOCS;
    const version: 1 | 2 = isV2 ? 2 : 1;

    const entry: SearchIndexCache = { buf, termCount, totalDocs, version };
    searchIndexCache.set(source.baseName, entry);
    searchIndexLoadPromises.delete(source.baseName);
    return entry;
  })();
  searchIndexLoadPromises.set(source.baseName, promise);
  return promise;
}

/** Clear cached search index (useful for tests) */
export function clearSearchCache(baseName?: string): void {
  if (baseName) {
    searchIndexCache.delete(baseName);
    searchIndexLoadPromises.delete(baseName);
    for (const key of postingCache.keys()) {
      if (key.startsWith(`${baseName}/`)) postingCache.delete(key);
    }
  } else {
    searchIndexCache.clear();
    searchIndexLoadPromises.clear();
    postingCache.clear();
  }
}

function readTerm(view: DataView, offset: number): string {
  let end = TERM_LEN;
  const bytes = new Uint8Array(view.buffer, view.byteOffset + offset, TERM_LEN);
  for (let i = 0; i < TERM_LEN; i++) {
    if (bytes[i] === 0) { end = i; break; }
  }
  return new TextDecoder().decode(bytes.subarray(0, end));
}

function binarySearchTerm(buf: ArrayBuffer, termCount: number, term: string): TermEntry | null {
  const view = new DataView(buf);
  let lo = 0;
  let hi = termCount - 1;

  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const entryOff = HEADER_SIZE + mid * ENTRY_SIZE;
    const midTerm = readTerm(view, entryOff);

    if (midTerm === term) {
      return {
        term: midTerm,
        shard: view.getUint8(entryOff + TERM_LEN),
        offset: view.getUint32(entryOff + TERM_LEN + 2, true),
        count: view.getUint32(entryOff + TERM_LEN + 6, true),
      };
    }
    if (midTerm < term) lo = mid + 1;
    else hi = mid - 1;
  }

  return null;
}

function lowerBound(buf: ArrayBuffer, termCount: number, prefix: string): number {
  const view = new DataView(buf);
  let lo = 0;
  let hi = termCount;

  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const entryOff = HEADER_SIZE + mid * ENTRY_SIZE;
    const midTerm = readTerm(view, entryOff);
    if (midTerm < prefix) lo = mid + 1;
    else hi = mid;
  }

  return lo;
}

function readEntryAt(buf: ArrayBuffer, index: number): TermEntry {
  const view = new DataView(buf);
  const entryOff = HEADER_SIZE + index * ENTRY_SIZE;
  return {
    term: readTerm(view, entryOff),
    shard: view.getUint8(entryOff + TERM_LEN),
    offset: view.getUint32(entryOff + TERM_LEN + 2, true),
    count: view.getUint32(entryOff + TERM_LEN + 6, true),
  };
}

function resolveToken(buf: ArrayBuffer, termCount: number, token: string, isLastToken: boolean): ResolvedToken {
  if (!isLastToken) {
    const exact = binarySearchTerm(buf, termCount, token);
    if (exact) {
      return { entries: [exact], isPrefix: false, totalDf: exact.count };
    }
  }

  if (isLastToken && token.length >= 4) {
    const exact = binarySearchTerm(buf, termCount, token);
    if (exact && exact.count >= 100) {
      return { entries: [exact], isPrefix: false, totalDf: exact.count };
    }
  }

  const prefixBytes = new TextEncoder().encode(token);
  const prefixLen = prefixBytes.length;
  const raw = new Uint8Array(buf);
  const view = new DataView(buf);
  const start = lowerBound(buf, termCount, token);
  const candidates: { idx: number; count: number }[] = [];

  for (let i = start; i < termCount && candidates.length < MAX_PREFIX_SCAN; i++) {
    const entryOff = HEADER_SIZE + i * ENTRY_SIZE;
    let match = true;
    for (let b = 0; b < prefixLen; b++) {
      if (raw[entryOff + b] !== prefixBytes[b]) { match = false; break; }
    }
    if (!match) break;
    candidates.push({ idx: i, count: view.getUint32(entryOff + TERM_LEN + 6, true) });
  }

  if (candidates.length > MAX_PREFIX_TERMS) {
    candidates.sort((a, b) => b.count - a.count);
    candidates.length = MAX_PREFIX_TERMS;
  }

  const entries = candidates.map(c => readEntryAt(buf, c.idx));
  let totalDf = 0;
  for (const e of entries) totalDf += e.count;

  return {
    entries,
    isPrefix: entries.length > 1 || (entries.length === 1 && entries[0].term !== token),
    totalDf,
  };
}

// --- V2 posting list fetch + intersection ---

function unionV2(lists: PostingEntryV2[][]): PostingEntryV2[] {
  if (lists.length === 0) return [];
  if (lists.length === 1) return lists[0];

  const indices = new Int32Array(lists.length);
  const result: PostingEntryV2[] = [];

  while (result.length < MAX_POSTINGS) {
    let minId: bigint | null = null;
    for (let k = 0; k < lists.length; k++) {
      if (indices[k] < lists[k].length) {
        const id = lists[k][indices[k]].id;
        if (minId === null || id < minId) minId = id;
      }
    }
    if (minId === null) break;

    let entry: PostingEntryV2 | null = null;
    for (let k = 0; k < lists.length; k++) {
      if (indices[k] < lists[k].length && lists[k][indices[k]].id === minId) {
        if (!entry) entry = lists[k][indices[k]];
        indices[k]++;
      }
    }

    if (entry && (result.length === 0 || result[result.length - 1].id !== minId)) {
      result.push(entry);
    }
  }

  return result;
}

function intersectV2(lists: PostingEntryV2[][], limit: number): PostingEntryV2[] {
  if (lists.length === 0) return [];
  if (lists.length === 1) return lists[0].slice(0, limit);

  lists.sort((a, b) => a.length - b.length);
  let current = lists[0];

  for (let li = 1; li < lists.length; li++) {
    const other = lists[li];
    const next: PostingEntryV2[] = [];
    let oi = 0;
    for (let ci = 0; ci < current.length && next.length < limit; ci++) {
      while (oi < other.length && other[oi].id < current[ci].id) oi++;
      if (oi < other.length && other[oi].id === current[ci].id) next.push(current[ci]);
    }
    current = next;
    if (current.length === 0) break;
  }

  return current.slice(0, limit);
}

async function fetchPostingListV2(
  storage: Storage,
  source: SourceConfig,
  entry: TermEntry,
): Promise<PostingEntryV2[]> {
  const cacheKey = `${source.baseName}/v2/${entry.shard}/${entry.offset}`;
  const hit = postingCache.get(cacheKey);
  if (hit?.v2) { hit.ts = Date.now(); return hit.v2; }

  const cappedCount = Math.min(entry.count, MAX_POSTINGS);
  const shardKey = `${source.baseName}-search-${entry.shard}.dat`;
  const byteLen = cappedCount * ENTRY_SIZE_V2;

  const obj = await storage.get(shardKey, { range: { offset: entry.offset, length: byteLen } });
  if (!obj) return [];

  const buf = await obj.arrayBuffer();
  const raw = new Uint8Array(buf);
  const decoder = new TextDecoder();
  const result: PostingEntryV2[] = [];

  for (let i = 0; i < cappedCount; i++) {
    const base = i * ENTRY_SIZE_V2;
    const view = new DataView(buf, base, ENTRY_SIZE_V2);
    const id = view.getBigUint64(0, true);
    const nameLen = raw[base + 8];
    const name = decoder.decode(raw.subarray(base + 9, base + 9 + nameLen));
    const statusLen = raw[base + 89];
    const status = decoder.decode(raw.subarray(base + 90, base + 90 + statusLen));
    result.push({ id, name, status });
  }

  evictPostingCacheIfNeeded();
  postingCache.set(cacheKey, { v2: result, ts: Date.now() });
  return result;
}

// --- V1 posting list fetch + intersection ---

function unionPostingLists(lists: BigUint64Array[]): BigUint64Array {
  if (lists.length === 0) return new BigUint64Array(0);
  if (lists.length === 1) return lists[0];

  const indices = new Int32Array(lists.length);
  const result: bigint[] = [];

  while (result.length < MAX_POSTINGS) {
    let minVal: bigint | null = null;
    for (let k = 0; k < lists.length; k++) {
      if (indices[k] < lists[k].length) {
        const val = lists[k][indices[k]];
        if (minVal === null || val < minVal) minVal = val;
      }
    }
    if (minVal === null) break;
    if (result.length === 0 || result[result.length - 1] !== minVal) result.push(minVal);
    for (let k = 0; k < lists.length; k++) {
      if (indices[k] < lists[k].length && lists[k][indices[k]] === minVal) indices[k]++;
    }
  }

  return new BigUint64Array(result);
}

function intersect(lists: BigUint64Array[], limit: number): bigint[] {
  if (lists.length === 0) return [];
  if (lists.length === 1) {
    const result: bigint[] = [];
    for (let i = 0; i < Math.min(lists[0].length, limit); i++) result.push(lists[0][i]);
    return result;
  }

  lists.sort((a, b) => a.length - b.length);
  let current: bigint[] = [];
  for (let i = 0; i < lists[0].length; i++) current.push(lists[0][i]);

  for (let li = 1; li < lists.length; li++) {
    const other = lists[li];
    const next: bigint[] = [];
    let oi = 0;
    for (let ci = 0; ci < current.length && next.length < limit; ci++) {
      while (oi < other.length && other[oi] < current[ci]) oi++;
      if (oi < other.length && other[oi] === current[ci]) next.push(current[ci]);
    }
    current = next;
    if (current.length === 0) break;
  }

  return current.slice(0, limit);
}

async function fetchPostingListV1(
  storage: Storage,
  source: SourceConfig,
  entry: TermEntry,
): Promise<BigUint64Array> {
  const cacheKey = `${source.baseName}/v1/${entry.shard}/${entry.offset}`;
  const hit = postingCache.get(cacheKey);
  if (hit?.v1) { hit.ts = Date.now(); return hit.v1; }

  const cappedCount = Math.min(entry.count, MAX_POSTINGS);
  const shardKey = `${source.baseName}-search-${entry.shard}.dat`;
  const obj = await storage.get(shardKey, { range: { offset: entry.offset, length: cappedCount * 8 } });
  if (!obj) return new BigUint64Array(0);

  const result = new BigUint64Array(await obj.arrayBuffer());
  evictPostingCacheIfNeeded();
  postingCache.set(cacheKey, { v1: result, ts: Date.now() });
  return result;
}

function evictPostingCacheIfNeeded(): void {
  if (postingCache.size < MAX_POSTING_CACHE_ENTRIES) return;
  const entries = [...postingCache.entries()].sort((a, b) => a[1].ts - b[1].ts);
  const toEvict = Math.floor(MAX_POSTING_CACHE_ENTRIES * 0.2);
  for (let i = 0; i < toEvict; i++) postingCache.delete(entries[i][0]);
}

// --- Scoring ---

function computeIdf(totalDocs: number, df: number): number {
  if (df === 0) return 0;
  return Math.log(totalDocs / df);
}

function scoreRecord(
  record: Record<string, string>,
  queryTokens: string[],
  resolvedTokens: ResolvedToken[],
  totalDocs: number,
  source: SourceConfig,
): number {
  let score = 0;

  for (const resolved of resolvedTokens) {
    const idf = computeIdf(totalDocs, resolved.totalDf);
    score += resolved.isPrefix ? idf * PREFIX_IDF_WEIGHT : idf;
  }

  const searchField = source.fieldNames[source.searchFieldIndex];
  const razonSocial = record[searchField] || '';
  const recordTokens = tokenize(razonSocial);

  if (queryTokens.length > 1 && recordTokens.length >= queryTokens.length) {
    for (let start = 0; start <= recordTokens.length - queryTokens.length; start++) {
      let match = true;
      for (let q = 0; q < queryTokens.length; q++) {
        const rw = recordTokens[start + q];
        if (q === queryTokens.length - 1) {
          if (!rw.startsWith(queryTokens[q])) { match = false; break; }
        } else {
          if (rw !== queryTokens[q]) { match = false; break; }
        }
      }
      if (match) { score += 10.0; break; }
    }
  }

  if (recordTokens.length > 0) {
    const first = recordTokens[0];
    if (first === queryTokens[0] || first.startsWith(queryTokens[0])) score += 3.0;
  }

  if (razonSocial.length > 0) score += Math.max(0, 2.0 - razonSocial.length / 50);

  return score;
}

export type SearchError = { error: string; message: string; status: number };
export type SearchResult = Record<string, string>[];

export async function searchByName(
  storage: Storage,
  source: SourceConfig,
  query: string,
): Promise<SearchResult | SearchError> {
  if (!query || query.length < 3) {
    return { error: 'invalid_query', message: 'Search query must be at least 3 characters', status: 400 };
  }

  if (isAllStopWords(query)) {
    return { error: 'query_too_broad', message: 'Query contains only common words', status: 400 };
  }

  const tokens = tokenize(query);
  if (tokens.length === 0) {
    return { error: 'invalid_query', message: 'No searchable terms in query', status: 400 };
  }

  const { buf, termCount, totalDocs, version } = await loadSearchIndex(storage, source);

  const resolvedTokens: ResolvedToken[] = [];
  for (let i = 0; i < tokens.length; i++) {
    const resolved = resolveToken(buf, termCount, tokens[i], i === tokens.length - 1);
    if (resolved.entries.length === 0) return [];
    resolvedTokens.push(resolved);
  }

  const allEntries: { tokenIdx: number; entry: TermEntry }[] = [];
  for (let i = 0; i < resolvedTokens.length; i++) {
    for (const entry of resolvedTokens[i].entries) allEntries.push({ tokenIdx: i, entry });
  }

  if (version === 2) {
    const allPostings = await Promise.all(
      allEntries.map(({ entry }) => fetchPostingListV2(storage, source, entry)),
    );

    const tokenPostings: PostingEntryV2[][] = [];
    for (let i = 0; i < resolvedTokens.length; i++) {
      const lists = allEntries
        .map((e, idx) => e.tokenIdx === i ? allPostings[idx] : null)
        .filter((l): l is PostingEntryV2[] => l !== null);
      tokenPostings.push(lists.length === 1 ? lists[0] : unionV2(lists));
    }

    const candidates = intersectV2(tokenPostings, CANDIDATE_POOL);
    if (candidates.length === 0) return [];

    const searchField = source.fieldNames[source.searchFieldIndex];
    const statusField = source.fieldNames[1] ?? null;

    const scored = candidates
      .map(e => ({
        entry: e,
        score: scoreRecord({ [searchField]: e.name }, tokens, resolvedTokens, totalDocs, source),
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, MAX_RESULTS);

    return scored.map(s => {
      const idStr = s.entry.id.toString().padStart(source.primaryId.length, '0');
      const r: Record<string, string> = { [source.primaryId.name]: idStr, [searchField]: s.entry.name };
      if (statusField && s.entry.status) r[statusField] = s.entry.status;
      return r;
    });
  }

  // --- V1 path: secondary lookups required ---
  const allPostings = await Promise.all(
    allEntries.map(({ entry }) => fetchPostingListV1(storage, source, entry)),
  );

  const tokenPostings: BigUint64Array[] = [];
  for (let i = 0; i < resolvedTokens.length; i++) {
    const lists = allEntries
      .map((e, idx) => e.tokenIdx === i ? allPostings[idx] : null)
      .filter((l): l is BigUint64Array => l !== null);
    tokenPostings.push(lists.length === 1 ? lists[0] : unionPostingLists(lists));
  }

  const matchedIds = intersect(tokenPostings, CANDIDATE_POOL);
  if (matchedIds.length === 0) return [];

  const idStrings = matchedIds.map(r => r.toString().padStart(source.primaryId.length, '0'));

  const BATCH_SIZE = 50;
  const records: (Record<string, string> | null)[] = [];
  for (let i = 0; i < idStrings.length; i += BATCH_SIZE) {
    const batch = idStrings.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(id => lookupId(storage, source, id)));
    records.push(...batchResults);
  }

  const scored: { record: Record<string, string>; score: number }[] = [];
  for (const record of records) {
    if (!record) continue;
    scored.push({ record, score: scoreRecord(record, tokens, resolvedTokens, totalDocs, source) });
  }

  scored.sort((a, b) => b.score - a.score);
  return scored.slice(0, MAX_RESULTS).map(s => s.record);
}
