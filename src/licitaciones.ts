/**
 * Licitaciones search engine.
 *
 * Supports two formats:
 *   - Sharded: manifest.json + per-month .bin/.idx/.filter files
 *   - Monolithic (legacy): single .bin + .idx + .filter
 *
 * Cached in memory: manifest, per-shard .idx + .filter, global search .idx
 */

import type { Storage } from './storage.js';

const BASE_NAME = 'pe-oece-tenders';
const CURRENT_BASE_NAME = 'pe-oece-tenders-current';
const SHARD_ID_MULTIPLIER = 1_000_000;

const OECE_URL = 'https://contratacionesabiertas.oece.gob.pe/proceso/';

const SEARCH_HEADER = 16;
const SEARCH_ENTRY = 32;
const TERM_LEN = 22;
const MAX_PREFIX_TERMS = 20;
const MAX_PREFIX_SCAN = 5000;
const MAX_RESULTS = 100;
const CANDIDATE_POOL = 200;

const CATEGORY_IDS: Record<string, number> = { goods: 1, services: 2, works: 3 };

const STOP_WORDS = new Set([
  'de', 'del', 'la', 'el', 'las', 'los', 'en', 'con', 'por',
  'para', 'al', 'un', 'una', 'su', 'sus', 'y', 'e', 'o',
]);

interface ShardCache {
  month: string;
  records: number;
  idx: ArrayBuffer;
  filter: ArrayBuffer;
}

interface LicitacionesCache {
  mode: 'sharded' | 'monolithic';
  shards: ShardCache[];
  monoIdx?: ArrayBuffer;
  monoFilter?: ArrayBuffer;
  monoIdxEntrySize?: number;
  searchIdx: ArrayBuffer;
  totalRecords: number;
  termCount: number;
  totalDocs: number;
  currentSearchIdx?: ArrayBuffer;
  currentTermCount?: number;
  currentTotalDocs?: number;
  currentSearchMonth?: string;
}

let cachedData: LicitacionesCache | null = null;

async function loadCache(storage: Storage): Promise<LicitacionesCache> {
  if (cachedData) return cachedData;

  const manifestObj = await storage.get('pe-oece-manifest.json');
  if (manifestObj) {
    cachedData = await loadShardedCache(storage, manifestObj);
  } else {
    cachedData = await loadMonolithicCache(storage);
  }

  return cachedData;
}

async function loadShardedCache(storage: Storage, manifestObj: { arrayBuffer(): Promise<ArrayBuffer> }): Promise<LicitacionesCache> {
  const manifest = JSON.parse(new TextDecoder().decode(new Uint8Array(await manifestObj.arrayBuffer())));
  const shardList: { month: string; records: number }[] = manifest.shards || [];

  const searchObj = await storage.get(`${BASE_NAME}-search.idx`);
  if (!searchObj) throw new Error('Search index not found');
  const searchIdx = await searchObj.arrayBuffer();

  const shardPromises = shardList.map(async (s) => {
    const [idxObj, filterObj] = await Promise.all([
      storage.get(`${BASE_NAME}-${s.month}.idx`),
      storage.get(`${BASE_NAME}-${s.month}-filter.bin`),
    ]);
    return {
      month: s.month,
      records: s.records,
      idx: idxObj ? await idxObj.arrayBuffer() : new ArrayBuffer(12),
      filter: filterObj ? await filterObj.arrayBuffer() : new ArrayBuffer(0),
    };
  });

  const shards = await Promise.all(shardPromises);

  const searchView = new DataView(searchIdx);
  const termCount = searchView.getUint32(4, true);
  const totalRecords = manifest.totalRecords || shards.reduce((s: number, sh: ShardCache) => s + sh.records, 0);
  const totalDocs = searchView.getUint32(12, true) || totalRecords;

  const cache: LicitacionesCache = {
    mode: 'sharded',
    shards,
    searchIdx,
    totalRecords,
    termCount,
    totalDocs,
  };

  if (manifest.currentSearchMonth) {
    const currentSearchObj = await storage.get(`${CURRENT_BASE_NAME}-search.idx`);
    if (currentSearchObj) {
      cache.currentSearchIdx = await currentSearchObj.arrayBuffer();
      const csView = new DataView(cache.currentSearchIdx);
      cache.currentTermCount = csView.getUint32(4, true);
      cache.currentTotalDocs = csView.getUint32(12, true);
      cache.currentSearchMonth = manifest.currentSearchMonth;
    }
  }

  return cache;
}

async function loadMonolithicCache(storage: Storage): Promise<LicitacionesCache> {
  const [recObj, searchObj, filterObj] = await Promise.all([
    storage.get(`${BASE_NAME}.idx`),
    storage.get(`${BASE_NAME}-search.idx`),
    storage.get(`${BASE_NAME}-filter.bin`),
  ]);

  if (!recObj || !searchObj || !filterObj) {
    throw new Error('Licitaciones index files not found');
  }

  const monoIdx = await recObj.arrayBuffer();
  const searchIdx = await searchObj.arrayBuffer();
  const monoFilter = await filterObj.arrayBuffer();

  const recView = new DataView(monoIdx);
  const totalRecords = recView.getUint32(4, true);
  const magic = String.fromCharCode(recView.getUint8(0), recView.getUint8(1), recView.getUint8(2), recView.getUint8(3));
  const monoIdxEntrySize = magic === 'LOC2' ? 12 : 8;

  const searchView = new DataView(searchIdx);
  const termCount = searchView.getUint32(4, true);
  const totalDocs = searchView.getUint32(12, true) || totalRecords;

  return {
    mode: 'monolithic',
    shards: [],
    monoIdx,
    monoFilter,
    monoIdxEntrySize,
    searchIdx,
    totalRecords,
    termCount,
    totalDocs,
  };
}

function readTerm(view: DataView, offset: number): string {
  const bytes = new Uint8Array(view.buffer, view.byteOffset + offset, TERM_LEN);
  let end = TERM_LEN;
  for (let i = 0; i < TERM_LEN; i++) {
    if (bytes[i] === 0) { end = i; break; }
  }
  return new TextDecoder().decode(bytes.subarray(0, end));
}

function lowerBound(buf: ArrayBuffer, termCount: number, prefix: string): number {
  const view = new DataView(buf);
  let lo = 0, hi = termCount;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (readTerm(view, SEARCH_HEADER + mid * SEARCH_ENTRY) < prefix) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function exactMatch(buf: ArrayBuffer, termCount: number, term: string): { shard: number; offset: number; count: number } | null {
  const view = new DataView(buf);
  let lo = 0, hi = termCount - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const off = SEARCH_HEADER + mid * SEARCH_ENTRY;
    const t = readTerm(view, off);
    if (t === term) return {
      shard: view.getUint8(off + TERM_LEN),
      offset: view.getUint32(off + TERM_LEN + 2, true),
      count: view.getUint32(off + TERM_LEN + 6, true),
    };
    if (t < term) lo = mid + 1; else hi = mid - 1;
  }
  return null;
}

interface ResolvedToken {
  entries: { shard: number; offset: number; count: number }[];
  totalDf: number;
}

function resolveToken(buf: ArrayBuffer, termCount: number, token: string, isLast: boolean): ResolvedToken {
  const exact = exactMatch(buf, termCount, token);
  if (exact && (!isLast || exact.count >= 100)) {
    return { entries: [exact], totalDf: exact.count };
  }

  const prefixBytes = new TextEncoder().encode(token);
  const raw = new Uint8Array(buf);
  const view = new DataView(buf);
  const start = lowerBound(buf, termCount, token);
  const entries: { shard: number; offset: number; count: number }[] = [];
  let totalDf = 0;

  for (let i = start; i < termCount && entries.length < MAX_PREFIX_SCAN; i++) {
    const off = SEARCH_HEADER + i * SEARCH_ENTRY;
    let match = true;
    for (let b = 0; b < prefixBytes.length; b++) {
      if (raw[off + b] !== prefixBytes[b]) { match = false; break; }
    }
    if (!match) break;

    const count = view.getUint32(off + TERM_LEN + 6, true);
    entries.push({
      shard: view.getUint8(off + TERM_LEN),
      offset: view.getUint32(off + TERM_LEN + 2, true),
      count,
    });
    totalDf += count;
  }

  if (entries.length > MAX_PREFIX_TERMS) {
    entries.sort((a, b) => b.count - a.count);
    entries.length = MAX_PREFIX_TERMS;
    totalDf = entries.reduce((s, e) => s + e.count, 0);
  }

  if (exact && entries.length === 0) return { entries: [exact], totalDf: exact.count };
  return { entries, totalDf };
}

function tokenizeLicitaciones(text: string): string[] {
  return [...new Set(
    text.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
      .split(/[^a-z0-9]+/).filter(w => w.length >= 2)
      .filter(w => !STOP_WORDS.has(w))
  )];
}

async function fetchPostings(storage: Storage, entry: { shard: number; offset: number; count: number }, prefix = BASE_NAME): Promise<BigUint64Array> {
  const key = `${prefix}-search-${entry.shard}.dat`;
  const byteLen = Math.min(entry.count, 50_000) * 8;
  const obj = await storage.get(key, { range: { offset: entry.offset, length: byteLen } });
  if (!obj) return new BigUint64Array(0);
  return new BigUint64Array(await obj.arrayBuffer());
}

function intersect(lists: BigUint64Array[], limit: number): number[] {
  if (lists.length === 0) return [];
  lists.sort((a, b) => a.length - b.length);

  let current: bigint[] = Array.from(lists[0]);
  for (let i = 1; i < lists.length; i++) {
    const other = lists[i];
    const next: bigint[] = [];
    let oi = 0;
    for (const val of current) {
      while (oi < other.length && other[oi] < val) oi++;
      if (oi < other.length && other[oi] === val) next.push(val);
      if (next.length >= limit) break;
    }
    current = next;
    if (current.length === 0) break;
  }

  return current.slice(0, limit).map(n => Number(n));
}

function unionPostings(lists: BigUint64Array[]): BigUint64Array {
  if (lists.length === 1) return lists[0];
  const merged = new Set<bigint>();
  for (const list of lists) {
    for (let i = 0; i < list.length; i++) merged.add(list[i]);
  }
  return new BigUint64Array([...merged].sort((a, b) => (a < b ? -1 : 1)));
}

function decodeRecord(data: ArrayBuffer): any | null {
  const view = new DataView(data);
  if (data.byteLength < 4) return null;

  const jsonLen = view.getUint32(0, true);
  if (4 + jsonLen > data.byteLength) return null;

  try {
    const json = new TextDecoder().decode(new Uint8Array(data, 4, jsonLen));
    const obj = JSON.parse(json);
    obj.url = OECE_URL + obj.ocid;
    return obj;
  } catch {
    return null;
  }
}

function readFilter(cache: LicitacionesCache, compositeId: number): { cat: number; amount: number } | null {
  if (cache.mode === 'sharded') {
    const shardIdx = Math.floor(compositeId / SHARD_ID_MULTIPLIER);
    const localIdx = compositeId % SHARD_ID_MULTIPLIER;
    if (shardIdx >= cache.shards.length) return null;
    const shard = cache.shards[shardIdx];
    const off = localIdx * 8;
    if (off + 8 > shard.filter.byteLength) return null;
    const view = new DataView(shard.filter);
    return { cat: view.getUint8(off), amount: view.getFloat32(off + 4, true) };
  } else {
    const off = compositeId * 8;
    if (off + 8 > cache.monoFilter!.byteLength) return null;
    const view = new DataView(cache.monoFilter!);
    return { cat: view.getUint8(off), amount: view.getFloat32(off + 4, true) };
  }
}

function resolveRecordLocation(cache: LicitacionesCache, compositeId: number): { key: string; offset: number; length: number } | null {
  if (cache.mode === 'sharded') {
    const shardIdx = Math.floor(compositeId / SHARD_ID_MULTIPLIER);
    const localIdx = compositeId % SHARD_ID_MULTIPLIER;
    if (shardIdx >= cache.shards.length) return null;
    const shard = cache.shards[shardIdx];
    const idxView = new DataView(shard.idx);
    const entryOff = 12 + localIdx * 8;
    if (entryOff + 8 > shard.idx.byteLength) return null;
    return {
      key: `${BASE_NAME}-${shard.month}.bin`,
      offset: idxView.getUint32(entryOff, true),
      length: idxView.getUint32(entryOff + 4, true),
    };
  } else {
    const entrySize = cache.monoIdxEntrySize!;
    const idxView = new DataView(cache.monoIdx!);
    const entryOff = 12 + compositeId * entrySize;
    if (entryOff + entrySize > cache.monoIdx!.byteLength) return null;
    let offset: number;
    let length: number;
    if (entrySize === 12) {
      const lo = idxView.getUint32(entryOff, true);
      const hi = idxView.getUint32(entryOff + 4, true);
      offset = lo + hi * 0x100000000;
      length = idxView.getUint32(entryOff + 8, true);
    } else {
      offset = idxView.getUint32(entryOff, true);
      length = idxView.getUint32(entryOff + 4, true);
    }
    return { key: `${BASE_NAME}.bin`, offset, length };
  }
}

function allCompositeIds(cache: LicitacionesCache): number[] {
  if (cache.mode === 'sharded') {
    const ids: number[] = [];
    for (let s = cache.shards.length - 1; s >= 0; s--) {
      for (let i = 0; i < cache.shards[s].records; i++) {
        ids.push(s * SHARD_ID_MULTIPLIER + i);
      }
    }
    return ids;
  } else {
    return Array.from({ length: cache.totalRecords }, (_, i) => i);
  }
}

export interface LicitacionesQuery {
  q?: string;
  category?: string;
  min_amount?: number;
  max_amount?: number;
  buyer?: string;
  method?: string;
  status?: string;
  limit?: number;
}

function applyFilterIndex(
  cache: LicitacionesCache,
  candidates: number[] | null,
  query: LicitacionesQuery,
  maxResults: number,
): number[] {
  const catFilter = query.category ? CATEGORY_IDS[query.category.toLowerCase()] : undefined;
  const matched: number[] = [];
  const indices = candidates || allCompositeIds(cache);

  for (const id of indices) {
    if (matched.length >= maxResults) break;
    const f = readFilter(cache, id);
    if (!f) continue;
    if (catFilter !== undefined && f.cat !== catFilter) continue;
    if (query.min_amount !== undefined && f.amount < query.min_amount) continue;
    if (query.max_amount !== undefined && f.amount > query.max_amount) continue;
    matched.push(id);
  }

  return matched;
}

async function searchIndex(
  storage: Storage,
  searchIdx: ArrayBuffer,
  termCount: number,
  tokens: string[],
  datPrefix: string,
): Promise<number[]> {
  const resolved: ResolvedToken[] = [];
  for (let i = 0; i < tokens.length; i++) {
    const r = resolveToken(searchIdx, termCount, tokens[i], i === tokens.length - 1);
    if (r.entries.length === 0) return [];
    resolved.push(r);
  }

  const allEntries: { tokenIdx: number; entry: { shard: number; offset: number; count: number } }[] = [];
  for (let i = 0; i < resolved.length; i++) {
    for (const entry of resolved[i].entries) {
      allEntries.push({ tokenIdx: i, entry });
    }
  }

  const allPostings = await Promise.all(
    allEntries.map(({ entry }) => fetchPostings(storage, entry, datPrefix)),
  );

  const tokenPostings: BigUint64Array[] = [];
  for (let i = 0; i < resolved.length; i++) {
    const lists = allEntries
      .map((e, idx) => e.tokenIdx === i ? allPostings[idx] : null)
      .filter((l): l is BigUint64Array => l !== null);
    tokenPostings.push(lists.length === 1 ? lists[0] : unionPostings(lists));
  }

  return intersect(tokenPostings, CANDIDATE_POOL);
}

export async function searchLicitaciones(
  storage: Storage,
  query: LicitacionesQuery,
): Promise<Record<string, string>[]> {
  const cache = await loadCache(storage);
  const maxResults = Math.min(query.limit || 20, MAX_RESULTS);

  let candidateIndices: number[] | null = null;

  if (query.q) {
    const tokens = tokenizeLicitaciones(query.q);
    if (tokens.length === 0) return [];

    const historicalCandidates = await searchIndex(storage, cache.searchIdx, cache.termCount, tokens, BASE_NAME);

    let currentCandidates: number[] = [];
    if (cache.currentSearchIdx && cache.currentTermCount) {
      currentCandidates = await searchIndex(storage, cache.currentSearchIdx, cache.currentTermCount, tokens, CURRENT_BASE_NAME);
    }

    candidateIndices = [...currentCandidates, ...historicalCandidates];
    if (candidateIndices.length === 0) return [];
  }

  const hasFilterIndexFilters = query.category || query.min_amount !== undefined || query.max_amount !== undefined;
  let matchedIndices: number[];

  if (hasFilterIndexFilters) {
    matchedIndices = applyFilterIndex(cache, candidateIndices, query, CANDIDATE_POOL);
  } else if (candidateIndices) {
    matchedIndices = candidateIndices.slice(0, CANDIDATE_POOL);
  } else {
    matchedIndices = allCompositeIds(cache).slice(0, CANDIDATE_POOL);
  }

  if (matchedIndices.length === 0) return [];

  const BATCH = 50;
  const results: Record<string, string>[] = [];

  for (let b = 0; b < matchedIndices.length && results.length < maxResults; b += BATCH) {
    const batch = matchedIndices.slice(b, b + BATCH);

    const fetched = await Promise.all(
      batch.map(async (recId) => {
        const loc = resolveRecordLocation(cache, recId);
        if (!loc) return null;
        const obj = await storage.get(loc.key, {
          range: { offset: loc.offset, length: loc.length },
        });
        if (!obj) return null;
        return decodeRecord(await obj.arrayBuffer());
      }),
    );

    for (const record of fetched) {
      if (!record) continue;
      const buyerName = record.buyer?.name || '';
      const methodDetail = record.tender?.procurementMethodDetails || '';
      const status = record.tender?.items?.[0]?.statusDetails || record.tender?.items?.[0]?.status || (record.awards?.length > 0 ? 'ADJUDICADO' : '');
      if (query.buyer && !buyerName.toLowerCase().includes(query.buyer.toLowerCase())) continue;
      if (query.method && !methodDetail.toLowerCase().includes(query.method.toLowerCase())) continue;
      if (query.status && status.toUpperCase() !== query.status.toUpperCase()) continue;
      results.push(record);
      if (results.length >= maxResults) break;
    }
  }

  return results;
}

export async function licitacionesInfo(storage: Storage): Promise<{ records: number } | null> {
  try {
    const cache = await loadCache(storage);
    return { records: cache.totalRecords };
  } catch {
    return null;
  }
}

export function clearLicitacionesCache(): void {
  cachedData = null;
}
