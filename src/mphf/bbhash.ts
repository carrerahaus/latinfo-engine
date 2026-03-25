/**
 * BBHash — Minimal Perfect Hash Function (pure TypeScript)
 *
 * Maps N keys to [0, N) with zero collisions.
 * Space: ~3.7 bits/key (gamma=2.0)
 * Lookup: O(1) average
 *
 * Works in browser, Node.js, Cloudflare Workers — no native deps.
 *
 * Reference: Limasset et al., "Fast and Scalable Minimal Perfect Hashing
 * for Massive Key Sets" (2017)
 */

// --- MurmurHash3 32-bit ---

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function murmurhash3(data: Uint8Array, seed: number): number {
  let h = seed >>> 0;
  const len = data.length;
  const nBlocks = len >>> 2;

  for (let i = 0; i < nBlocks; i++) {
    const off = i * 4;
    let k = data[off] | (data[off + 1] << 8) | (data[off + 2] << 16) | (data[off + 3] << 24);
    k = Math.imul(k, 0xcc9e2d51);
    k = (k << 15) | (k >>> 17);
    k = Math.imul(k, 0x1b873593);
    h ^= k;
    h = (h << 13) | (h >>> 19);
    h = (Math.imul(h, 5) + 0xe6546b64) >>> 0;
  }

  const tail = nBlocks * 4;
  let k = 0;
  switch (len & 3) {
    case 3: k ^= data[tail + 2] << 16; // fallthrough
    case 2: k ^= data[tail + 1] << 8;  // fallthrough
    case 1:
      k ^= data[tail];
      k = Math.imul(k, 0xcc9e2d51);
      k = (k << 15) | (k >>> 17);
      k = Math.imul(k, 0x1b873593);
      h ^= k;
  }

  h ^= len;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

function hashString(str: string, seed: number): number {
  return murmurhash3(encoder.encode(str), seed);
}

function popcount32(x: number): number {
  x = x - ((x >>> 1) & 0x55555555);
  x = (x & 0x33333333) + ((x >>> 2) & 0x33333333);
  return (((x + (x >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24;
}

// --- Ranked Bit Vector ---

class RankedBitVector {
  readonly words: Uint32Array;
  readonly rankTable: Uint32Array;
  readonly bitCount: number;
  private _popcount = 0;

  constructor(bitCount: number) {
    this.bitCount = bitCount;
    this.words = new Uint32Array(Math.ceil(bitCount / 32));
    this.rankTable = new Uint32Array(Math.ceil(bitCount / 256) + 1);
  }

  set(i: number): void {
    this.words[i >>> 5] |= 1 << (i & 31);
  }

  get(i: number): boolean {
    return (this.words[i >>> 5] & (1 << (i & 31))) !== 0;
  }

  buildRanks(): void {
    let cumulative = 0;
    const blockCount = Math.ceil(this.bitCount / 256);
    for (let block = 0; block <= blockCount; block++) {
      this.rankTable[block] = cumulative;
      const wordStart = block * 8;
      const wordEnd = Math.min(wordStart + 8, this.words.length);
      for (let w = wordStart; w < wordEnd; w++) {
        cumulative += popcount32(this.words[w]);
      }
    }
    this._popcount = cumulative;
  }

  rank(pos: number): number {
    const blockIdx = pos >>> 8;
    let count = this.rankTable[blockIdx];
    const wordStart = blockIdx * 8;
    const targetWord = pos >>> 5;

    for (let w = wordStart; w < targetWord; w++) {
      count += popcount32(this.words[w]);
    }

    const bitInWord = pos & 31;
    if (bitInWord > 0) {
      count += popcount32(this.words[targetWord] & ((1 << bitInWord) - 1));
    }
    return count;
  }

  get popcount(): number {
    return this._popcount;
  }

  byteSize(): number {
    return 12 + this.words.byteLength + this.rankTable.byteLength;
  }

  writeTo(view: DataView, offset: number): number {
    view.setUint32(offset, this.bitCount, true);
    view.setUint32(offset + 4, this.words.length, true);
    view.setUint32(offset + 8, this.rankTable.length, true);
    offset += 12;

    for (let i = 0; i < this.words.length; i++) {
      view.setUint32(offset, this.words[i], true);
      offset += 4;
    }
    for (let i = 0; i < this.rankTable.length; i++) {
      view.setUint32(offset, this.rankTable[i], true);
      offset += 4;
    }
    return offset;
  }

  static readFrom(view: DataView, offset: number): { bv: RankedBitVector; nextOffset: number } {
    const bitCount = view.getUint32(offset, true);
    const wordCount = view.getUint32(offset + 4, true);
    const rankBlockCount = view.getUint32(offset + 8, true);
    offset += 12;

    const bv = new RankedBitVector(bitCount);

    for (let i = 0; i < wordCount; i++) {
      bv.words[i] = view.getUint32(offset, true);
      offset += 4;
    }
    for (let i = 0; i < rankBlockCount; i++) {
      bv.rankTable[i] = view.getUint32(offset, true);
      offset += 4;
    }

    let pc = 0;
    for (let i = 0; i < bv.words.length; i++) pc += popcount32(bv.words[i]);
    (bv as unknown as { _popcount: number })._popcount = pc;

    return { bv, nextOffset: offset };
  }
}

// --- BBHash ---

const GAMMA = 2.0;
const MAX_LEVELS = 30;

export class BBHash {
  private levels: RankedBitVector[];
  private levelOffsets: number[];
  private _size: number;

  private constructor(levels: RankedBitVector[], size: number) {
    this.levels = levels;
    this._size = size;
    this.levelOffsets = new Array(levels.length);
    let cum = 0;
    for (let i = 0; i < levels.length; i++) {
      this.levelOffsets[i] = cum;
      cum += levels[i].popcount;
    }
  }

  get size(): number { return this._size; }

  static build(keys: string[], gamma: number = GAMMA): BBHash {
    const n = keys.length;
    if (n === 0) return new BBHash([], 0);

    let remaining = keys;
    const levels: RankedBitVector[] = [];

    for (let level = 0; level < MAX_LEVELS && remaining.length > 0; level++) {
      const arraySize = Math.max(Math.ceil(remaining.length * gamma), 1);
      const counts = new Uint8Array(arraySize);
      const seed = level * 0x9E3779B9;

      for (const key of remaining) {
        const h = hashString(key, seed) % arraySize;
        if (counts[h] < 2) counts[h]++;
      }

      const bv = new RankedBitVector(arraySize);
      for (let i = 0; i < arraySize; i++) {
        if (counts[i] === 1) bv.set(i);
      }
      bv.buildRanks();
      levels.push(bv);

      const nextRemaining: string[] = [];
      for (const key of remaining) {
        const h = hashString(key, seed) % arraySize;
        if (counts[h] !== 1) nextRemaining.push(key);
      }

      remaining = nextRemaining;
    }

    if (remaining.length > 0) {
      throw new Error(`BBHash: ${remaining.length} keys unassigned after ${MAX_LEVELS} levels`);
    }

    return new BBHash(levels, n);
  }

  /** Returns index in [0, N) or -1 if key was not in the build set */
  lookup(key: string): number {
    for (let level = 0; level < this.levels.length; level++) {
      const bv = this.levels[level];
      const seed = level * 0x9E3779B9;
      const h = hashString(key, seed) % bv.bitCount;
      if (bv.get(h)) {
        return this.levelOffsets[level] + bv.rank(h);
      }
    }
    return -1;
  }

  serialize(): ArrayBuffer {
    let size = 4;
    for (const bv of this.levels) size += bv.byteSize();

    const buf = new ArrayBuffer(size);
    const view = new DataView(buf);
    let offset = 0;

    view.setUint32(offset, this.levels.length, true);
    offset += 4;
    for (const bv of this.levels) {
      offset = bv.writeTo(view, offset);
    }
    return buf;
  }

  static deserialize(view: DataView, startOffset: number): { bbhash: BBHash; nextOffset: number } {
    let offset = startOffset;
    const levelCount = view.getUint32(offset, true);
    offset += 4;

    const levels: RankedBitVector[] = [];
    for (let i = 0; i < levelCount; i++) {
      const { bv, nextOffset } = RankedBitVector.readFrom(view, offset);
      levels.push(bv);
      offset = nextOffset;
    }

    let size = 0;
    for (const bv of levels) size += bv.popcount;

    return { bbhash: new BBHash(levels, size), nextOffset: offset };
  }
}

// --- MPHF File Format ---

const MPHF_MAGIC = 0x48504D4C; // "LMPH" LE
const MPHF_VERSION = 1;
const MPHF_HEADER_SIZE = 48;
const VALUE_ENTRY_SIZE = 9;
const FINGERPRINT_SEED = 0xDEADBEEF;

export interface MphfTermInfo {
  term: string;
  shard: number;
  offset: number;
  count: number;
}

export interface MphfFile {
  bbhash: BBHash;
  fingerprints: Uint16Array;
  shards: Uint8Array;
  offsets: Uint32Array;
  counts: Uint16Array;
  sortedTerms: string[] | null;
  termCount: number;
  totalDocs: number;
  entrySize: number;
}

function computeFingerprint(term: string): number {
  return hashString(term, FINGERPRINT_SEED) & 0xFFFF;
}

export function buildMphfFile(
  terms: MphfTermInfo[],
  totalDocs: number,
  entrySize: number,
  includeSortedTerms: boolean = true,
): MphfFile {
  const keys = terms.map(t => t.term);
  const bbhash = BBHash.build(keys);

  const n = terms.length;
  const fingerprints = new Uint16Array(n);
  const shards = new Uint8Array(n);
  const offsets = new Uint32Array(n);
  const counts = new Uint16Array(n);

  for (const term of terms) {
    const idx = bbhash.lookup(term.term);
    if (idx < 0 || idx >= n) throw new Error(`BBHash lookup failed for: ${term.term}`);
    fingerprints[idx] = computeFingerprint(term.term);
    shards[idx] = term.shard;
    offsets[idx] = term.offset;
    counts[idx] = Math.min(term.count, 65535);
  }

  const sortedTerms = includeSortedTerms ? keys.slice().sort() : null;

  return { bbhash, fingerprints, shards, offsets, counts, sortedTerms, termCount: n, totalDocs, entrySize };
}

export function serializeMphf(mphf: MphfFile): ArrayBuffer {
  const bbhashBuf = mphf.bbhash.serialize();

  let termBuffers: Uint8Array[] | null = null;
  let termsSize = 0;
  if (mphf.sortedTerms) {
    termBuffers = [];
    for (const term of mphf.sortedTerms) {
      const bytes = encoder.encode(term);
      termBuffers.push(bytes);
      termsSize += 1 + bytes.length;
    }
  }

  const valuesOffset = MPHF_HEADER_SIZE + bbhashBuf.byteLength;
  const valuesSize = mphf.termCount * VALUE_ENTRY_SIZE;
  const termsOffset = termBuffers ? valuesOffset + valuesSize : 0;
  const totalSize = MPHF_HEADER_SIZE + bbhashBuf.byteLength + valuesSize + termsSize;

  const buf = new ArrayBuffer(totalSize);
  const view = new DataView(buf);
  const bytes = new Uint8Array(buf);

  view.setUint32(0, MPHF_MAGIC, true);
  view.setUint32(4, MPHF_VERSION, true);
  view.setUint32(8, mphf.termCount, true);
  view.setUint32(12, mphf.totalDocs, true);
  view.setUint32(16, mphf.entrySize, true);
  view.setUint32(20, valuesOffset, true);
  view.setUint32(24, termsOffset, true);
  view.setUint32(28, termsSize, true);

  bytes.set(new Uint8Array(bbhashBuf), MPHF_HEADER_SIZE);

  let vOff = valuesOffset;
  for (let i = 0; i < mphf.termCount; i++) {
    view.setUint16(vOff, mphf.fingerprints[i], true); vOff += 2;
    view.setUint8(vOff, mphf.shards[i]); vOff += 1;
    view.setUint32(vOff, mphf.offsets[i], true); vOff += 4;
    view.setUint16(vOff, mphf.counts[i], true); vOff += 2;
  }

  if (termBuffers) {
    let tOff = termsOffset;
    for (const tb of termBuffers) {
      view.setUint8(tOff, tb.length); tOff += 1;
      bytes.set(tb, tOff); tOff += tb.length;
    }
  }

  return buf;
}

export function deserializeMphf(buf: ArrayBuffer): MphfFile {
  const view = new DataView(buf);

  if (view.getUint32(0, true) !== MPHF_MAGIC) throw new Error('Invalid MPHF magic');
  if (view.getUint32(4, true) !== MPHF_VERSION) throw new Error('Unsupported MPHF version');

  const termCount = view.getUint32(8, true);
  const totalDocs = view.getUint32(12, true);
  const entrySize = view.getUint32(16, true);
  const valuesOffset = view.getUint32(20, true);
  const termsOffset = view.getUint32(24, true);
  const termsSize = view.getUint32(28, true);

  const { bbhash } = BBHash.deserialize(view, MPHF_HEADER_SIZE);

  const fingerprints = new Uint16Array(termCount);
  const shards = new Uint8Array(termCount);
  const offsets = new Uint32Array(termCount);
  const counts = new Uint16Array(termCount);

  let vOff = valuesOffset;
  for (let i = 0; i < termCount; i++) {
    fingerprints[i] = view.getUint16(vOff, true); vOff += 2;
    shards[i] = view.getUint8(vOff); vOff += 1;
    offsets[i] = view.getUint32(vOff, true); vOff += 4;
    counts[i] = view.getUint16(vOff, true); vOff += 2;
  }

  let sortedTerms: string[] | null = null;
  if (termsOffset > 0 && termsSize > 0) {
    sortedTerms = [];
    let tOff = termsOffset;
    const termsEnd = termsOffset + termsSize;
    while (tOff < termsEnd && sortedTerms.length < termCount) {
      const len = view.getUint8(tOff); tOff += 1;
      const termBytes = new Uint8Array(buf, tOff, len);
      sortedTerms.push(decoder.decode(termBytes));
      tOff += len;
    }
  }

  return { bbhash, fingerprints, shards, offsets, counts, sortedTerms, termCount, totalDocs, entrySize };
}

// --- Query Resolution ---

export interface MphfLookupResult {
  term: string;
  shard: number;
  offset: number;
  count: number;
}

export interface MphfResolvedToken {
  entries: MphfLookupResult[];
  totalDf: number;
  isPrefix: boolean;
}

const MAX_PREFIX_TERMS = 20;
const MAX_PREFIX_SCAN = 5000;

export function mphfExactLookup(mphf: MphfFile, term: string): MphfLookupResult | null {
  const idx = mphf.bbhash.lookup(term);
  if (idx < 0 || idx >= mphf.termCount) return null;

  const fp = computeFingerprint(term);
  if (mphf.fingerprints[idx] !== fp) return null;

  return {
    term,
    shard: mphf.shards[idx],
    offset: mphf.offsets[idx],
    count: mphf.counts[idx],
  };
}

function lowerBoundSorted(arr: string[], target: string): number {
  let lo = 0, hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (arr[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

export function mphfResolveToken(mphf: MphfFile, token: string, isLast: boolean): MphfResolvedToken {
  if (!isLast) {
    const exact = mphfExactLookup(mphf, token);
    if (exact) return { entries: [exact], totalDf: exact.count, isPrefix: false };
    return { entries: [], totalDf: 0, isPrefix: false };
  }

  if (token.length >= 4) {
    const exact = mphfExactLookup(mphf, token);
    if (exact && exact.count >= 100) return { entries: [exact], totalDf: exact.count, isPrefix: false };
  }

  if (!mphf.sortedTerms) {
    const exact = mphfExactLookup(mphf, token);
    if (exact) return { entries: [exact], totalDf: exact.count, isPrefix: false };
    return { entries: [], totalDf: 0, isPrefix: false };
  }

  const start = lowerBoundSorted(mphf.sortedTerms, token);
  const candidates: { term: string; count: number }[] = [];

  for (let i = start; i < mphf.sortedTerms.length && candidates.length < MAX_PREFIX_SCAN; i++) {
    const t = mphf.sortedTerms[i];
    if (!t.startsWith(token)) break;
    const info = mphfExactLookup(mphf, t);
    if (info) candidates.push({ term: t, count: info.count });
  }

  if (candidates.length > MAX_PREFIX_TERMS) {
    candidates.sort((a, b) => b.count - a.count);
    candidates.length = MAX_PREFIX_TERMS;
  }

  const entries: MphfLookupResult[] = [];
  let totalDf = 0;
  for (const c of candidates) {
    const info = mphfExactLookup(mphf, c.term)!;
    entries.push(info);
    totalDf += info.count;
  }

  return {
    entries,
    totalDf,
    isPrefix: entries.length > 1 || (entries.length === 1 && entries[0].term !== token),
  };
}
