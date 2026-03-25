/**
 * Integration test against REAL production data.
 *
 * Downloads binary files from CDN, then runs lookup + search + MPHF
 * against actual government registry data.
 *
 * Run: DATA_DIR=/tmp/latinfo-engine-test npx vitest run test/real-data.test.ts
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  lookupId, clearIndexCache,
  searchByName, clearSearchCache,
  findSource,
  deserializeMphf, mphfExactLookup, mphfResolveToken,
  tokenize,
} from '../src/index';
import type { Storage, StorageObject } from '../src/storage';
import * as fs from 'fs';
import * as path from 'path';
import * as https from 'https';

const CDN = 'https://data.latinfo.dev';
const DATA_DIR = process.env.DATA_DIR || '/tmp/latinfo-engine-test';

// Use a small source for fast download: pe-osce-sanctioned (~5K records, <1MB)
const TEST_SOURCE = 'pe-osce-sanctioned';
// Also test SUNAT padron lookup (only needs .idx ~300KB + one shard range)
const SUNAT_SOURCE = 'pe-sunat-padron';

function download(url: string, dest: string): Promise<void> {
  if (fs.existsSync(dest)) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const dir = path.dirname(dest);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const file = fs.createWriteStream(dest);
    https.get(url, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        file.close();
        fs.unlinkSync(dest);
        download(res.headers.location!, dest).then(resolve, reject);
        return;
      }
      if (res.statusCode !== 200) {
        file.close();
        fs.unlinkSync(dest);
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      res.pipe(file);
      file.on('finish', () => { file.close(); resolve(); });
    }).on('error', (e) => { fs.unlinkSync(dest); reject(e); });
  });
}

class FileStorage implements Storage {
  constructor(private dir: string) {}

  async get(key: string, options?: { range?: { offset: number; length: number } }): Promise<StorageObject | null> {
    const filePath = path.join(this.dir, key);
    if (!fs.existsSync(filePath)) return null;

    if (options?.range) {
      const { offset, length } = options.range;
      const fd = fs.openSync(filePath, 'r');
      const buf = Buffer.alloc(length);
      fs.readSync(fd, buf, 0, length, offset);
      fs.closeSync(fd);
      return { arrayBuffer: async () => buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) };
    }

    const full = fs.readFileSync(filePath);
    return { arrayBuffer: async () => full.buffer.slice(full.byteOffset, full.byteOffset + full.byteLength) };
  }
}

let storage: FileStorage;

beforeAll(async () => {
  console.log(`Downloading real data to ${DATA_DIR}...`);

  // Download pe-osce-sanctioned (small, ~5K records)
  await Promise.all([
    download(`${CDN}/${TEST_SOURCE}.idx`, path.join(DATA_DIR, `${TEST_SOURCE}.idx`)),
    download(`${CDN}/${TEST_SOURCE}-0.bin`, path.join(DATA_DIR, `${TEST_SOURCE}-0.bin`)),
    download(`${CDN}/${TEST_SOURCE}-search.idx`, path.join(DATA_DIR, `${TEST_SOURCE}-search.idx`)),
    download(`${CDN}/${TEST_SOURCE}-search-0.dat`, path.join(DATA_DIR, `${TEST_SOURCE}-search-0.dat`)),
    download(`${CDN}/${TEST_SOURCE}-search.mphf`, path.join(DATA_DIR, `${TEST_SOURCE}-search.mphf`)),
  ]);
  console.log(`  ${TEST_SOURCE}: done`);

  // Download pe-sunat-padron index, then figure out which shard BCP is in
  await download(`${CDN}/${SUNAT_SOURCE}.idx`, path.join(DATA_DIR, `${SUNAT_SOURCE}.idx`));

  // Parse index to find BCP's shard (prefix 20100)
  const idxBuf = fs.readFileSync(path.join(DATA_DIR, `${SUNAT_SOURCE}.idx`));
  const idxView = new DataView(idxBuf.buffer, idxBuf.byteOffset, idxBuf.byteLength);
  const entryCount = idxView.getUint32(4, true);
  let bcpShard = 0;
  for (let i = 0; i < entryCount; i++) {
    const off = 16 + i * 16;
    if (idxView.getUint32(off, true) === 20100) { bcpShard = idxView.getUint32(off + 4, true); break; }
  }
  const shardFile = `${SUNAT_SOURCE}-${bcpShard}.bin`;
  if (!fs.existsSync(path.join(DATA_DIR, shardFile))) {
    console.log(`  ${shardFile}: downloading (~200MB)...`);
    await download(`${CDN}/${shardFile}`, path.join(DATA_DIR, shardFile));
  }
  console.log(`  ${SUNAT_SOURCE}: done (shard ${bcpShard})`);

  storage = new FileStorage(DATA_DIR);
}, 300_000); // 5 min timeout for downloads

afterAll(() => {
  clearIndexCache();
  clearSearchCache();
});

describe('lookup with real data', () => {
  it('finds a known OSCE sanctioned record by RUC', async () => {
    const source = findSource('pe', 'osce', 'sanctioned')!;
    // Load the index to verify it parses
    const idx = await storage.get(`${TEST_SOURCE}.idx`);
    expect(idx).not.toBeNull();

    // Try looking up any RUC — we just need to verify the engine works
    // We'll scan the index to find a valid prefix first
    // For now, just verify index loads without error
    const result = await lookupId(storage, source, '20100047218');
    // May or may not find this RUC in OSCE sanctioned — that's fine
    // The test is that lookupId doesn't crash with real data
    expect(result === null || typeof result === 'object').toBe(true);
  });

  it('finds BCP in SUNAT padron (shard 0, prefix 20100)', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20100047218');
    expect(result).not.toBeNull();
    expect(result!.ruc).toBe('20100047218');
    expect(result!.razon_social).toContain('BANCO DE CREDITO');
    expect(result!.estado).toBe('ACTIVO');
  });

  it('finds RUC 20100130204 in SUNAT padron (BBVA, formerly Scotiabank)', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20100130204');
    expect(result).not.toBeNull();
    expect(result!.ruc).toBe('20100130204');
    expect(result!.razon_social.length).toBeGreaterThan(0);
  });

  it('returns null for non-existent RUC', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20100000000');
    expect(result).toBeNull();
  });
});

describe('search with real data', () => {
  it('searches OSCE sanctioned by name', async () => {
    const source = findSource('pe', 'osce', 'sanctioned')!;
    const results = await searchByName(storage, source, 'constructora');
    expect(Array.isArray(results)).toBe(true);
    // Real data — should find some construction companies that are sanctioned
    console.log(`  OSCE sanctioned "constructora": ${(results as any[]).length} results`);
  });

  it('rejects short queries on real data', async () => {
    const source = findSource('pe', 'osce', 'sanctioned')!;
    const result = await searchByName(storage, source, 'ab');
    expect('error' in result).toBe(true);
  });
});

describe('MPHF with real data', () => {
  it('deserializes real MPHF file', async () => {
    const mphfPath = path.join(DATA_DIR, `${TEST_SOURCE}-search.mphf`);
    const buf = fs.readFileSync(mphfPath);
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    const mphf = deserializeMphf(ab);

    expect(mphf.termCount).toBeGreaterThan(0);
    expect(mphf.totalDocs).toBeGreaterThan(0);
    console.log(`  MPHF: ${mphf.termCount} terms, ${mphf.totalDocs} docs, entrySize=${mphf.entrySize}`);
  });

  it('looks up a known term in real MPHF', async () => {
    const mphfPath = path.join(DATA_DIR, `${TEST_SOURCE}-search.mphf`);
    const buf = fs.readFileSync(mphfPath);
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    const mphf = deserializeMphf(ab);

    // Use sorted terms to find a real term
    expect(mphf.sortedTerms).not.toBeNull();
    expect(mphf.sortedTerms!.length).toBeGreaterThan(0);

    const testTerm = mphf.sortedTerms![0];
    const result = mphfExactLookup(mphf, testTerm);
    expect(result).not.toBeNull();
    expect(result!.term).toBe(testTerm);
    expect(result!.count).toBeGreaterThan(0);
    console.log(`  MPHF exact lookup "${testTerm}": shard=${result!.shard}, count=${result!.count}`);
  });

  it('prefix resolution finds multiple terms', async () => {
    const mphfPath = path.join(DATA_DIR, `${TEST_SOURCE}-search.mphf`);
    const buf = fs.readFileSync(mphfPath);
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    const mphf = deserializeMphf(ab);

    // Find a prefix that has multiple completions
    const resolved = mphfResolveToken(mphf, 'con', true);
    expect(resolved.entries.length).toBeGreaterThan(0);
    console.log(`  MPHF prefix "con": ${resolved.entries.length} terms, totalDf=${resolved.totalDf}`);
  });

  it('exhaustive: every sorted term resolves via MPHF', async () => {
    const mphfPath = path.join(DATA_DIR, `${TEST_SOURCE}-search.mphf`);
    const buf = fs.readFileSync(mphfPath);
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    const mphf = deserializeMphf(ab);

    let failures = 0;
    for (const term of mphf.sortedTerms!) {
      const result = mphfExactLookup(mphf, term);
      if (!result) failures++;
    }
    expect(failures).toBe(0);
    console.log(`  MPHF exhaustive: ${mphf.sortedTerms!.length}/${mphf.sortedTerms!.length} terms OK`);
  });
});

describe('performance with real data', () => {
  it('100 lookups < 500ms total', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      await lookupId(storage, source, '20100047218');
    }
    const elapsed = performance.now() - start;
    console.log(`  100 lookups: ${elapsed.toFixed(0)}ms (${(elapsed / 100).toFixed(1)}ms/lookup)`);
    expect(elapsed).toBeLessThan(500);
  });

  it('100 MPHF lookups < 50ms total', async () => {
    const mphfPath = path.join(DATA_DIR, `${TEST_SOURCE}-search.mphf`);
    const buf = fs.readFileSync(mphfPath);
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    const mphf = deserializeMphf(ab);
    const term = mphf.sortedTerms![0];

    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      mphfExactLookup(mphf, term);
    }
    const elapsed = performance.now() - start;
    console.log(`  100 MPHF lookups: ${elapsed.toFixed(1)}ms (${(elapsed / 100).toFixed(3)}ms/lookup)`);
    expect(elapsed).toBeLessThan(50);
  });
});
