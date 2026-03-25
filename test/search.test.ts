import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { searchByName, clearSearchCache, clearIndexCache, findSource } from '../src/index';
import { buildBinaryFiles } from '../src/build/binary';
import { buildSearchIndex } from '../src/build/search-index';
import type { Storage, StorageObject } from '../src/storage';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

class FileStorage implements Storage {
  constructor(private dir: string) {}

  async get(key: string, options?: { range?: { offset: number; length: number } }): Promise<StorageObject | null> {
    const filePath = path.join(this.dir, key);
    if (!fs.existsSync(filePath)) return null;

    const full = fs.readFileSync(filePath);
    if (options?.range) {
      const { offset, length } = options.range;
      const slice = full.subarray(offset, offset + length);
      return { arrayBuffer: async () => slice.buffer.slice(slice.byteOffset, slice.byteOffset + slice.byteLength) };
    }
    return { arrayBuffer: async () => full.buffer.slice(full.byteOffset, full.byteOffset + full.byteLength) };
  }
}

const tmpDir = path.join(os.tmpdir(), `latinfo-engine-search-test-${Date.now()}`);
let storage: FileStorage;

// Synthetic data: companies with different name patterns
const TEST_RECORDS = [
  '20100047218\tBANCO DE CREDITO DEL PERU\tACTIVO\tHABIDO\t150114\tAV.\tCENTENARIO\t\t\t156\t\t\t\t',
  '20100130204\tSCOTIABANK PERU S.A.A.\tACTIVO\tHABIDO\t150131\tAV.\tDIONISIO DERTEANO\t\t\t102\t\t\t\t',
  '20100153090\tBANCO INTERAMERICANO DE FINANZAS\tACTIVO\tHABIDO\t150115\tAV.\tRICARDO PALMA\t\t\t278\t\t\t\t',
  '20259702637\tCOMPAÑIA MINERA ANTAMINA S.A.\tACTIVO\tHABIDO\t150115\tAV.\tEL DERBY\t\t\t055\t\t\t\t',
  '20330791412\tCONSTRUCCIONES METÁLICAS PERÚ E.I.R.L.\tBAJA\tHABIDO\t150101\tJR.\tLAMPA\t\t\t200\t\t\t\t',
  '20418896915\tLATAM AIRLINES PERU S.A.\tACTIVO\tHABIDO\t150112\tAV.\tJOSE PARDO\t\t\t231\t\t\t\t',
  '20462509236\tBANCO FALABELLA PERU S.A.\tACTIVO\tHABIDO\t150115\tAV.\tPASEO DE LA REPUBLICA\t\t\t3220\t\t\t\t',
  '20544489917\tINTERBANK\tACTIVO\tHABIDO\t150115\tAV.\tCARLOS VILLARÁN\t\t\t140\t\t\t\t',
];

const baseName = 'pe-sunat-padron';

beforeAll(async () => {
  fs.mkdirSync(tmpDir, { recursive: true });
  const tsvPath = path.join(tmpDir, 'test.tsv');
  fs.writeFileSync(tsvPath, TEST_RECORDS.join('\n'));

  // Build binary files (needed for V1 secondary lookups)
  await buildBinaryFiles(tsvPath, tmpDir, baseName, {
    idLength: 11,
    idRegex: /^\d{11}$/,
    prefixLength: 5,
    fieldCount: 14,
  });

  // Build V2 search index (inline fields)
  await buildSearchIndex(tsvPath, tmpDir, baseName, {
    searchFieldIndex: 0,
    idRegex: /^\d{11}$/,
    statusFieldIndex: 1,
  }, TEST_RECORDS.length);

  storage = new FileStorage(tmpDir);
});

afterAll(() => {
  clearSearchCache(baseName);
  clearIndexCache(baseName);
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('search end-to-end (V2)', () => {
  it('builds search index files', () => {
    expect(fs.existsSync(path.join(tmpDir, `${baseName}-search.idx`))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, `${baseName}-search-0.dat`))).toBe(true);
  });

  it('finds "banco" — multiple results', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const results = await searchByName(storage, source, 'banco');
    expect(Array.isArray(results)).toBe(true);
    expect((results as any[]).length).toBeGreaterThanOrEqual(3);
    const names = (results as any[]).map((r: any) => r.razon_social);
    expect(names.some((n: string) => n.includes('CREDITO'))).toBe(true);
    expect(names.some((n: string) => n.includes('FALABELLA'))).toBe(true);
  });

  it('finds "banco credito" — multi-token intersection', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const results = await searchByName(storage, source, 'banco credito');
    expect(Array.isArray(results)).toBe(true);
    expect((results as any[]).length).toBeGreaterThanOrEqual(1);
    expect((results as any[])[0].razon_social).toContain('CREDITO');
  });

  it('finds "latam airlines" — exact phrase scores highest', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const results = await searchByName(storage, source, 'latam airlines');
    expect(Array.isArray(results)).toBe(true);
    expect((results as any[])[0].razon_social).toContain('LATAM');
  });

  it('handles prefix search — "const" matches "construcciones"', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const results = await searchByName(storage, source, 'const');
    expect(Array.isArray(results)).toBe(true);
    expect((results as any[]).length).toBeGreaterThanOrEqual(1);
  });

  it('returns V2 inline fields (razon_social + estado)', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const results = await searchByName(storage, source, 'interbank');
    expect(Array.isArray(results)).toBe(true);
    const r = (results as any[])[0];
    expect(r.ruc).toBe('20544489917');
    expect(r.razon_social).toBe('INTERBANK');
    expect(r.estado).toBe('ACTIVO');
  });

  it('handles diacritics — "metalica" matches "METÁLICAS"', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const results = await searchByName(storage, source, 'metalica');
    expect(Array.isArray(results)).toBe(true);
    expect((results as any[]).length).toBeGreaterThanOrEqual(1);
  });

  it('rejects short queries', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await searchByName(storage, source, 'ab');
    expect('error' in result).toBe(true);
  });

  it('rejects all-stopword queries', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await searchByName(storage, source, 'de la el');
    expect('error' in result).toBe(true);
  });

  it('returns empty for non-matching query', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const results = await searchByName(storage, source, 'zzzznotexist');
    expect(Array.isArray(results)).toBe(true);
    expect((results as any[]).length).toBe(0);
  });
});
