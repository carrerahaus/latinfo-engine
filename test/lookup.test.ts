import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { lookupId, clearIndexCache, findSource } from '../src/index';
import { buildBinaryFiles } from '../src/build/binary';
import type { Storage, StorageObject } from '../src/storage';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// In-memory storage backed by files on disk (simulates R2/S3)
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

const tmpDir = path.join(os.tmpdir(), `latinfo-engine-test-${Date.now()}`);
let storage: FileStorage;

// Synthetic test data: 5 RUCs with known fields
const TEST_RECORDS = [
  '20100047218\tBANCO DE CREDITO DEL PERU\tACTIVO\tHABIDO\t150114\tAV.\tCENTENARIO\t\t\t156\t\t\t\t',
  '20100130204\tSCOTIABANK PERU S.A.A.\tACTIVO\tHABIDO\t150131\tAV.\tDIONISIO DERTEANO\t\t\t102\t\t\t\t',
  '20259702637\tCOMPANIA MINERA ANTAMINA\tACTIVO\tHABIDO\t150115\tAV.\tEL DERBY\t\t\t055\t\t\t\t',
  '20418896915\tLATAM AIRLINES PERU\tACTIVO\tHABIDO\t150112\tAV.\tJOSE PARDO\t\t\t231\t\t\t\t',
  '20544489917\tINTERBANK\tACTIVO\tHABIDO\t150115\tAV.\tCARLOS VILLARÁN\t\t\t140\t\t\t\t',
];

beforeAll(async () => {
  // Write sorted TSV
  fs.mkdirSync(tmpDir, { recursive: true });
  const tsvPath = path.join(tmpDir, 'test.tsv');
  fs.writeFileSync(tsvPath, TEST_RECORDS.join('\n'));

  // Build binary files
  await buildBinaryFiles(tsvPath, tmpDir, 'pe-sunat-padron', {
    idLength: 11,
    idRegex: /^\d{11}$/,
    prefixLength: 5,
    fieldCount: 14,
  });

  storage = new FileStorage(tmpDir);
});

afterAll(() => {
  clearIndexCache('pe-sunat-padron');
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('lookup end-to-end', () => {
  it('builds .idx and .bin files', () => {
    expect(fs.existsSync(path.join(tmpDir, 'pe-sunat-padron.idx'))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, 'pe-sunat-padron-0.bin'))).toBe(true);
  });

  it('finds BCP by RUC', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20100047218');
    expect(result).not.toBeNull();
    expect(result!.ruc).toBe('20100047218');
    expect(result!.razon_social).toBe('BANCO DE CREDITO DEL PERU');
    expect(result!.estado).toBe('ACTIVO');
    expect(result!.condicion).toBe('HABIDO');
  });

  it('finds Scotiabank by RUC', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20100130204');
    expect(result).not.toBeNull();
    expect(result!.razon_social).toBe('SCOTIABANK PERU S.A.A.');
  });

  it('finds Interbank by RUC', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20544489917');
    expect(result).not.toBeNull();
    expect(result!.razon_social).toBe('INTERBANK');
  });

  it('returns null for non-existent RUC', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20100047219');
    expect(result).toBeNull();
  });

  it('returns null for non-existent prefix', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '99999999999');
    expect(result).toBeNull();
  });

  it('decodes all fields correctly', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const result = await lookupId(storage, source, '20259702637');
    expect(result).not.toBeNull();
    expect(result!.razon_social).toBe('COMPANIA MINERA ANTAMINA');
    expect(result!.ubigeo).toBe('150115');
    expect(result!.tipo_via).toBe('AV.');
    expect(result!.nombre_via).toBe('EL DERBY');
    expect(result!.numero).toBe('055');
  });

  it('lookups are fast (< 5ms per lookup after index loaded)', async () => {
    const source = findSource('pe', 'sunat', 'padron')!;
    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      await lookupId(storage, source, '20100047218');
    }
    const elapsed = performance.now() - start;
    expect(elapsed / 100).toBeLessThan(5);
  });
});
