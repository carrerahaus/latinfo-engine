import { describe, it, expect } from 'vitest';
import {
  BBHash,
  buildMphfFile,
  serializeMphf,
  deserializeMphf,
  mphfExactLookup,
  mphfResolveToken,
} from '../src/mphf/bbhash';
import type { MphfTermInfo } from '../src/mphf/bbhash';

describe('BBHash', () => {
  it('builds and looks up small key set', () => {
    const keys = ['banco', 'credito', 'peru', 'lima', 'activo'];
    const bbhash = BBHash.build(keys);
    expect(bbhash.size).toBe(5);

    const indices = new Set<number>();
    for (const key of keys) {
      const idx = bbhash.lookup(key);
      expect(idx).toBeGreaterThanOrEqual(0);
      expect(idx).toBeLessThan(5);
      indices.add(idx);
    }
    // Perfect hash: all indices unique
    expect(indices.size).toBe(5);
  });

  it('returns -1 for unknown keys', () => {
    const keys = ['banco', 'credito', 'peru'];
    const bbhash = BBHash.build(keys);
    // Unknown key may return -1 or a valid index (false positive),
    // but fingerprint check in mphfExactLookup catches false positives
    const idx = bbhash.lookup('zzzznotexist');
    // Just verify it doesn't crash
    expect(typeof idx).toBe('number');
  });

  it('handles 1000 keys with zero collisions', () => {
    const keys = Array.from({ length: 1000 }, (_, i) => `term_${i.toString().padStart(6, '0')}`);
    const bbhash = BBHash.build(keys);
    expect(bbhash.size).toBe(1000);

    const indices = new Set<number>();
    for (const key of keys) {
      const idx = bbhash.lookup(key);
      expect(idx).toBeGreaterThanOrEqual(0);
      expect(idx).toBeLessThan(1000);
      indices.add(idx);
    }
    expect(indices.size).toBe(1000);
  });

  it('serialize + deserialize roundtrips', () => {
    const keys = ['banco', 'credito', 'peru', 'lima', 'activo'];
    const original = BBHash.build(keys);
    const buf = original.serialize();
    const { bbhash: restored } = BBHash.deserialize(new DataView(buf), 0);

    expect(restored.size).toBe(original.size);
    for (const key of keys) {
      expect(restored.lookup(key)).toBe(original.lookup(key));
    }
  });

  it('handles empty key set', () => {
    const bbhash = BBHash.build([]);
    expect(bbhash.size).toBe(0);
    expect(bbhash.lookup('anything')).toBe(-1);
  });
});

describe('MPHF file', () => {
  const terms: MphfTermInfo[] = [
    { term: 'banco', shard: 0, offset: 0, count: 500 },
    { term: 'credito', shard: 0, offset: 4400, count: 200 },
    { term: 'peru', shard: 0, offset: 6600, count: 1000 },
    { term: 'lima', shard: 0, offset: 17600, count: 300 },
    { term: 'construccion', shard: 1, offset: 0, count: 150 },
    { term: 'construcciones', shard: 1, offset: 1320, count: 50 },
    { term: 'minera', shard: 0, offset: 20900, count: 80 },
  ];

  it('build + exact lookup all terms', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110);

    for (const t of terms) {
      const result = mphfExactLookup(mphf, t.term);
      expect(result).not.toBeNull();
      expect(result!.term).toBe(t.term);
      expect(result!.shard).toBe(t.shard);
      expect(result!.offset).toBe(t.offset);
      expect(result!.count).toBe(t.count);
    }
  });

  it('exact lookup returns null for unknown term', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110);
    const result = mphfExactLookup(mphf, 'zzzznotexist');
    expect(result).toBeNull();
  });

  it('serialize + deserialize roundtrips', () => {
    const original = buildMphfFile(terms, 18_000_000, 110);
    const buf = serializeMphf(original);
    const restored = deserializeMphf(buf);

    expect(restored.termCount).toBe(original.termCount);
    expect(restored.totalDocs).toBe(original.totalDocs);
    expect(restored.entrySize).toBe(original.entrySize);

    for (const t of terms) {
      const result = mphfExactLookup(restored, t.term);
      expect(result).not.toBeNull();
      expect(result!.shard).toBe(t.shard);
      expect(result!.offset).toBe(t.offset);
      expect(result!.count).toBe(t.count);
    }
  });

  it('includes sorted terms for prefix search', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110, true);
    expect(mphf.sortedTerms).not.toBeNull();
    expect(mphf.sortedTerms!.length).toBe(terms.length);
    // Verify sorted
    for (let i = 1; i < mphf.sortedTerms!.length; i++) {
      expect(mphf.sortedTerms![i] >= mphf.sortedTerms![i - 1]).toBe(true);
    }
  });

  it('sorted terms survive serialization', () => {
    const original = buildMphfFile(terms, 18_000_000, 110, true);
    const buf = serializeMphf(original);
    const restored = deserializeMphf(buf);

    expect(restored.sortedTerms).not.toBeNull();
    expect(restored.sortedTerms!.length).toBe(original.sortedTerms!.length);
    for (let i = 0; i < restored.sortedTerms!.length; i++) {
      expect(restored.sortedTerms![i]).toBe(original.sortedTerms![i]);
    }
  });
});

describe('MPHF token resolution', () => {
  const terms: MphfTermInfo[] = [
    { term: 'banco', shard: 0, offset: 0, count: 500 },
    { term: 'bancos', shard: 0, offset: 4400, count: 50 },
    { term: 'bancolombia', shard: 0, offset: 4840, count: 30 },
    { term: 'credito', shard: 0, offset: 5100, count: 200 },
    { term: 'peru', shard: 0, offset: 7300, count: 1000 },
    { term: 'construccion', shard: 1, offset: 0, count: 150 },
    { term: 'construcciones', shard: 1, offset: 1320, count: 50 },
  ];

  it('non-last token: exact match only', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110);
    const result = mphfResolveToken(mphf, 'banco', false);
    expect(result.isPrefix).toBe(false);
    expect(result.entries.length).toBe(1);
    expect(result.entries[0].term).toBe('banco');
  });

  it('last token: prefix expands to matching terms', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110);
    const result = mphfResolveToken(mphf, 'ban', true);
    expect(result.isPrefix).toBe(true);
    expect(result.entries.length).toBe(3); // banco, bancos, bancolombia
    const resultTerms = result.entries.map(e => e.term).sort();
    expect(resultTerms).toEqual(['banco', 'bancolombia', 'bancos']);
  });

  it('last token: "const" matches construccion + construcciones', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110);
    const result = mphfResolveToken(mphf, 'const', true);
    expect(result.isPrefix).toBe(true);
    expect(result.entries.length).toBe(2);
  });

  it('non-matching token returns empty', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110);
    const result = mphfResolveToken(mphf, 'zzzzz', true);
    expect(result.entries.length).toBe(0);
    expect(result.totalDf).toBe(0);
  });

  it('totalDf sums all matching entries', () => {
    const mphf = buildMphfFile(terms, 18_000_000, 110);
    const result = mphfResolveToken(mphf, 'ban', true);
    expect(result.totalDf).toBe(500 + 50 + 30); // banco + bancos + bancolombia
  });
});
