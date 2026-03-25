import { describe, it, expect } from 'vitest';
import { sources, findSource, dniToRuc } from '../src/sources';

describe('sources config', () => {
  it('has at least one source', () => {
    expect(sources.length).toBeGreaterThan(0);
  });

  it('Peru SUNAT padron has correct structure', () => {
    const pe = findSource('pe', 'sunat', 'padron');
    expect(pe).toBeDefined();
    expect(pe!.country).toBe('pe');
    expect(pe!.institution).toBe('sunat');
    expect(pe!.dataset).toBe('padron');
    expect(pe!.baseName).toBe('pe-sunat-padron');
    expect(pe!.routePath).toBe('/pe/sunat/padron');
    expect(pe!.primaryId.name).toBe('ruc');
    expect(pe!.primaryId.length).toBe(11);
    expect(pe!.primaryId.prefixLength).toBe(5);
  });

  it('Peru primary ID regex validates 11-digit RUCs', () => {
    const pe = findSource('pe', 'sunat', 'padron')!;
    expect(pe.primaryId.regex.test('20100047218')).toBe(true);
    expect(pe.primaryId.regex.test('123')).toBe(false);
    expect(pe.primaryId.regex.test('abcdefghijk')).toBe(false);
  });

  it('Peru has DNI as alternate ID', () => {
    const pe = findSource('pe', 'sunat', 'padron')!;
    expect(pe.alternateIds).toHaveLength(1);
    expect(pe.alternateIds[0].name).toBe('dni');
    expect(pe.alternateIds[0].length).toBe(8);
  });

  it('Peru DNI alternate converts to valid RUC', () => {
    const pe = findSource('pe', 'sunat', 'padron')!;
    const ruc = pe.alternateIds[0].toPrimaryId('09346247');
    expect(ruc).toMatch(/^\d{11}$/);
    expect(ruc.startsWith('10')).toBe(true);
  });

  it('Peru SUNAT padron has 14 field names', () => {
    const pe = findSource('pe', 'sunat', 'padron')!;
    expect(pe.fieldNames).toHaveLength(14);
    expect(pe.fieldNames[0]).toBe('razon_social');
  });

  it('Colombia RUES has correct structure', () => {
    const co = findSource('co', 'rues', 'registry');
    expect(co).toBeDefined();
    expect(co!.routePath).toBe('/co/rues/registry');
    expect(co!.primaryId.name).toBe('nit');
  });

  it('all sources have unique route paths', () => {
    const paths = sources.map(s => s.routePath);
    expect(new Set(paths).size).toBe(paths.length);
  });

  it('all sources have routePath in correct format', () => {
    for (const s of sources) {
      expect(s.routePath).toMatch(/^\/[a-z]+\/[a-z-]+\/[a-z-]+$/);
    }
  });

  it('OSCE sources exist', () => {
    expect(findSource('pe', 'osce', 'sanctioned')).toBeDefined();
    expect(findSource('pe', 'osce', 'fines')).toBeDefined();
  });

  it('SUNAT coactiva exists', () => {
    expect(findSource('pe', 'sunat', 'coactiva')).toBeDefined();
  });
});

describe('dniToRuc', () => {
  it('converts known DNI 09346247 to RUC 10093462471', () => {
    expect(dniToRuc('09346247')).toBe('10093462471');
  });

  it('always starts with 10 (persona natural)', () => {
    const ruc = dniToRuc('12345678');
    expect(ruc.startsWith('10')).toBe(true);
  });

  it('always returns 11 digits', () => {
    expect(dniToRuc('00000001')).toHaveLength(11);
    expect(dniToRuc('99999999')).toHaveLength(11);
  });

  it('check digit is 0-9', () => {
    const ruc = dniToRuc('09346247');
    const check = parseInt(ruc[10]);
    expect(check).toBeGreaterThanOrEqual(0);
    expect(check).toBeLessThanOrEqual(9);
  });

  it('handles DNI with leading zeros', () => {
    const ruc = dniToRuc('00000001');
    expect(ruc).toMatch(/^\d{11}$/);
  });
});
