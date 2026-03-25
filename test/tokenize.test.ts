import { describe, it, expect } from 'vitest';
import { tokenize, isAllStopWords, STOP_WORDS_BY_LANG } from '../src/tokenize';

describe('tokenize', () => {
  it('lowercases and splits on non-alphanumeric', () => {
    expect(tokenize('BANCO DE CREDITO')).toEqual(['banco', 'credito']);
  });

  it('removes Spanish stop words by default', () => {
    const tokens = tokenize('SERVICIOS DE LA CONSTRUCCION');
    expect(tokens).not.toContain('de');
    expect(tokens).not.toContain('la');
    expect(tokens).toContain('servicios');
    expect(tokens).toContain('construccion');
  });

  it('removes diacritics', () => {
    expect(tokenize('CONSTRUCCIÓN METÁLICA')).toEqual(['construccion', 'metalica']);
  });

  it('deduplicates tokens', () => {
    expect(tokenize('BANCO BANCO BANCO')).toEqual(['banco']);
  });

  it('filters words shorter than 2 chars', () => {
    expect(tokenize('A B CD')).toEqual(['cd']);
  });

  it('accepts Portuguese stop words', () => {
    const pt = STOP_WORDS_BY_LANG.pt;
    const tokens = tokenize('EMPRESA DO BRASIL', pt);
    expect(tokens).not.toContain('do');
    expect(tokens).toContain('empresa');
    expect(tokens).toContain('brasil');
  });

  it('accepts English stop words', () => {
    const en = STOP_WORDS_BY_LANG.en;
    const tokens = tokenize('THE BANK OF AMERICA', en);
    expect(tokens).not.toContain('the');
    expect(tokens).not.toContain('of');
    expect(tokens).toContain('bank');
    expect(tokens).toContain('america');
  });
});

describe('isAllStopWords', () => {
  it('returns true for only stop words', () => {
    expect(isAllStopWords('de la el')).toBe(true);
  });

  it('returns false when non-stop words present', () => {
    expect(isAllStopWords('de la empresa')).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(isAllStopWords('')).toBe(false);
  });
});

describe('abbreviation collapsing', () => {
  it('collapses and filters E.C.A. as stop word', () => {
    expect(tokenize('E.C.A.')).toEqual([]);
  });

  it('collapses and filters S.A.C. as stop word', () => {
    expect(tokenize('S.A.C.')).toEqual([]);
  });

  it('collapses and filters E.I.R.L. as stop word', () => {
    expect(tokenize('E.I.R.L.')).toEqual([]);
  });

  it('collapses and filters S.R.L. as stop word', () => {
    expect(tokenize('S.R.L.')).toEqual([]);
  });

  it('collapses and filters S.A. as stop word', () => {
    expect(tokenize('S.A.')).toEqual([]);
  });

  it('collapses and filters S.A (no trailing dot) as stop word', () => {
    expect(tokenize('S.A')).toEqual([]);
  });

  it('strips abbreviation from company name', () => {
    expect(tokenize('BANCO DE CREDITO E.C.A.')).toEqual(['banco', 'credito']);
  });

  it('strips S.A.C. from company name', () => {
    expect(tokenize('PERU ACTIVA S.A.C.')).toEqual(['peru', 'activa']);
  });

  it('strips E.I.R.L. from company name', () => {
    expect(tokenize('GLORIANA TEXTILES E.I.R.L.')).toEqual(['gloriana', 'textiles']);
  });

  it('does not collapse multi-char words', () => {
    const tokens = tokenize('BANCO.');
    expect(tokens).toContain('banco');
  });
});
