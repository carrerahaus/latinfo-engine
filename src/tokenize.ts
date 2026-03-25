const STOP_WORDS_ES = new Set([
  'de', 'del', 'la', 'el', 'las', 'los', 'en', 'con', 'por',
  'para', 'al', 'un', 'una', 'su', 'sus', 'y', 'e', 'o',
  // Legal entity types (post-abbreviation collapsing)
  'sa', 'sac', 'srl', 'eirl', 'eca', 'saa', 'scrl',
]);

const STOP_WORDS_PT = new Set([
  'de', 'do', 'da', 'dos', 'das', 'em', 'no', 'na', 'nos', 'nas',
  'um', 'uma', 'com', 'por', 'para', 'ao', 'aos', 'seu', 'sua', 'e', 'ou',
]);

const STOP_WORDS_EN = new Set([
  'the', 'of', 'and', 'in', 'to', 'for', 'is', 'on', 'at', 'by',
  'an', 'be', 'or', 'as', 'it', 'its', 'with', 'from',
]);

export const STOP_WORDS_BY_LANG: Record<string, Set<string>> = {
  es: STOP_WORDS_ES,
  pt: STOP_WORDS_PT,
  en: STOP_WORDS_EN,
};

// Default: Spanish (backwards compatible)
const DEFAULT_STOP_WORDS = STOP_WORDS_ES;

function normalize(text: string): string {
  return text
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\b([a-z])(?:\.([a-z]))+\.?\b/g, m => m.replace(/\./g, ''));
}

function splitWords(text: string): string[] {
  return normalize(text).split(/[^a-z0-9]+/).filter(w => w.length >= 2);
}

export function tokenize(text: string, stopWords: Set<string> = DEFAULT_STOP_WORDS): string[] {
  return [...new Set(splitWords(text).filter(w => !stopWords.has(w)))];
}

export function tokenizeKeepStopWords(text: string): string[] {
  return splitWords(text);
}

export function isAllStopWords(query: string, stopWords: Set<string> = DEFAULT_STOP_WORDS): boolean {
  const words = tokenizeKeepStopWords(query);
  return words.length > 0 && words.every(w => stopWords.has(w));
}
