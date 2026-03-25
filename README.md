# @latinfo/engine

Binary lookup, inverted-index search, and MPHF engine for Latin American business registries. Storage-agnostic — bring your own backend (R2, S3, filesystem, in-memory).

## Install

```bash
npm install @latinfo/engine
```

## Usage

```ts
import { lookupId, searchByName, findSource } from '@latinfo/engine';
import type { Storage } from '@latinfo/engine';

// Implement the Storage interface for your backend
const storage: Storage = {
  async get(key, options) {
    // Return { arrayBuffer() } or null
  }
};

const source = findSource('pe', 'sunat', 'padron')!;

// Lookup by ID
const record = await lookupId(storage, source, '20100047218');

// Search by name
const results = await searchByName(storage, source, 'banco de credito');
```

## API

### Lookup
- `lookupId(storage, source, id)` — O(log n) binary search
- `loadIndex(storage, source)` — preload prefix index

### Search
- `searchByName(storage, source, query)` — inverted index with TF-IDF scoring

### Licitaciones
- `searchLicitaciones(storage, query)` — procurement search with filters

### MPHF
- `buildMphfFile(terms, totalDocs, entrySize)` — build minimal perfect hash
- `serializeMphf(mphf)` / `deserializeMphf(buf)` — serialize/deserialize
- `mphfExactLookup(mphf, term)` — O(1) term lookup
- `mphfResolveToken(mphf, token, isLast)` — prefix-aware resolution

### Tokenizer
- `tokenize(text, stopWords?)` — normalize, split, filter stop words
- `STOP_WORDS_BY_LANG` — stop word sets for ES, PT, EN

### Sources
- `findSource(country, institution, dataset)` — find source config
- `dniToRuc(dni)` — Peru DNI to RUC conversion

### Storage Interface

```ts
interface Storage {
  get(key: string, options?: {
    range?: { offset: number; length: number }
  }): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>;
}
```

## Build Tools

```ts
import { buildBinaryFiles } from '@latinfo/engine/build/binary';
import { buildSearchIndex } from '@latinfo/engine/build/search-index';
```

## License

AGPL-3.0-only
