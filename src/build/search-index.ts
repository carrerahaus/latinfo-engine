/**
 * Build inverted search index from a pre-sorted TSV file.
 *
 * V1 (default): posting lists store uint64 IDs only (8 bytes/entry).
 * V2 (statusFieldIndex provided): posting lists store ID + name + status
 *   inline (110 bytes/entry, magic "LSRY"). Enables zero-lookup search.
 *
 * 3-pass streaming pipeline:
 *   1. Tokenize — stream sorted.tsv, emit pairs to temp file
 *   2. Sort — LC_ALL=C sort pairs by term+ID
 *   3. Build — stream sorted pairs, group by term, write .idx + posting .dat shards
 */

import { createReadStream, createWriteStream, WriteStream } from 'fs';
import * as readline from 'readline';
import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';
import { tokenize } from '../tokenize.js';

const MAGIC_V1 = Buffer.from('LSRX');
const MAGIC_V2 = Buffer.from('LSRY');
const HEADER_SIZE = 16;
const ENTRY_SIZE = 32;
const TERM_LEN = 22;
const MAX_SHARD_BYTES = 250 * 1024 * 1024;

const ENTRY_SIZE_V2 = 110;
const NAME_MAX = 80;
const STATUS_MAX = 20;

export interface SearchBuildConfig {
  searchFieldIndex: number;
  idRegex: RegExp;
  statusFieldIndex?: number;
}

interface PairEntry {
  id: bigint;
  name: string;
  status: string;
}

async function emitPairs(
  sortedTsvPath: string,
  pairsPath: string,
  config: SearchBuildConfig,
): Promise<number> {
  const isV2 = config.statusFieldIndex !== undefined;
  const rl = readline.createInterface({
    input: createReadStream(sortedTsvPath, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });
  const out = createWriteStream(pairsPath, { encoding: 'utf-8' });

  let pairCount = 0;

  for await (const line of rl) {
    const tab = line.indexOf('\t');
    if (tab === -1) continue;

    const id = line.substring(0, tab);
    if (!config.idRegex.test(id)) continue;

    const rest = line.substring(tab + 1);
    let cols: string[] | null = null;

    let searchText: string;
    if (config.searchFieldIndex === 0) {
      const nextTab = rest.indexOf('\t');
      searchText = nextTab === -1 ? rest : rest.substring(0, nextTab);
    } else {
      cols = rest.split('\t');
      searchText = cols[config.searchFieldIndex] || '';
    }

    let inlineName = '';
    let inlineStatus = '';
    if (isV2) {
      if (!cols) cols = rest.split('\t');
      inlineName = (cols[config.searchFieldIndex] ?? searchText).substring(0, NAME_MAX);
      inlineStatus = (cols[config.statusFieldIndex!] || '').substring(0, STATUS_MAX);
    }

    const tokens = tokenize(searchText);
    for (const token of tokens) {
      if (token.length > TERM_LEN) continue;
      if (isV2) {
        out.write(`${token}\t${id}\t${inlineName}\t${inlineStatus}\n`);
      } else {
        out.write(`${token}\t${id}\n`);
      }
      pairCount++;
    }
  }

  await new Promise<void>((resolve, reject) => {
    out.end(() => resolve());
    out.on('error', reject);
  });

  return pairCount;
}

function sortPairs(pairsPath: string, sortedPairsPath: string, tempDir: string): void {
  execSync(`LC_ALL=C sort -t'\t' -k1,1 -k2,2 -u "${pairsPath}" -o "${sortedPairsPath}"`, {
    stdio: 'inherit',
    env: { ...process.env, TMPDIR: tempDir },
  });
}

async function buildIndex(
  sortedPairsPath: string,
  outDir: string,
  baseName: string,
  totalDocs: number = 0,
  isV2: boolean = false,
): Promise<{ idxPath: string; shardPaths: string[] }> {
  const rl = readline.createInterface({
    input: createReadStream(sortedPairsPath, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });

  const ENTRY_BYTES = isV2 ? ENTRY_SIZE_V2 : 8;
  const terms: { term: string; shard: number; offset: number; count: number }[] = [];
  const shardPaths: string[] = [];

  let currentShard = 0;
  let shardPath = path.join(outDir, `${baseName}-search-${currentShard}.dat`);
  let stream: WriteStream = createWriteStream(shardPath);
  shardPaths.push(shardPath);
  let shardOffset = 0;

  let currentTerm = '';
  let currentEntries: PairEntry[] = [];

  const finishShard = (): Promise<void> =>
    new Promise((resolve, reject) => {
      stream.end(() => resolve());
      stream.on('error', reject);
    });

  const flushTerm = () => {
    if (!currentTerm || currentEntries.length === 0) return;

    const postingBytes = currentEntries.length * ENTRY_BYTES;
    terms.push({ term: currentTerm, shard: currentShard, offset: shardOffset, count: currentEntries.length });

    const buf = Buffer.alloc(postingBytes);
    for (let i = 0; i < currentEntries.length; i++) {
      const e = currentEntries[i];
      const base = i * ENTRY_BYTES;
      if (isV2) {
        buf.writeBigUInt64LE(e.id, base);
        const nameBytes = Buffer.from(e.name, 'utf-8').subarray(0, NAME_MAX);
        buf.writeUInt8(nameBytes.length, base + 8);
        nameBytes.copy(buf, base + 9);
        const statusBytes = Buffer.from(e.status, 'utf-8').subarray(0, STATUS_MAX);
        buf.writeUInt8(statusBytes.length, base + 89);
        statusBytes.copy(buf, base + 90);
      } else {
        buf.writeBigUInt64LE(e.id, base);
      }
    }
    stream.write(buf);
    shardOffset += postingBytes;
  };

  for await (const line of rl) {
    const tab = line.indexOf('\t');
    if (tab === -1) continue;

    const term = line.substring(0, tab);
    const rest = line.substring(tab + 1);

    let id: string;
    let name = '';
    let status = '';

    if (isV2) {
      const parts = rest.split('\t');
      id = parts[0];
      name = parts[1] || '';
      status = parts[2] || '';
    } else {
      id = rest;
    }

    if (term !== currentTerm) {
      flushTerm();

      if (shardOffset >= MAX_SHARD_BYTES) {
        await finishShard();
        currentShard++;
        shardPath = path.join(outDir, `${baseName}-search-${currentShard}.dat`);
        stream = createWriteStream(shardPath);
        shardPaths.push(shardPath);
        shardOffset = 0;
      }

      currentTerm = term;
      currentEntries = [];
    }

    currentEntries.push({ id: BigInt(id), name, status });
  }

  flushTerm();
  await finishShard();

  // Write .idx
  const idxPath = path.join(outDir, `${baseName}-search.idx`);
  const idxSize = HEADER_SIZE + terms.length * ENTRY_SIZE;
  const idxBuf = Buffer.alloc(idxSize);

  const magic = isV2 ? MAGIC_V2 : MAGIC_V1;
  magic.copy(idxBuf, 0);
  idxBuf.writeUInt32LE(terms.length, 4);
  idxBuf.writeUInt32LE(currentShard + 1, 8);
  idxBuf.writeUInt32LE(totalDocs, 12);

  for (let i = 0; i < terms.length; i++) {
    const off = HEADER_SIZE + i * ENTRY_SIZE;
    const termBuf = Buffer.alloc(TERM_LEN);
    termBuf.write(terms[i].term, 0, TERM_LEN, 'utf-8');
    termBuf.copy(idxBuf, off);
    idxBuf.writeUInt8(terms[i].shard, off + TERM_LEN);
    idxBuf.writeUInt32LE(terms[i].offset, off + TERM_LEN + 2);
    idxBuf.writeUInt32LE(terms[i].count, off + TERM_LEN + 6);
  }

  fs.writeFileSync(idxPath, idxBuf);

  return { idxPath, shardPaths };
}

export async function buildSearchIndex(
  sortedTsvPath: string,
  outDir: string,
  baseName: string,
  config: SearchBuildConfig,
  totalDocs: number = 0,
): Promise<{ idxPath: string; shardPaths: string[] }> {
  const isV2 = config.statusFieldIndex !== undefined;

  const pairsPath = path.join(outDir, 'search-pairs.tsv');
  const sortedPairsPath = path.join(outDir, 'search-pairs-sorted.tsv');

  await emitPairs(sortedTsvPath, pairsPath, config);
  sortPairs(pairsPath, sortedPairsPath, outDir);
  fs.unlinkSync(pairsPath);

  const result = await buildIndex(sortedPairsPath, outDir, baseName, totalDocs, isV2);
  fs.unlinkSync(sortedPairsPath);

  return result;
}
