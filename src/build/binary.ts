/**
 * Build binary shard files + index from a pre-sorted TSV file.
 *
 * Binary format:
 *   .bin shards — records sorted by primary ID, split at prefix boundaries (~250MB each)
 *     {baseName}-0.bin, {baseName}-1.bin, ...
 *     Each record:
 *       uint16 LE: total record length (including this field)
 *       N bytes ASCII: primary ID (fixed length from config)
 *       fieldCount × (uint8 length + N bytes UTF-8): variable-length fields
 *
 *   .idx — prefix index:
 *     Header (16 bytes): magic "LIDX" + uint32 entry count + uint32 shard count + 4 reserved
 *     Entries (16 bytes each, sorted by prefix):
 *       uint32 LE: prefix (first prefixLength digits of ID)
 *       uint32 LE: shard index (0-based)
 *       uint32 LE: byte offset within shard
 *       uint32 LE: byte length of prefix group in shard
 */

import { createReadStream, createWriteStream, WriteStream } from 'fs';
import * as readline from 'readline';
import * as fs from 'fs';
import * as path from 'path';

const MAGIC = Buffer.from('LIDX');
const HEADER_SIZE = 16;
const ENTRY_SIZE = 16;
const MAX_SHARD_BYTES = 200 * 1024 * 1024; // 200MB per shard

export interface BinaryBuildConfig {
  idLength: number;
  idRegex: RegExp;
  prefixLength: number;
  fieldCount: number;
}

function encodeRecord(fields: string[], config: BinaryBuildConfig): Buffer {
  const id = fields[0];

  const fieldBufs: Buffer[] = [];
  for (let i = 1; i <= config.fieldCount; i++) {
    const val = i < fields.length ? fields[i] : '';
    const encoded = Buffer.from(val, 'utf-8');
    let len = Math.min(encoded.length, 255);
    // Don't cut a multi-byte UTF-8 character in half
    while (len > 0 && (encoded[len] & 0xC0) === 0x80) len--;
    const lenByte = Buffer.alloc(1);
    lenByte.writeUInt8(len);
    fieldBufs.push(lenByte, encoded.subarray(0, len));
  }

  const fieldsLen = fieldBufs.reduce((s, b) => s + b.length, 0);
  const recordLen = 2 + config.idLength + fieldsLen;

  const header = Buffer.alloc(2 + config.idLength);
  header.writeUInt16LE(recordLen, 0);
  header.write(id, 2, config.idLength, 'ascii');

  return Buffer.concat([header, ...fieldBufs]);
}

interface PrefixEntry {
  prefix: number;
  shard: number;
  offset: number;
  length: number;
}

/**
 * Stream a sorted TSV file and produce sharded .bin files + .idx.
 */
export async function buildBinaryFiles(
  sortedTsvPath: string,
  outDir: string,
  baseName: string,
  config: BinaryBuildConfig,
): Promise<{ shardPaths: string[]; idxPath: string; recordCount: number }> {
  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true });
  }

  const prefixes: PrefixEntry[] = [];
  const shardPaths: string[] = [];

  let currentShard = 0;
  let shardPath = path.join(outDir, `${baseName}-${currentShard}.bin`);
  let stream: WriteStream = createWriteStream(shardPath);
  shardPaths.push(shardPath);

  let shardOffset = 0;
  let currentPrefix = -1;
  let prefixStart = 0;
  let recordCount = 0;

  const finishShard = (): Promise<void> =>
    new Promise((resolve, reject) => {
      stream.end(() => resolve());
      stream.on('error', reject);
    });

  const rl = readline.createInterface({
    input: createReadStream(sortedTsvPath, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const fields = line.split('\t');
    if (fields.length < 1 || !config.idRegex.test(fields[0])) continue;

    const prefix = parseInt(fields[0].substring(0, config.prefixLength));

    // New prefix and shard is over limit → start new shard
    if (prefix !== currentPrefix && shardOffset >= MAX_SHARD_BYTES) {
      if (currentPrefix !== -1) {
        prefixes.push({ prefix: currentPrefix, shard: currentShard, offset: prefixStart, length: shardOffset - prefixStart });
      }

      await finishShard();
      currentShard++;
      shardPath = path.join(outDir, `${baseName}-${currentShard}.bin`);
      stream = createWriteStream(shardPath);
      shardPaths.push(shardPath);
      shardOffset = 0;
      currentPrefix = -1;
    }

    const record = encodeRecord(fields, config);
    stream.write(record);

    if (prefix !== currentPrefix) {
      if (currentPrefix !== -1) {
        prefixes.push({ prefix: currentPrefix, shard: currentShard, offset: prefixStart, length: shardOffset - prefixStart });
      }
      currentPrefix = prefix;
      prefixStart = shardOffset;
    }

    shardOffset += record.length;
    recordCount++;
  }

  // Flush last prefix + shard
  if (currentPrefix !== -1) {
    prefixes.push({ prefix: currentPrefix, shard: currentShard, offset: prefixStart, length: shardOffset - prefixStart });
  }
  await finishShard();

  // Write .idx
  const idxPath = path.join(outDir, `${baseName}.idx`);
  const idxBuf = Buffer.alloc(HEADER_SIZE + prefixes.length * ENTRY_SIZE);
  MAGIC.copy(idxBuf, 0);
  idxBuf.writeUInt32LE(prefixes.length, 4);
  idxBuf.writeUInt32LE(currentShard + 1, 8);

  for (let i = 0; i < prefixes.length; i++) {
    const off = HEADER_SIZE + i * ENTRY_SIZE;
    idxBuf.writeUInt32LE(prefixes[i].prefix, off);
    idxBuf.writeUInt32LE(prefixes[i].shard, off + 4);
    idxBuf.writeUInt32LE(prefixes[i].offset, off + 8);
    idxBuf.writeUInt32LE(prefixes[i].length, off + 12);
  }

  fs.writeFileSync(idxPath, idxBuf);

  return { shardPaths, idxPath, recordCount };
}
