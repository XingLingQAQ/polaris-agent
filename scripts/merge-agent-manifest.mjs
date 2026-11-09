#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const dir = resolve(process.argv[2] ?? join(root, 'dist'));
const version = readFileSync(join(root, 'Cargo.toml'), 'utf8').match(/^version\s*=\s*"([^\"]+)"/m)?.[1];
const targets = [
  ['linux-x86_64', ''],
  ['linux-aarch64', ''],
  ['windows-x86_64', '.exe'],
  ['darwin-x86_64', ''],
  ['darwin-aarch64', ''],
];

if (!version) throw new Error('Cargo.toml has no Agent version');
const platforms = targets.map(([key, ext]) => {
  const file = `polaris-agent-${version}-${key}${ext}`;
  const path = join(dir, file);
  if (!existsSync(path)) throw new Error(`missing matrix artifact ${file}`);
  const bytes = readFileSync(path);
  copyFileSync(path, join(dir, `polaris-agent-${key}${ext}`));
  return { key, file, size: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex'), version };
});
writeFileSync(join(dir, 'manifest.json'), JSON.stringify({ version, platforms }, null, 2) + '\n');
console.log(`merged ${platforms.length} Agent artifacts for ${version}`);
