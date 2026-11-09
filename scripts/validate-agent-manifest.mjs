#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const manifestPath = resolve(process.argv[2] ?? join(root, 'dist', 'manifest.json'));
const version = readFileSync(join(root, 'Cargo.toml'), 'utf8').match(/^version\s*=\s*"([^\"]+)"/m)?.[1];
const allowed = new Set(['linux-x86_64', 'linux-aarch64', 'windows-x86_64', 'darwin-x86_64', 'darwin-aarch64']);

function fail(message) {
  console.error(message);
  process.exit(1);
}

let manifest;
try {
  manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
} catch (error) {
  fail(`invalid manifest: ${error.message}`);
}

if (!version || manifest.version !== version || !Array.isArray(manifest.platforms) || !manifest.platforms.length) {
  fail('manifest version or platform list does not match Cargo.toml');
}

const dir = dirname(manifestPath);
const seen = new Set();
for (const entry of manifest.platforms) {
  const ext = entry.key === 'windows-x86_64' ? '.exe' : '';
  const expectedFile = `polaris-agent-${version}-${entry.key}${ext}`;
  if (!allowed.has(entry.key) || seen.has(entry.key) || entry.file !== expectedFile || entry.version !== manifest.version ||
      !Number.isSafeInteger(entry.size) || entry.size <= 0 || !/^[a-f0-9]{64}$/.test(entry.sha256)) {
    fail(`invalid release metadata for ${entry.key ?? 'unknown platform'}`);
  }
  seen.add(entry.key);
  let bytes;
  try {
    bytes = readFileSync(join(dir, entry.file));
  } catch {
    fail(`missing release file ${entry.file}`);
  }
  if (bytes.length !== entry.size || createHash('sha256').update(bytes).digest('hex') !== entry.sha256 || statSync(join(dir, entry.file)).size !== entry.size) {
    fail(`size or SHA-256 mismatch for ${entry.file}`);
  }
}
console.log(`validated ${manifest.platforms.length} Agent release artifact(s) for ${version}`);
