// 验证发布清单的合并与拒绝条件。
// Check release manifest merging and rejection conditions.

import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const temp = mkdtempSync(join(tmpdir(), 'uptime-polaris-manifest-'));
const targets = [
  ['linux-x86_64', ''], ['linux-aarch64', ''], ['windows-x86_64', '.exe'],
  ['darwin-x86_64', ''], ['darwin-aarch64', ''],
];
const version = readFileSync(join(root, 'Cargo.toml'), 'utf8').match(/^version\s*=\s*"([^"]+)"/m)[1];

function run(script, path = temp) {
  return spawnSync(process.execPath, [join(root, 'scripts', script), path], { encoding: 'utf8' });
}

try {
  for (const [key, ext] of targets) {
    writeFileSync(join(temp, `polaris-agent-${version}-${key}${ext}`), `fixture:${key}`);
  }
  const merged = run('merge-agent-manifest.mjs');
  assert.equal(merged.status, 0, merged.stderr);
  const manifest = JSON.parse(readFileSync(join(temp, 'manifest.json'), 'utf8'));
  assert.equal(manifest.version, version);
  assert.equal(manifest.platforms.length, targets.length);
  assert.equal(manifest.platforms.find((entry) => entry.key === 'windows-x86_64').file, `polaris-agent-${version}-windows-x86_64.exe`);
  for (const [key, ext] of targets) {
    assert.equal(readFileSync(join(temp, `polaris-agent-${key}${ext}`), 'utf8'), `fixture:${key}`);
  }
  const manifestPath = join(temp, 'manifest.json');
  const valid = run('validate-agent-manifest.mjs', manifestPath);
  assert.equal(valid.status, 0, valid.stderr);

  const corruptFile = join(temp, manifest.platforms[0].file);
  writeFileSync(corruptFile, 'truncated');
  const truncated = run('validate-agent-manifest.mjs', manifestPath);
  assert.equal(truncated.status, 1);
  assert.match(truncated.stderr, /size or SHA-256 mismatch/);
  writeFileSync(corruptFile, `fixture:${manifest.platforms[0].key}`);

  manifest.platforms[0].key = 'windows-x86_64';
  writeFileSync(join(temp, 'manifest.json'), JSON.stringify(manifest));
  const wrongPlatform = run('validate-agent-manifest.mjs', manifestPath);
  assert.equal(wrongPlatform.status, 1);
  assert.match(wrongPlatform.stderr, /invalid release metadata/);
  assert.ok(isAbsolute(temp));
} finally {
  const tempRoot = resolve(tmpdir());
  const rel = relative(tempRoot, resolve(temp));
  if (!isAbsolute(temp) || !rel || rel.startsWith(`..${sep}`) || rel === '..') {
    throw new Error(`refusing cleanup outside temp root: ${temp}`);
  }
  rmSync(temp, { recursive: true, force: true });
}

console.log('Agent release manifest merge and validation passed');
