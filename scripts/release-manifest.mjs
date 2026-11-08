import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2] ?? 'release';
const tag = process.env.GITHUB_REF_NAME;
const config = JSON.parse(readFileSync(new URL('../release-repository.json', import.meta.url), 'utf8'));
if (!/^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(tag ?? '') || process.env.GITHUB_REPOSITORY !== config.repository) throw new Error('tag or repository does not match release configuration');
const version = tag.slice(1);
const keys = ['linux-x86_64', 'linux-aarch64', 'windows-x86_64', 'darwin-x86_64', 'darwin-aarch64'];
const platforms = keys.map(key => {
  const file = `polaris-agent-${version}-${key}${key.startsWith('windows-') ? '.exe' : ''}`;
  const bytes = readFileSync(join(dir, file));
  return { key, file, version, size: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') };
});
writeFileSync(join(dir, 'manifest.json'), JSON.stringify({ repository: config.repository, tag, version, platforms }, null, 2) + '\n');
