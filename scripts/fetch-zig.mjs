#!/usr/bin/env node
/**
 * Fetch the Zig toolchain into .tools/, for cross-compiling the probe.
 *
 * Zig is here for one job: it ships a clang+lld that targets musl, which is
 * what `ring` (the crypto backend under rustls) needs to assemble for a
 * foreign architecture. Nothing else in the probe tree is C, so this is the
 * whole cross-compilation story - no per-arch musl-gcc packages, no Docker
 * build container, no SDK per target.
 *
 * Only needed when building Linux probes from a non-Linux host. On Linux CI
 * the system gcc handles it, and `scripts/build-agent.mjs` says so and skips
 * this.
 *
 * Usage: node scripts/fetch-zig.mjs [--force]
 *
 * 将 Zig 下载到 .tools/，供探针交叉编译。Zig 提供支持 musl 的 clang 和 lld，
 * 供 rustls 下的 ring 在其他架构编译汇编；探针其他部分没有 C，因此无需逐架构 musl-gcc、
 * Docker 构建容器或每个目标的 SDK。仅非 Linux 主机构建 Linux 探针需要此工具；
 * Linux CI 使用系统 gcc，scripts/build-agent.mjs 会检测并跳过下载。
 * 用法：node scripts/fetch-zig.mjs [--force]。
 */

import { execFileSync } from 'node:child_process';
import { createWriteStream, existsSync, mkdirSync, rmSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const TOOLS = join(ROOT, '.tools');
const ZIG_DIR = join(TOOLS, 'zig');

/**
 * Pinned deliberately. Zig's CLI and its target triples move between minor
 * releases - `-target x86_64-linux-musl` has changed spelling more than once -
 * so an unpinned download makes the build depend on whatever shipped that
 * week. Bump this and re-run the build when a newer one is wanted.
 * 固定版本以避免 Zig CLI 和目标三元组的次版本变化影响构建；例如
 * -target x86_64-linux-musl 的写法曾多次改变。需要新版本时更新此处并重新构建。
 */
const ZIG_VERSION = '0.13.0';

/** No Windows ARM64 or 32-bit hosts: the probe build is x64/arm64 only. / 不支持 Windows ARM64 或 32 位主机；探针只构建 x64/arm64。 */
function zigArchive() {
  const platform = process.platform;
  const arch = process.arch;
  const name =
    platform === 'win32'
      ? `zig-windows-${arch === 'arm64' ? 'aarch64' : 'x86_64'}-${ZIG_VERSION}`
      : platform === 'darwin'
        ? `zig-macos-${arch === 'arm64' ? 'aarch64' : 'x86_64'}-${ZIG_VERSION}`
        : `zig-linux-${arch === 'arm64' ? 'aarch64' : 'x86_64'}-${ZIG_VERSION}`;
  const ext = platform === 'win32' ? 'zip' : 'tar.xz';
  return { name, ext, url: `https://ziglang.org/download/${ZIG_VERSION}/${name}.${ext}` };
}

function alreadyInstalled() {
  const exe = process.platform === 'win32' ? 'zig.exe' : 'zig';
  const p = join(ZIG_DIR, exe);
  if (!existsSync(p)) return null;
  try {
    const v = execFileSync(p, ['version'], { encoding: 'utf8' }).trim();
    return v === ZIG_VERSION ? p : null;
  } catch {
    return null;
  }
}

async function download(url, dest) {
  process.stdout.write(`downloading ${url}\n`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  const total = Number(res.headers.get('content-length') ?? 0);
  let seen = 0;
  let lastPrint = 0;

  const body = Readable.fromWeb(res.body);
  body.on('data', (chunk) => {
    seen += chunk.length;
    const pct = total ? Math.floor((seen / total) * 100) : 0;
    // Once a percent, not once a chunk: this is a ~45 MB download and a line
    // per chunk would bury the rest of the output.
    // 下载约 45 MB；每增加一个百分点输出一次，避免分块日志淹没其他内容。
    if (pct >= lastPrint + 10) {
      lastPrint = pct;
      process.stdout.write(`  ${pct}%\n`);
    }
  });

  await pipeline(body, createWriteStream(dest));
  return statSync(dest).size;
}

async function main() {
  const force = process.argv.includes('--force');

  mkdirSync(TOOLS, { recursive: true });

  if (!force) {
    const found = alreadyInstalled();
    if (found) {
      console.log(`zig ${ZIG_VERSION} already at ${found}`);
      return;
    }
  }

  const { name, ext, url } = zigArchive();
  const archive = join(TOOLS, `${name}.${ext}`);

  const size = await download(url, archive);
  console.log(`  ${(size / 1048576).toFixed(1)} MB`);

  // Extract. Windows has tar (bsdtar) in system32 from Windows 10 on, so the
  // same call works everywhere and avoids a zip dependency.
  // Windows 10 起 system32 自带 tar，因此可统一解压而无需 zip 依赖。
  rmSync(ZIG_DIR, { recursive: true, force: true });
  mkdirSync(ZIG_DIR, { recursive: true });
  console.log(`extracting into ${ZIG_DIR}`);
  execFileSync('tar', ['-xf', archive, '-C', ZIG_DIR, '--strip-components=1'], {
    stdio: 'inherit',
  });

  rmSync(archive, { force: true });

  const found = alreadyInstalled();
  if (!found) {
    console.error('extraction finished but zig does not report the expected version');
    process.exit(1);
  }
  console.log(`\nzig ${ZIG_VERSION} ready: ${found}`);
  console.log('now run: npm run build');
}

main().catch((e) => {
  console.error(`\nfailed: ${e.message}`);
  console.error('The Linux probe cannot be cross-compiled without a C toolchain.');
  console.error('On a Linux host the system gcc is enough and this step is unnecessary.');
  process.exit(1);
});
