#!/usr/bin/env node
/**
 * 构建选定平台的本地 Agent 制品和清单，默认写入 dist/。
 * Build local Agent artifacts and a manifest for selected platforms, defaulting to dist/.
 * 这不是站点的 Release 下载来源；可用参数见下方 TARGETS，--current 按主机系统筛选。
 * These files are not the site’s release download source; TARGETS lists platform keys and --current filters by host OS.
 */

import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const AGENT_DIR = ROOT;
const OUT_DIR = resolve(process.env.AGENT_OUT_DIR ?? join(ROOT, 'dist'));
const TARGET_DIR = resolve(AGENT_DIR, process.env.CARGO_TARGET_DIR ?? 'target');
const PACKAGE_VERSION = readFileSync(join(AGENT_DIR, 'Cargo.toml'), 'utf8').match(/^version\s*=\s*"([^"]+)"/m)?.[1] ?? 'unknown';

/**
 * The matrix. `key` is the installer-facing name; `triple` is the Rust target.
 *
 * Windows and macOS are built natively only when running on that host - cargo
 * cannot cross-compile to them without the platform SDK, which is a much
 * heavier dependency than the Zig linker the Linux targets need. So on a
 * Linux CI runner the matrix produces the Linux artifacts and skips the rest
 * with a printed note, rather than failing the whole build.
 * 构建矩阵中的 key 面向安装器，triple 是 Rust 目标。Windows 和 macOS 需要各自的 SDK，
 * 其依赖比 Linux 目标使用的 Zig 链接器重，因此仅在对应主机上原生构建。
 * Linux CI 会构建 Linux 制品，跳过其他平台并输出提示，而非使整个构建失败。
 */
const TARGETS = [
  { key: 'linux-x86_64', triple: 'x86_64-unknown-linux-musl', ext: '', hostOs: 'linux', static: true },
  { key: 'linux-aarch64', triple: 'aarch64-unknown-linux-musl', ext: '', hostOs: 'linux', static: true },
  { key: 'windows-x86_64', triple: 'x86_64-pc-windows-msvc', ext: '.exe', hostOs: 'win32', static: false },
  { key: 'darwin-x86_64', triple: 'x86_64-apple-darwin', ext: '', hostOs: 'darwin', static: false },
  { key: 'darwin-aarch64', triple: 'aarch64-apple-darwin', ext: '', hostOs: 'darwin', static: false },
];

function run(cmd, args, opts = {}) {
  return execFileSync(cmd, args, {
    cwd: AGENT_DIR,
    stdio: 'inherit',
    // No `shell: true`. Going through cmd.exe re-parses the environment block,
    // and a target-specific variable like
    // `CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER` (a path with backslashes
    // on Windows) survives that trip only by luck. execFile passes the env
    // through untouched - and cargo/cargo.exe resolves fine without a shell.
    // 不使用 shell: true：cmd.exe 会重新解析环境块，目标变量如
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER 在 Windows 上可能包含反斜杠，不能依赖其恰好保留。
    // execFile 原样传递环境，且无需 shell 即可解析 cargo/cargo.exe。
    ...opts,
  });
}

/** Is an executable on PATH? / 可执行文件是否位于 PATH 中？ */
function has(cmd) {
  try {
    execFileSync(process.platform === 'win32' ? 'where' : 'which', [cmd], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

/**
 * Find a C compiler for the musl target.
 *
 * `ring` (the crypto backend under rustls) compiles a little assembly, so a
 * cross C toolchain is genuinely required even though nothing else in the
 * tree is C. Zig is the smallest way to get one: it is a single download that
 * ships a full clang+lld and knows every musl target, where the alternative is
 * a per-arch `musl-gcc` package that most machines do not have.
 * 为 musl 目标查找 C 编译器。虽然项目其他部分不是 C，ring 的汇编仍需要跨平台 C 工具链。
 * Zig 一次下载即可提供支持所有 musl 目标的 clang 和 lld，无需逐架构安装常见主机没有的 musl-gcc。
 */
function findZig() {
  if (process.env.ZIG && existsSync(process.env.ZIG)) return process.env.ZIG;
  if (has('zig')) return 'zig';
  const local = join(ROOT, '.tools', 'zig', 'zig.exe');
  if (existsSync(local)) return local;
  const localUnix = join(ROOT, '.tools', 'zig', 'zig');
  if (existsSync(localUnix)) return localUnix;
  return null;
}

function main() {
  const argv = process.argv.slice(2);
  const onlyCurrent = argv.includes('--current');
  const named = argv.filter((a) => !a.startsWith('--'));
  const explicitlyRequested = named.length > 0;

  let targets = TARGETS;
  if (onlyCurrent) {
    targets = TARGETS.filter((t) => t.hostOs === process.platform);
  } else if (named.length > 0) {
    targets = TARGETS.filter((t) => named.includes(t.key));
    const unknown = named.filter((n) => !TARGETS.some((t) => t.key === n));
    if (unknown.length > 0) {
      console.error(`unknown target(s): ${unknown.join(', ')}`);
      console.error(`known: ${TARGETS.map((t) => t.key).join(', ')}`);
      process.exit(2);
    }
  }

  mkdirSync(OUT_DIR, { recursive: true });
  const built = [];
  const skipped = [];

  for (const t of targets) {
    // Linux is the one platform we cross-compile to from anywhere; the other
    // two need their own SDK, so they only build on their own host.
      // Linux 支持跨平台构建；其他两个平台需要各自的 SDK。
    const crossable = t.hostOs === 'linux';
    if (!crossable && t.hostOs !== process.platform) {
      if (explicitlyRequested) {
        console.error(`${t.key} requires a ${t.hostOs} host or SDK`);
        process.exit(1);
      }
      skipped.push({ key: t.key, why: `needs a ${t.hostOs} host or SDK` });
      continue;
    }

    console.log(`\n=== ${t.key} (${t.triple}) ===`);

    try {
      run('rustup', ['target', 'add', t.triple]);
    } catch {
      // Already installed is the common case; a genuine failure surfaces on
      // the build below with a clearer message than rustup's.
      // 通常目标已经安装；真正的失败会在后续构建中给出更清楚的提示。
    }

    const env = { ...process.env };
    env.CARGO_TARGET_DIR = TARGET_DIR;
    const nativeLinuxArch = process.arch === 'arm64' ? 'aarch64' : 'x86_64';
    const targetLinuxArch = t.key === 'linux-aarch64' ? 'aarch64' : 'x86_64';
    const needsZig = crossable && (process.platform !== 'linux' || nativeLinuxArch !== targetLinuxArch);
    if (needsZig) {
      const zig = findZig();
      if (!zig) {
        skipped.push({
          key: t.key,
          why: 'no C cross-compiler. Run `node scripts/fetch-zig.mjs`, or set $ZIG',
        });
        continue;
      }
      /**
       * Point cargo at Zig for this target, without touching PATH.
       *
       * Zig's `cc` is a full clang+lld, so it serves as both the C compiler
       * (for ring's assembly) and the linker (for the final binary). Both are
       * named explicitly rather than installed as a `<triple>-gcc` on PATH:
       * on Windows the environment variable is `Path`, so writing `env.PATH`
       * adds a *second*, empty-search-path entry and cmd.exe then cannot find
       * cargo at all.
       *
       * cc-rs reads `CC_<target>` with dashes as underscores, and rustc reads
       * `CARGO_TARGET_<TRIPLE>_LINKER`.
       *
       * 为 cargo 显式指定 Zig 编译器和链接器，不改 PATH。Zig cc 同时提供 clang 和 lld，
       * 可编译 ring 汇编并链接最终二进制，无需向 PATH 安装 <triple>-gcc。
       * Windows 使用 Path，写入 env.PATH 会新增一个空搜索路径变量，导致 cmd.exe 找不到 cargo。
       * cc-rs 使用连字符替换为下划线的 CC_<target>，rustc 使用 CARGO_TARGET_<TRIPLE>_LINKER。
       */
      const zigTarget = t.triple.replace('unknown-linux-musl', 'linux-musl');
      const wrapperDir = join(ROOT, '.tools', 'cc');
      mkdirSync(wrapperDir, { recursive: true });
      const wrapperName = process.platform === 'win32' ? `${t.triple}-gcc.cmd` : `${t.triple}-gcc`;
      const wrapper = join(wrapperDir, wrapperName);
      writeWrapper(wrapper, zig, zigTarget);
      const arWrapper = join(wrapperDir, `${t.triple}-ar${process.platform === 'win32' ? '.cmd' : ''}`);
      writeArWrapper(arWrapper, zig);

      env[`CC_${t.triple.replace(/-/g, '_')}`] = wrapper;
      env[`CARGO_TARGET_${t.triple.toUpperCase().replace(/-/g, '_')}_LINKER`] = wrapper;
      // A static musl build archives objects between compiling and linking, and
      // the host `ar` cannot read foreign objects - it fails with a bare
      // "cannot open cq", because `cq` is an ar *flag* it took for a filename.
      // 静态 musl 构建在编译和链接间归档目标文件；主机 ar 可能无法读取跨架构目标文件，
      // 并把 ar 选项 cq 当作文件名，报出简略的“cannot open cq”。
      env[`AR_${t.triple.replace(/-/g, '_')}`] = arWrapper;
      /**
       * `link-self-contained=no` because both sides ship a C runtime startup.
       *
       * The musl Rust targets bundle their own `crt1.o`/`rcrt1.o`, and Zig
       * bundles musl's. Linking both is a duplicate `_start_c` and the link
       * fails. Turning off rustc's copy leaves Zig's - which is the one that
       * matches the libc Zig is actually linking against.
       * 使用 link-self-contained=no：Rust 的 musl 目标和 Zig 都提供 C 运行时启动文件；
       * 同时链接会重复定义 _start_c。关闭 rustc 自带的 crt1.o/rcrt1.o，保留与 Zig 所链接 libc 匹配的版本。
       */
      env.RUSTFLAGS = `${env.RUSTFLAGS ?? ''} -C link-self-contained=no`.trim();
    }

    try {
      run('cargo', ['build', '--release', '--target', t.triple], { env });
    } catch (error) {
      console.error(`build failed for requested target ${t.key}`);
      process.exit(error.status || 1);
    }

    const src = join(TARGET_DIR, t.triple, 'release', `polaris-agent${t.ext}`);
    if (!existsSync(src)) {
      console.error(`cargo succeeded but requested artifact is missing: ${src}`);
      process.exit(1);
    }

    const dest = join(OUT_DIR, `polaris-agent-${PACKAGE_VERSION}-${t.key}${t.ext}`);
    copyFileSync(src, dest);
    copyFileSync(src, join(OUT_DIR, `polaris-agent-${t.key}${t.ext}`));
    const size = statSync(dest).size;
    built.push({ key: t.key, size, file: dest });
    console.log(`→ ${dest}  (${(size / 1048576).toFixed(1)} MB)`);
  }

  console.log('\n--- summary ---');
  for (const b of built) {
    console.log(`  built   ${b.key.padEnd(16)} ${(b.size / 1048576).toFixed(1)} MB`);
  }
  for (const s of skipped) {
    console.log(`  skipped ${s.key.padEnd(16)} ${s.why}`);
  }

  if (built.length === 0) {
    console.error('\nNo artifacts were built for the selected platforms.');
    process.exit(1);
  }

  /** 生成清单前确认输出目录存在当前版本的制品。 / Confirm current-version artifacts exist in the output directory before generating the manifest. */
  const present = TARGETS.filter((t) => existsSync(join(OUT_DIR, `polaris-agent-${PACKAGE_VERSION}-${t.key}${t.ext}`)));
  if (present.length === 0) {
    console.error(`\nNo current-version artifacts found in ${OUT_DIR}.`);
    process.exit(1);
  }

  const platforms = present.flatMap((target) => {
    const filename = `polaris-agent-${PACKAGE_VERSION}-${target.key}${target.ext}`;
    const file = join(OUT_DIR, filename);
    const bytes = readFileSync(file);
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    return [{ key: target.key, file: filename, size: bytes.length, sha256, version: PACKAGE_VERSION }];
  });
  writeFileSync(join(OUT_DIR, 'manifest.json'), JSON.stringify({ version: PACKAGE_VERSION, platforms }, null, 2) + '\n');

  console.log(`\n${built.length} artifact(s) in ${OUT_DIR}`);
}

/**
 * The archiver shim.
 *
 * A separate shim from the compiler because the argument shapes are nothing
 * alike: `ar` gets `cq <archive> <objects...>` and no `--target` at all, so
 * routing it through the compiler shim made clang read `cq` as an input file
 * and fail with "cannot open cq". Zig's `ar` is a full llvm-ar.
 * 归档器使用独立包装器，因为 ar 接收 cq <archive> <objects...> 且不使用 --target；
 * 若经过编译器包装器，clang 会把 cq 当作输入文件并报“cannot open cq”。Zig ar 是完整的 llvm-ar。
 */
function writeArWrapper(path, zig) {
  const js = `#!/usr/bin/env node
// Generated by scripts/build-agent.mjs - do not edit.
// 由 scripts/build-agent.mjs 自动生成，请勿修改。
const { spawnSync } = require('node:child_process');
const args = process.argv.slice(2);
const r = spawnSync(${JSON.stringify(zig)}, ['ar', ...args], { stdio: 'inherit' });
process.exit(r.status ?? 1);
`;
  const jsPath = `${path}.cjs`;
  writeFileSync(jsPath, js);
  const shim =
    process.platform === 'win32'
      ? `@echo off\r\nnode "${jsPath}" %*\r\n`
      : `#!/bin/sh\nexec node "${jsPath}" "$@"\n`;
  writeFileSync(path, shim, { mode: 0o755 });
}

/**
 * A shim so cc-rs finds a `<triple>-gcc` it can call.
 *
 * cc-rs passes the Rust triple in `--target=` (and `-m64`), while Zig names
 * the same platform `x86_64-linux-musl` - no `unknown-` vendor field, so Zig
 * rejects the Rust spelling with "UnknownOperatingSystem". The shim drops any
 * `--target` the caller supplied and substitutes Zig's spelling.
 *
 * Written in Node rather than shell on purpose. The first version was a .cmd
 * `for`-style loop and it spun forever on Windows: `findstr` plus `shift` plus
 * `goto` in a batch file is a maze of quoting rules, and a build tool that can
 * hang is worse than one that fails. Node is already the build's runtime, so
 * this costs nothing extra and the argument handling is ordinary code.
 * cc-rs 通过 --target= 和 -m64 传入 Rust 三元组；Zig 的 x86_64-linux-musl 没有 unknown- 厂商段，
 * 会因 Rust 名称报 UnknownOperatingSystem。包装器删除调用方的 --target 并替换为 Zig 名称。
 * 选择 Node 而非 shell：第一版 .cmd for 循环因 findstr、shift、goto 及批处理引号规则在 Windows 上
 * 无限循环；构建工具挂起比失败更糟。Node 已是构建运行时，因此无额外依赖且参数处理更直接。
 */
function writeWrapper(path, zig, zigTarget) {
  const js = `#!/usr/bin/env node
// Generated by scripts/build-agent.mjs - do not edit.
// 由 scripts/build-agent.mjs 自动生成，请勿修改。
const { spawnSync } = require('node:child_process');
const args = process.argv.slice(2).filter((a) => !a.startsWith('--target='));
const r = spawnSync(${JSON.stringify(zig)}, ['cc', '-target', ${JSON.stringify(zigTarget)}, ...args], {
  stdio: 'inherit',
});
process.exit(r.status ?? 1);
`;
  const jsPath = `${path}.cjs`;
  writeFileSync(jsPath, js);

  // The entry point cc-rs actually invokes. Both hosts run the Node script.
  // cc-rs 实际调用的入口；两种主机均运行该 Node 脚本。
  const shim =
    process.platform === 'win32'
      ? `@echo off\r\nnode "${jsPath}" %*\r\n`
      : `#!/bin/sh\nexec node "${jsPath}" "$@"\n`;
  writeFileSync(path, shim, { mode: 0o755 });
}

main();
