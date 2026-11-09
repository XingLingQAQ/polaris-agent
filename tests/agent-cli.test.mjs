// 执行实际 Agent 命令，检查配置、服务预览和命令注册。
// Run the Agent CLI to check configuration, service previews, and command registration.

import assert from 'node:assert/strict';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const source = resolve(process.env.AGENT_NATIVE_BINARY ?? join(process.env.CARGO_TARGET_DIR ?? 'target', 'debug', process.platform === 'win32' ? 'polaris-agent.exe' : 'polaris-agent'));
assert.ok(existsSync(source), 'Build the native Agent first or set AGENT_NATIVE_BINARY.');
const prefix = join(tmpdir(), 'polaris CLI 空间 ');
const temp = mkdtempSync(prefix);
const executable = join(temp, process.platform === 'win32' ? 'renamed.exe' : 'renamed');
const config = join(temp, 'config.json');
const log = join(temp, 'agent.log');
const metadata = join(temp, 'install.json');
const directory = join(temp, 'commands');
const shortCommand = join(directory, process.platform === 'win32' ? 'pa.cmd' : 'pa');
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('POLARIS_AGENT_')));
const invoke = (args, expected = 0) => {
  const result = spawnSync(executable, args, { env, encoding: 'utf8', windowsHide: true, timeout: 10000 });
  if (expected === 0) assert.equal(result.status, 0, result.error?.message ?? result.stderr);
  else assert.notEqual(result.status, 0);
  return result;
};
const record = (manager, scope = 'user') => invoke(['record-installation', '--config', config, '--manager', manager, '--service-name', 'polaris-fixture', '--scope', scope, '--mode', 'manual', '--log-path', log, '--extra-path', join(temp, 'service.conf'), '--user-id', '501']);
const throughWrapper = (path, args) => {
  const result = process.platform === 'win32'
    ? spawnSync('cmd.exe', ['/d', '/s', '/c', `""${path}" ${args.join(' ')}"`], { env, encoding: 'utf8', windowsHide: true, windowsVerbatimArguments: true, cwd: tmpdir() })
    : spawnSync(path, args, { env, encoding: 'utf8', cwd: tmpdir() });
  assert.equal(result.status, 0, result.error?.message ?? result.stdout + result.stderr);
  return result;
};
try {
  copyFileSync(source, executable);
  writeFileSync(config, '{}');
  writeFileSync(log, 'one\ntwo\nthree\n');
  assert.match(invoke(['--version']).stdout, /^polaris-agent \d+\.\d+\.\d+/);
  assert.match(invoke(['--help']).stdout, /run[\s\S]*service[\s\S]*command[\s\S]*update[\s\S]*rollback/);
  invoke([], 1);
  invoke(['--update'], 1);
  assert.match(invoke(['run', '--dry-run'], 1).stderr, /--dry-run is supported only/);
  assert.match(invoke(['update', '--dry-run'], 1).stderr, /--dry-run is supported only/);
  invoke(['run', '--interval', '30'], 1);
  invoke(['run', '--enrollment-token', 'retired'], 1);
  writeFileSync(config, JSON.stringify({ endpoint: 'http://127.0.0.1:1', token: 'fixture', interval: 30 }));
  assert.match(invoke(['run', '--config', config], 1).stderr, /unknown field.*interval/);
  writeFileSync(config, JSON.stringify({ endpoint: 'http://127.0.0.1:1', token: 'fixture', enrollment_token: 'retired' }));
  assert.match(invoke(['run', '--config', config], 1).stderr, /unknown field.*enrollment_token/);
  writeFileSync(config, '{}');

  record('systemd');
  for (const action of ['start', 'stop', 'restart', 'status']) {
    assert.equal(invoke([action, '--config', config, '--dry-run']).stdout, invoke(['service', action, '--config', config, '--dry-run']).stdout);
  }
  assert.match(invoke(['service', 'restart', '--config', config, '--dry-run']).stdout, /systemctl --user restart polaris-fixture/);
  assert.match(invoke(['service', 'logs', '--config', config, '--dry-run', '--follow', '--lines', '12']).stdout, /journalctl --user -u polaris-fixture -n 12 --no-pager --follow/);
  record('windows-task', 'system');
  assert.match(invoke(['service', 'restart', '--config', config, '--dry-run']).stdout, /\/End[\s\S]*\/Run/);
  record('launchd');
  assert.match(invoke(['service', 'restart', '--config', config, '--dry-run']).stdout, /kickstart -k gui\/501\/polaris-fixture/);
  record('docker');
  assert.match(invoke(['service', 'status', '--config', config, '--dry-run']).stdout, /docker compose -f .*service.conf ps/);
  record('manual');
  assert.match(invoke(['service', 'logs', '--config', config, '--lines', '2']).stdout, /two\r?\nthree\r?\n/);
  assert.match(invoke(['service', 'start', '--config', config], 1).stderr, /no supported service manager/);

  const registerArgs = ['command', 'register', '--config', config, '--directory', directory, '--no-path'];
  invoke([...registerArgs, '--dry-run']);
  assert.equal(existsSync(directory), false);
  mkdirSync(directory);
  writeFileSync(shortCommand, 'owned by another program');
  assert.match(invoke(registerArgs, 1).stderr, /another installation/);
  assert.equal(existsSync(join(directory, process.platform === 'win32' ? 'polaris-agent.cmd' : 'polaris-agent')), false);
  assert.equal(JSON.parse(readFileSync(metadata, 'utf8')).registration, null);
  unlinkSync(shortCommand);
  invoke(registerArgs);
  const registration = JSON.parse(readFileSync(metadata, 'utf8')).registration;
  assert.equal(registration.path_added, false);
  assert.deepEqual(registration.profiles, []);
  const wrapper = registration.path;
  const wrapperSource = readFileSync(wrapper, 'utf8');
  const shortSource = readFileSync(shortCommand, 'utf8');
  assert.match(invoke(['command', 'status', '--config', config]).stdout, /short command: pa/);
  assert.match(throughWrapper(shortCommand, ['--version']).stdout, /^polaris-agent /);
  assert.match(throughWrapper(shortCommand, ['logs', '-n', '2']).stdout, /two\r?\nthree/);
  record('systemd');
  assert.match(throughWrapper(shortCommand, ['status', '--dry-run']).stdout, /systemctl --user status polaris-fixture/);
  assert.equal(wrapperSource.includes('fixture'), false, 'wrapper must contain no credentials');
  if (process.platform === 'win32') {
    assert.equal(readFileSync(join(directory, 'polaris-agent.ps1'))[0], 0xef);
    const version = spawnSync('cmd.exe', ['/d', '/s', '/c', `""${wrapper}" --version"`], { env, encoding: 'utf8', windowsHide: true, windowsVerbatimArguments: true });
    assert.equal(version.status, 0, version.stderr);
    assert.match(version.stdout, /^polaris-agent /);
  } else {
    assert.match(spawnSync(wrapper, ['--version'], { env, encoding: 'utf8' }).stdout, /^polaris-agent /);
  }
  invoke(registerArgs);
  writeFileSync(wrapper, wrapperSource + '\nmodified by another owner\n');
  assert.match(invoke(['command', 'unregister', '--config', config], 1).stderr, /modified/);
  assert.equal(existsSync(wrapper), true);
  assert.equal(existsSync(shortCommand), true);
  writeFileSync(wrapper, wrapperSource);
  writeFileSync(shortCommand, shortSource + '\nmodified\n');
  assert.match(invoke(['command', 'unregister', '--config', config], 1).stderr, /modified/);
  assert.equal(existsSync(wrapper), true);
  writeFileSync(shortCommand, shortSource);
  throughWrapper(shortCommand, ['command', 'unregister']);
  assert.equal(existsSync(wrapper), false);
  assert.equal(existsSync(shortCommand), false);
  if (process.platform === 'win32') assert.equal(existsSync(join(directory, 'pa.ps1')), false);
  assert.equal(JSON.parse(readFileSync(metadata, 'utf8')).registration, null);
  assert.equal(existsSync(config), true);
  assert.equal(existsSync(executable), true);
} finally {
  assert.ok(resolve(temp).startsWith(resolve(prefix)));
  rmSync(temp, { recursive: true, force: true });
}
console.log('PASS native CLI: current flags, strict config, service plans, log tails, Unicode global wrappers, ownership and unregister');
