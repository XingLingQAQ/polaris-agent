// 验证 Windows 更新替换、回退和篡改拒绝。
// Check Windows update replacement, rollback, and tamper rejection.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import { tmpdir } from 'node:os';

if (process.platform !== 'win32') {
  console.log('SKIP Windows update handoff requires Windows PowerShell');
} else {
  const base = realpathSync(tmpdir());
  const directory = mkdtempSync(join(base, 'polaris-windows-update-'));
  const helper = readFileSync('src/updater/windows-update.ps1', 'utf8');
  try {
    const scenarios = ['success', 'restart-failure', 'tampered', 'existing-replacement'];
    if (process.env.AGENT_WINDOWS_TEST_BINARY) scenarios.push('native');
    for (const scenario of scenarios) {
      const succeeds = scenario === 'success' || scenario === 'native';
      const executable = join(directory, `${scenario}.exe`);
      const staged = join(directory, scenario === 'native' ? '.polaris-agent.exe.update-123.exe' : `${scenario}.cmd`);
      const script = join(directory, `${scenario}.ps1`);
      const ready = join(directory, `${scenario}.ready`);
      const log = join(directory, `${scenario}.calls`);
      const candidate = scenario === 'native' ? readFileSync(process.env.AGENT_WINDOWS_TEST_BINARY) : Buffer.from('@echo off\r\necho polaris-agent 9.9.9\r\n');
      const expectedVersion = scenario === 'native' ? readFileSync('Cargo.toml', 'utf8').match(/^version = "([^"]+)"/m)[1] : '9.9.9';
      writeFileSync(executable, 'old-binary');
      writeFileSync(staged, candidate);
      writeFileSync(script, helper);
      if (scenario === 'existing-replacement') writeFileSync(staged + '.replace', 'unrelated-existing-file');
      const body = `
function Get-Process { param([int]$Id)
 $process = [pscustomobject]@{ Path=$env:POLARIS_FIXTURE_EXE }
 $process | Add-Member -MemberType ScriptMethod -Name WaitForExit -Value { param($timeout) return $true }
 return $process
}
function Get-ScheduledTask { [CmdletBinding()]param($TaskPath,$TaskName) return [pscustomobject]@{State='Ready'} }
function Start-ScheduledTask { [CmdletBinding()]param($TaskPath,$TaskName)
 Add-Content -LiteralPath $env:POLARIS_FIXTURE_CALLS -Value 'start'
 if ($env:POLARIS_FIXTURE_MODE -eq 'restart-failure' -and (Get-Content -LiteralPath $env:POLARIS_FIXTURE_CALLS).Count -eq 1) { throw 'fixture restart failed' }
}
& $env:POLARIS_FIXTURE_SCRIPT -AgentProcessId 1234 -Executable $env:POLARIS_FIXTURE_EXE -Staged $env:POLARIS_FIXTURE_STAGE -TaskName 'fixture' -ReadyFile $env:POLARIS_FIXTURE_READY -ExpectedHash $env:POLARIS_FIXTURE_HASH -ExpectedVersion $env:POLARIS_FIXTURE_VERSION
`;
      const command = spawnSync('powershell.exe', ['-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', Buffer.from(body, 'utf16le').toString('base64')], {
        encoding: 'utf8', timeout: 20000, windowsHide: true,
        env: { ...process.env, POLARIS_FIXTURE_EXE: executable, POLARIS_FIXTURE_STAGE: staged, POLARIS_FIXTURE_SCRIPT: script,
          POLARIS_FIXTURE_READY: ready, POLARIS_FIXTURE_CALLS: log, POLARIS_FIXTURE_MODE: scenario,
          POLARIS_FIXTURE_VERSION: expectedVersion,
          POLARIS_FIXTURE_HASH: scenario === 'tampered' ? '0'.repeat(64) : createHash('sha256').update(candidate).digest('hex') },
      });
      assert.ifError(command.error);
      assert.equal(command.status, succeeds ? 0 : 1, command.stderr);
      assert.equal(existsSync(ready), false);
      if (succeeds) {
        assert.deepEqual(readFileSync(executable), candidate);
        assert.equal(readFileSync(join(directory, `.${scenario}.exe.previous`), 'utf8'), 'old-binary');
        assert.equal(existsSync(staged), false);
      } else {
        assert.equal(readFileSync(executable, 'utf8'), 'old-binary', 'failed updates retain or restore the running version');
      }
      if (scenario === 'restart-failure') assert.equal(readFileSync(log, 'utf8').trim().split(/\r?\n/).length, 2, 'rollback restarts the previous version');
      if (scenario === 'tampered') assert.equal(existsSync(log), false, 'tampered files never stop or start a task');
      if (scenario === 'existing-replacement') assert.equal(readFileSync(staged + '.replace', 'utf8'), 'unrelated-existing-file');
    }
    console.log('PASS Windows update helper: verified replacement, restart rollback, tamper rejection and unrelated-file preservation');
  } finally {
    const resolved = realpathSync(directory);
    const child = relative(base, resolved);
    assert.ok(child.startsWith('polaris-windows-update-') && !child.includes(sep));
    rmSync(resolved, { recursive: true, force: true });
  }
}
