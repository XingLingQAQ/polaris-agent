// 验证 Windows 重启助手的父进程检查与计划任务重试。
// Check parent-process validation and scheduled-task retries in the Windows restart helper.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import { tmpdir } from 'node:os';

if (process.platform !== 'win32') {
  console.log('SKIP Windows restart helper requires Windows PowerShell');
} else {
  const base = realpathSync(tmpdir());
  const directory = mkdtempSync(join(base, 'polaris-windows-restart-'));
  const helper = readFileSync('src/updater/windows-restart.ps1', 'utf8');
  try {
    for (const scenario of ['success', 'retry', 'wrong-parent']) {
      const executable = join(directory, `${scenario}.exe`);
      const script = join(directory, `${scenario}.ps1`);
      const ready = join(directory, `${scenario}.ready`);
      const log = join(directory, `${scenario}.calls`);
      writeFileSync(executable, 'fixture');
      writeFileSync(script, helper);
      const body = `
function Get-Process { param([int]$Id)
 $path = if ($env:MODE -eq 'wrong-parent') { $env:WRONG } else { $env:EXE }
 $process = [pscustomobject]@{ Path=$path }
 $process | Add-Member ScriptMethod WaitForExit { param($timeout) Add-Content -LiteralPath $env:CALLS -Value 'exit'; return $true }
 return $process
}
function Get-ScheduledTask { [CmdletBinding()]param($TaskPath,$TaskName) return [pscustomobject]@{State='Ready'} }
function Start-ScheduledTask { [CmdletBinding()]param($TaskPath,$TaskName)
 Add-Content -LiteralPath $env:CALLS -Value 'start'
 if ($env:MODE -eq 'retry' -and @(Get-Content -LiteralPath $env:CALLS | Where-Object { $_ -eq 'start' }).Count -eq 1) { throw 'fixture start failure' }
}
function Start-Sleep { param($Seconds, $Milliseconds) }
& $env:SCRIPT -AgentProcessId 1234 -Executable $env:EXE -TaskName 'fixture' -ReadyFile $env:READY
`;
      const result = spawnSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-EncodedCommand', Buffer.from(body, 'utf16le').toString('base64')], {
        encoding: 'utf8', timeout: 20000, windowsHide: true,
        env: { ...process.env, MODE: scenario, EXE: executable, WRONG: join(directory, 'wrong.exe'), SCRIPT: script, READY: ready, CALLS: log },
      });
      assert.ifError(result.error);
      assert.equal(result.status, scenario === 'wrong-parent' ? 1 : 0, result.stderr);
      assert.equal(existsSync(ready), false);
      if (scenario === 'wrong-parent') assert.equal(existsSync(log), false);
      else assert.deepEqual(readFileSync(log, 'utf8').trim().split(/\r?\n/), scenario === 'retry' ? ['exit', 'start', 'start'] : ['exit', 'start']);
    }
    console.log('PASS Windows restart helper: parent validation, exit handoff and task start retry');
  } finally {
    const child = relative(base, realpathSync(directory));
    assert.ok(child.startsWith('polaris-windows-restart-') && !child.includes(sep));
    rmSync(directory, { recursive: true, force: true });
  }
}
