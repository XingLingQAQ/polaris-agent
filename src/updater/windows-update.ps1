param(
    [Parameter(Mandatory=$true)][int]$AgentProcessId,
    [Parameter(Mandatory=$true)][string]$Executable,
    [Parameter(Mandatory=$true)][string]$Staged,
    [Parameter(Mandatory=$true)][string]$TaskName,
    [Parameter(Mandatory=$true)][string]$ReadyFile,
    [Parameter(Mandatory=$true)][string]$ExpectedHash,
    [Parameter(Mandatory=$true)][string]$ExpectedVersion
)
$ErrorActionPreference = 'Stop'
$installed = $false
$replacement = $null
$createdReplacement = $false
$previous = $null
$parentExited = $false
$failed = $null
$rolledBack = $false
try {
    $binary = [IO.Path]::GetFullPath($Executable)
    $candidate = [IO.Path]::GetFullPath($Staged)
    $directory = [IO.Path]::GetDirectoryName($binary)
    if ([IO.Path]::GetDirectoryName($candidate) -ne $directory -or
        [IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($ReadyFile)) -ne $directory) {
        throw 'Update paths must stay in the installed binary directory'
    }
    foreach ($path in @($binary, $candidate, $directory)) {
        if ((Get-Item -LiteralPath $path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Update paths cannot be reparse points'
        }
    }
    if ((Get-FileHash -LiteralPath $candidate -Algorithm SHA256).Hash -ne $ExpectedHash) {
        throw 'Staged Agent checksum changed'
    }
    $version = & $candidate --version
    if ($LASTEXITCODE -ne 0 -or $version.Trim() -ne "polaris-agent $ExpectedVersion") {
        throw 'Staged Agent version check failed'
    }
    $task = Get-ScheduledTask -TaskPath '\' -TaskName $TaskName -ErrorAction Stop
    $parent = Get-Process -Id $AgentProcessId -ErrorAction Stop
    if ([IO.Path]::GetFullPath($parent.Path) -ne $binary) { throw 'Update parent does not match the installed Agent' }
    [IO.File]::WriteAllText($ReadyFile, 'ready')
    if (-not $parent.WaitForExit(90000)) { throw 'Agent did not exit for its update' }
    $parentExited = $true
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ((Get-ScheduledTask -TaskPath '\' -TaskName $TaskName).State -eq 'Running') {
        if ([DateTime]::UtcNow -ge $deadline) { throw 'Scheduled task did not release the Agent' }
        Start-Sleep -Milliseconds 250
    }
    $previous = Join-Path $directory ('.' + [IO.Path]::GetFileName($binary) + '.previous')
    $replacement = $candidate + '.replace'
    $failed = $candidate + '.failed'
    if ([IO.File]::Exists($failed)) { throw 'Update rollback destination already exists' }
    if ([IO.File]::Exists($previous)) {
        if ((Get-Item -LiteralPath $previous -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Previous Agent path cannot be a reparse point'
        }
    }
    if ([IO.File]::Exists($replacement)) { throw 'Update replacement already exists' }
    [IO.File]::Copy($candidate, $replacement, $false)
    $createdReplacement = $true
    [IO.File]::Replace($replacement, $binary, $previous, $true)
    $installed = $true
    Start-ScheduledTask -TaskPath '\' -TaskName $TaskName -ErrorAction Stop
    [IO.File]::Delete($candidate)
} catch {
    $failure = $_
    try {
        if ($installed -and [IO.File]::Exists($previous)) {
            [IO.File]::Replace($previous, $binary, $failed, $true)
            $rolledBack = $true
        }
    } catch {
        Write-Error $_ -ErrorAction Continue
    } finally {
        if ($parentExited) {
            Start-ScheduledTask -TaskPath '\' -TaskName $TaskName -ErrorAction Continue
        }
    }
    Write-Error $failure -ErrorAction Continue
    exit 1
} finally {
    if ($createdReplacement -and [IO.File]::Exists($replacement)) { [IO.File]::Delete($replacement) }
    if ($rolledBack -and [IO.File]::Exists($failed)) { [IO.File]::Delete($failed) }
    if ([IO.File]::Exists($ReadyFile)) { [IO.File]::Delete($ReadyFile) }
    if ($PSCommandPath) { [IO.File]::Delete($PSCommandPath) }
}
