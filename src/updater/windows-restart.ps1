param(
    [Parameter(Mandatory=$true)][int]$AgentProcessId,
    [Parameter(Mandatory=$true)][string]$Executable,
    [Parameter(Mandatory=$true)][string]$TaskName,
    [Parameter(Mandatory=$true)][string]$ReadyFile
)
$ErrorActionPreference = 'Stop'
try {
    $binary = [IO.Path]::GetFullPath($Executable)
    if ([IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($ReadyFile)) -ne [IO.Path]::GetDirectoryName($binary)) { throw 'Invalid restart path' }
    $task = Get-ScheduledTask -TaskPath '\' -TaskName $TaskName -ErrorAction Stop
    $parent = Get-Process -Id $AgentProcessId -ErrorAction Stop
    if ([IO.Path]::GetFullPath($parent.Path) -ne $binary) { throw 'Restart parent does not match the installed Agent' }
    [IO.File]::WriteAllText($ReadyFile, 'ready')
    $parent.WaitForExit()
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ((Get-ScheduledTask -TaskPath '\' -TaskName $TaskName).State -eq 'Running') {
        if ([DateTime]::UtcNow -ge $deadline) { throw 'Scheduled task did not release the Agent' }
        Start-Sleep -Milliseconds 250
    }
    $started = $false
    for ($attempt = 0; $attempt -lt 5 -and -not $started; $attempt++) {
        try { Start-ScheduledTask -TaskPath '\' -TaskName $TaskName -ErrorAction Stop; $started = $true }
        catch { if ($attempt -eq 4) { throw }; Start-Sleep -Seconds 1 }
    }
} catch {
    Write-Error $_ -ErrorAction Continue
    exit 1
} finally {
    if ([IO.File]::Exists($ReadyFile)) { [IO.File]::Delete($ReadyFile) }
    if ($PSCommandPath) { [IO.File]::Delete($PSCommandPath) }
}
