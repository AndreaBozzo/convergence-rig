# Run the implementing agent (Antigravity CLI, Gemini 4 Neon) headless, one
# milestone per session. Each session starts fresh and resumes from the
# committed reports, so a quota stop or crash loses at most one milestone.
#
#   pwsh scripts/run-agent.ps1                     # M0..M3, stop at the first problem
#   pwsh scripts/run-agent.ps1 -Milestones M1      # one milestone
#   pwsh scripts/run-agent.ps1 -Model gemini-4-neon-xhigh
#
# Logs: agent-runs/<timestamp>-<milestone>.json (agy's JSON envelope) and .err.
#
# Permissions: agy runs in its default headless mode (auto), where Google's
# safety monitor approves each command. agy 1.5.12 loads the deny list in
# .agy/settings.json but did not enforce it in any mode tested on 2026-11-19
# (auto, accept-edits, sandbox); the real limits are AGENTS.md and the absence
# of a git remote. Its sandbox blocks Docker, so it cannot run this lab.
param(
    [string[]]$Milestones = @('M0', 'M1', 'M2', 'M3'),
    [string]$Model = 'gemini-4-neon-high',
    [string]$Timeout = '5h'
)
$ErrorActionPreference = 'Stop'
# Under `pwsh -File`, "-Milestones L1,L2,L3" arrives as one string; split it.
$Milestones = @($Milestones | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$env:VIRTUAL_ENV = $null   # a caller's activated venv would leak into uv
$agy =Join-Path $env:LOCALAPPDATA 'agy\bin\agy.exe'
New-Item -ItemType Directory -Force (Join-Path $repo 'agent-runs') | Out-Null

docker version --format '{{.Server.Version}}' 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host 'Starting Docker Desktop...'
    Start-Process "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 5
        docker version --format '{{.Server.Version}}' 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { break }
    }
    if ($LASTEXITCODE -ne 0) { throw 'Docker engine did not start within 5 minutes.' }
}

foreach ($m in $Milestones) {
    if (Test-Path 'reports/BLOCKED.md') {
        Write-Host "reports/BLOCKED.md exists; resolve it (and delete it) before continuing."
        exit 2
    }
    $report = "reports/$m.md"
    if ((Test-Path $report) -and (Select-String -Path $report -Pattern '(?i)verdict\W+pass' -Quiet)) {
        Write-Host "$m already passed; skipping."
        continue
    }

    $prompt = @"
You are implementing milestone $m of the CDC convergence lab in this repository.
Read AGENTS.md and docs/PLAN.md first, then every file in reports/ and the git log.
Do milestone $m only, end to end, following its steps and its Pass criteria in docs/PLAN.md.
Finish with reports/$m.md and one local commit, as AGENTS.md describes.
If you cannot finish, write reports/BLOCKED.md as AGENTS.md describes and stop.
Your final answer: one paragraph with the verdict, the commit hash and anything the reviewer must look at.
"@

    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
    $out = "agent-runs/$stamp-$m.json"
    $err = "agent-runs/$stamp-$m.err"
    Write-Host "[$stamp] $m with $Model (timeout $Timeout) -> $out"
    & $agy -p $prompt --model $Model --output-format json --print-timeout $Timeout `
        --settings (Join-Path $repo '.agy/settings.json') > $out 2> $err
    $code = $LASTEXITCODE
    Write-Host "${m}: agy exited $code"

    if ($code -ne 0) { Write-Host "Stopping: non-zero exit. See $out and $err."; exit $code }
    if (Test-Path 'reports/BLOCKED.md') { Write-Host 'Stopping: agent wrote reports/BLOCKED.md.'; exit 2 }
    if (-not (Test-Path $report)) { Write-Host "Stopping: $report was not written."; exit 3 }
    if (-not (Select-String -Path $report -Pattern '(?i)verdict\W+pass' -Quiet)) {
        Write-Host "Stopping: $report does not record a pass verdict; review before the next milestone."
        exit 4
    }
}
Write-Host 'All requested milestones reported a pass.'
