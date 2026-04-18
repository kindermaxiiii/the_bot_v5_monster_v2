param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("start", "stop", "status")]
    [string]$Action
)

$RepoRoot = "D:\bot\the_bot_v5_monster_v2"
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$PidDir = Join-Path $RepoRoot "exports\vnext\runtime"
$PidFile = Join-Path $PidDir "bot.pid"
$ExportPath = Join-Path $RepoRoot "exports\vnext\live_bot.jsonl"
$ReportPath = Join-Path $RepoRoot "exports\vnext\live_bot_report.json"

New-Item -ItemType Directory -Force -Path $PidDir | Out-Null

function Get-BotProcess {
    if (-not (Test-Path $PidFile)) {
        return $null
    }

    try {
        $storedPid = [int](Get-Content $PidFile -ErrorAction Stop)
        return Get-Process -Id $storedPid -ErrorAction Stop
    }
    catch {
        Remove-Item $PidFile -ErrorAction SilentlyContinue
        return $null
    }
}

switch ($Action) {
    "start" {
        $existing = Get-BotProcess
        if ($null -ne $existing) {
            Write-Host "bot_deja_lance pid=$($existing.Id)"
            exit 0
        }

        $command = @"
Set-Location '$RepoRoot'
while (`$true) {
    & '$Python' 'scripts/run_vnext_shadow.py' --source live --notifier discord --persist-state --cycles 1 --max-active-matches 3 --cooldown-seconds 180 --export-path '$ExportPath' --report '$ReportPath'
    Start-Sleep -Seconds 30
}
"@

        $proc = Start-Process powershell.exe `
            -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command `
            -WindowStyle Hidden `
            -PassThru

        $proc.Id | Set-Content $PidFile -Encoding utf8
        Write-Host "bot_lance pid=$($proc.Id)"
        exit 0
    }

    "stop" {
        $existing = Get-BotProcess
        if ($null -eq $existing) {
            Write-Host "bot_deja_arrete"
            exit 0
        }

        Stop-Process -Id $existing.Id -Force
        Remove-Item $PidFile -ErrorAction SilentlyContinue
        Write-Host "bot_arrete pid=$($existing.Id)"
        exit 0
    }

    "status" {
        $existing = Get-BotProcess
        if ($null -eq $existing) {
            Write-Host "bot_status=stopped"
        }
        else {
            Write-Host "bot_status=running pid=$($existing.Id)"
        }
        exit 0
    }
}