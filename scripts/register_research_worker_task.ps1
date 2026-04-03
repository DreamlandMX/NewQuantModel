$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoDir = Split-Path -Parent $scriptDir
$taskName = "newquantmodel-research-worker"
$launchScript = Join-Path $repoDir "scripts\start_research_worker.ps1"

if (-not (Test-Path $launchScript)) {
  throw "Missing launcher script at $launchScript"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `"$launchScript`""
$triggerAtLogon = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -MultipleInstances IgnoreNew `
  -RestartCount 3 `
  -RestartInterval (New-TimeSpan -Minutes 5)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

Register-ScheduledTask `
  -TaskName $taskName `
  -Action $action `
  -Trigger $triggerAtLogon `
  -Settings $settings `
  -Principal $principal `
  -Force | Out-Null

Write-Host "Registered scheduled task: $taskName"
