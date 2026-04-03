$ErrorActionPreference = "Stop"

$taskName = "newquantmodel-research-worker"
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue

if (-not $task) {
  Write-Host "Scheduled task not found: $taskName"
  exit 1
}

$info = Get-ScheduledTaskInfo -TaskName $taskName

[pscustomobject]@{
  TaskName     = $task.TaskName
  State        = $task.State
  LastRunTime  = $info.LastRunTime
  LastTaskResult = $info.LastTaskResult
  NextRunTime  = $info.NextRunTime
} | Format-List
