$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoDir = Split-Path -Parent $scriptDir
$wslRepoDir = (wsl.exe wslpath -a $repoDir).Trim()

$launchCommand = "cd '$wslRepoDir' && nohup bash scripts/run_research_worker.sh >/dev/null 2>&1 &"
wsl.exe bash -lc $launchCommand
