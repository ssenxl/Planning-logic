<#
  deploy.ps1 - deploy Knit Plan web ขึ้น server docker-webchat
  - pack source เป็น tarball
  - scp ขึ้น server + run server-bootstrap.sh (build + up, project = knitplan)
  ใช้: powershell.exe -ExecutionPolicy Bypass -File webapp\deploy.ps1
#>
param(
  [string]$SshHost = "docker-webchat",
  [string]$User = "scm",
  [string]$Key = "$env:USERPROFILE\.ssh\docker-webchat"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$target = "$User@$SshHost"
$tgz = Join-Path $env:TEMP "knitplan_src.tgz"

Write-Host "==> Pack source: $repo -> $tgz" -ForegroundColor Cyan
$excludes = @(
  "--exclude=./.venv", "--exclude=./.venv-*", "--exclude=./.git", "--exclude=./build", "--exclude=./dist",
  "--exclude=./model", "--exclude=./data_plan", "--exclude=./Booking", "--exclude=./Stock",
  "--exclude=./Order", "--exclude=./KnitPlan_Release", "--exclude=./.claude",
  "--exclude=./webapp/frontend/node_modules", "--exclude=./webapp/frontend/dist",
  "--exclude=./webapp/logs", "--exclude=*.zip", "--exclude=*.pyc", "--exclude=__pycache__",
  "--exclude=*.log", "--exclude=./plan_log.txt"
)

tar.exe -czf $tgz @excludes -C $repo .
$sizeMB = [math]::Round((Get-Item $tgz).Length / 1MB, 1)
Write-Host "    Tarball size: $sizeMB MB"

Write-Host "==> Upload to server ($target)" -ForegroundColor Cyan
& scp.exe -i $Key $tgz "${target}:/home/scm/knitplan_src.tgz"
if ($LASTEXITCODE -ne 0) { throw "scp tarball failed" }

$bootstrap = Join-Path $PSScriptRoot "server-bootstrap.sh"
& scp.exe -i $Key $bootstrap "${target}:/home/scm/knitplan_bootstrap.sh"
if ($LASTEXITCODE -ne 0) { throw "scp bootstrap failed" }

Write-Host "==> Run bootstrap on server" -ForegroundColor Cyan
$remoteCmd = 'sed -i ''s/\r$//'' /home/scm/knitplan_bootstrap.sh; bash /home/scm/knitplan_bootstrap.sh'

& ssh.exe -i $Key $target $remoteCmd
if ($LASTEXITCODE -ne 0) { throw "bootstrap failed" }

Write-Host "==> Done: http://docker-webchat:8080" -ForegroundColor Green

