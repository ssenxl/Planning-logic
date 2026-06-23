<#
  deploy.ps1 — deploy Knit Plan web ขึ้น server docker-webchat
  - แพ็คซอร์ส (ตัด .venv/build/dist/model ฯลฯ) เป็น tarball
  - scp ขึ้น server + รัน server-bootstrap.sh (build + up, project แยก "knitplan")
  ใช้: pwsh webapp\deploy.ps1
#>
param(
  [string]$SshHost = "docker-webchat",
  [string]$User = "scm",
  [string]$Key = "$env:USERPROFILE\.ssh\docker-webchat"
)
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot   # webapp\.. = repo root
$target = "$User@$SshHost"

$tgz = Join-Path $env:TEMP "knitplan_src.tgz"
Write-Host "==> แพ็คซอร์สจาก $repo -> $tgz" -ForegroundColor Cyan
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
Write-Host "    ขนาด tarball: $sizeMB MB"

Write-Host "==> ส่งขึ้น server ($target)" -ForegroundColor Cyan
& scp.exe -i $Key $tgz "${target}:/home/scm/knitplan_src.tgz"
if ($LASTEXITCODE -ne 0) { throw "scp tarball ล้มเหลว" }
& scp.exe -i $Key (Join-Path $PSScriptRoot "server-bootstrap.sh") "${target}:/home/scm/knitplan_bootstrap.sh"
if ($LASTEXITCODE -ne 0) { throw "scp bootstrap ล้มเหลว" }

Write-Host "==> รัน bootstrap บน server (build + up)" -ForegroundColor Cyan
& ssh.exe -i $Key $target "sed -i 's/\r`$//' /home/scm/knitplan_bootstrap.sh && bash /home/scm/knitplan_bootstrap.sh"
if ($LASTEXITCODE -ne 0) { throw "bootstrap ล้มเหลว" }

Write-Host "==> เสร็จสิ้น — เปิด http://docker-webchat:8080" -ForegroundColor Green
