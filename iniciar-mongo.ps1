$ErrorActionPreference = 'Stop'
$utf8 = New-Object System.Text.UTF8Encoding $false
[Console]::OutputEncoding = $utf8
$OutputEncoding = $utf8
Set-Location $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw 'Ambiente Python ausente em .venv. Consulte o README.' }
$pingCode = @'
import sys
from pymongo import MongoClient
c = MongoClient('mongodb://127.0.0.1:27018', serverSelectionTimeoutMS=500, connectTimeoutMS=500)
try:
    c.admin.command('ping')
except Exception:
    sys.exit(1)
finally:
    c.close()
'@
function Test-MongoReady {
    & $python -c $pingCode
    return ($LASTEXITCODE -eq 0)
}
if (Test-MongoReady) {
    Write-Host 'MongoDB local já está disponível na porta 27018.'
    return
}
$bin = Get-ChildItem '.runtime/mongo' -Filter mongod.exe -Recurse | Select-Object -First 1
if (-not $bin) { throw 'MongoDB portátil ausente em .runtime/mongo.' }
New-Item -ItemType Directory -Force '.runtime/mongo-data' | Out-Null
$dbpath = (Resolve-Path '.runtime/mongo-data').Path
$logpath = Join-Path $PSScriptRoot '.runtime/mongo.log'
Write-Host 'Iniciando MongoDB local na porta 27018...'
$mongoProcess = Start-Process -WindowStyle Hidden -FilePath $bin.FullName -ArgumentList @('--dbpath', ('"' + $dbpath + '"'), '--bind_ip','127.0.0.1','--port','27018','--logpath',('"'+$logpath+'"'), '--logappend') -PassThru
$deadline = [DateTime]::UtcNow.AddSeconds(30)
while ([DateTime]::UtcNow -lt $deadline) {
    if (Test-MongoReady) {
        Write-Host 'MongoDB pronto.'
        return
    }
    $mongoProcess.Refresh()
    if ($mongoProcess.HasExited) { throw "MongoDB encerrou durante a inicialização. Consulte $logpath" }
    Start-Sleep -Milliseconds 250
}
throw "MongoDB não ficou pronto em 30 segundos. Consulte $logpath"
