param([switch]$Relatorio, [string[]]$Argumentos = @())
$ErrorActionPreference = 'Stop'
$utf8 = New-Object System.Text.UTF8Encoding $false
[Console]::OutputEncoding = $utf8
$OutputEncoding = $utf8
Set-Location $PSScriptRoot
$jdk = Get-ChildItem '.runtime/java' -Directory | Select-Object -First 1
if ($jdk) { $env:JAVA_HOME = $jdk.FullName; $env:PATH = "$env:JAVA_HOME\bin;$env:PATH" }
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:HADOOP_HOME = Join-Path $PSScriptRoot '.runtime/hadoop'
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
$env:PYTHONIOENCODING = 'utf-8'
$customUri = @($Argumentos | Where-Object { $_ -eq '--uri' -or $_ -like '--uri=*' }).Count -gt 0
$helpRequested = @($Argumentos | Where-Object { $_ -eq '--help' -or $_ -eq '-h' }).Count -gt 0
if (-not $customUri -and -not $helpRequested -and (-not $env:MONGO_URI -or $env:MONGO_URI -eq 'mongodb://127.0.0.1:27018')) {
    & (Join-Path $PSScriptRoot 'iniciar-mongo.ps1')
}
if ($Relatorio) { & $env:PYSPARK_PYTHON pipeline.py --report @Argumentos }
else { & $env:PYSPARK_PYTHON pipeline.py @Argumentos }
exit $LASTEXITCODE
