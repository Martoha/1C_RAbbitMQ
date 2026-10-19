# Регистрация COM-прослойки для 64-битной 1С.
# Запускать в Windows от имени администратора из папки, где лежит OneCRabbit.dll.
$dll = Join-Path $PSScriptRoot "OneCRabbit.dll"
if (-not (Test-Path $dll)) {
    Write-Error "Не найден $dll. Запустите скрипт из папки dist."
}

$regasm = Join-Path $env:WINDIR "Microsoft.NET\Framework64\v4.0.30319\regasm.exe"
& $regasm $dll /codebase
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Зарегистрировано: OneCRabbit.Client"
