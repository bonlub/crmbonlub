@echo off
echo ============================================
echo  CRM Bonlub Sync - Instalação
echo ============================================

:: Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: Python nao encontrado. Instale em https://python.org
    pause
    exit /b 1
)

:: Verificar ODBC Driver 17
reg query "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Driver 17 for SQL Server" >nul 2>&1
if errorlevel 1 (
    echo AVISO: ODBC Driver 17 for SQL Server nao encontrado.
    echo Baixe em: https://go.microsoft.com/fwlink/?linkid=2282778
    echo Instalando assim mesmo...
)

:: Instalar dependências
echo.
echo Instalando dependencias Python...
pip install -r requirements.txt

echo.
echo ============================================
echo  PRÓXIMOS PASSOS:
echo ============================================
echo 1. Edite o arquivo config.json com suas credenciais SQL
echo 2. Coloque o arquivo firebase-credentials.json nesta pasta
echo    (Gere em: Firebase Console > Configurações > Contas de serviço)
echo 3. Execute: rodar.bat
echo ============================================
pause
