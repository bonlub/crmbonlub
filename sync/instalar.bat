@echo off
chcp 65001 >nul
echo ============================================
echo  CRM Bonlub Sync - Instalação
echo ============================================

:: Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: Python nao encontrado.
    echo Baixe em: https://python.org/downloads
    pause
    exit /b 1
)

echo.
echo Instalando dependencias Python...
pip install -r requirements.txt

if errorlevel 1 (
    echo.
    echo ERRO na instalacao. Tente rodar como Administrador.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  INSTALACAO CONCLUIDA
echo ============================================
echo.
echo PROXIMOS PASSOS:
echo.
echo  1. Execute: configurar.bat
echo     Configure IP, porta e senha do banco
echo     Use a opcao [T] para testar a conexao
echo.
echo  2. Coloque o arquivo firebase-credentials.json
echo     nesta pasta (gere em Firebase Console ^>
echo     Configuracoes ^> Contas de servico)
echo.
echo  3. Execute: descobrir_tabelas.bat
echo     Para mapear as tabelas do Symplex
echo.
echo  4. Execute: rodar.bat
echo     Para iniciar a sincronizacao
echo ============================================
pause
