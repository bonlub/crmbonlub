@echo off
chcp 65001 >nul
echo Executando descoberta de tabelas do Symplex...
echo Resultado sera salvo em: tabelas_symplex.txt
echo.
python descobrir_tabelas.py > tabelas_symplex.txt 2>&1
python descobrir_tabelas.py
echo.
echo O resultado tambem foi salvo em tabelas_symplex.txt
pause
