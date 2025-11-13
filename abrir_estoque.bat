@echo off
chcp 65001 > nul
title Estoque BOSS BLANC
echo ===============================
echo    INICIANDO ESTOQUE BOSS BLANC
echo ===============================
echo.

cd /d "C:\Users\Rafael Cintra\Desktop\Estoque_bossblanc_teste"

if exist app_boss.py (
    echo Aplicacao encontrada. Iniciando...
    echo.
    echo Aguarde alguns segundos...
    echo O navegador abrira automaticamente.
    echo.
    echo Para fechar: Ctrl+C no terminal
    echo ===============================
    echo.
    streamlit run app_boss.py
) else (
    echo ERRO: Arquivo app_boss.py nao encontrado!
    echo Verifique o caminho: C:\Users\Rafael Cintra\Desktop\Estoque - Teste
    pause
)