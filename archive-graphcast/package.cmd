:: Copyright (c) 2025 INFISYS INC

@echo off
if exist ".\decorator\__pycache__" rmdir .\decorator\__pycache__ /S /Q
if exist ".\model\__pycache__" rmdir .\model\__pycache__ /S /Q
if exist ".\sources\__pycache__" rmdir .\sources\__pycache__ /S /Q
if exist ".\graphcast.zip" del .\graphcast.zip /Q
install\7za a graphcast.zip decorator model sources graphcast.cmd run_graphcast.py
