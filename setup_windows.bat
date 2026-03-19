@echo off
setlocal enabledelayedexpansion

:: Function to check if a command exists
:check_command {
    where /q %1
}

:: Check if Python is installed
echo Checking if Python is installed...
check_command python
if errorlevel 1 (
    echo Python is not installed. Please install Python and try again.
    exit /b 1
)

echo Python is installed.

:: Check if pip is available
echo Checking if pip is available...
check_command pip
if errorlevel 1 (
    echo pip is not available. Please ensure pip is installed and try again.
    exit /b 1
)

echo pip is available.

:: Install dependencies from requirements.txt
if exist requirements.txt (
    echo Installing dependencies from requirements.txt...
    pip install -e .
    if errorlevel 1 (
        echo Failed to install dependencies. Please check requirements.txt and try again.
        exit /b 1
    )
    echo Dependencies installed successfully.
) else (
    echo requirements.txt not found. Please ensure it is in the project root.
    exit /b 1
)

:: Download and install Tesseract OCR
set TESSERACT_VERSION=5.3.0
set TESSERACT_INSTALLER=tesseract-%TESSERACT_VERSION%-setup.exe

echo Downloading Tesseract OCR...
wget https://github.com/tesseract-ocr/tesseract/releases/download/v%TESSERACT_VERSION%/%TESSERACT_INSTALLER%

if errorlevel 1 (
    echo Failed to download Tesseract OCR. Please check your internet connection and try again.
    exit /b 1
)

echo Installing Tesseract OCR...
%TESSERACT_INSTALLER% /silent
if errorlevel 1 (
    echo Failed to install Tesseract OCR. Please try manually installing Tesseract.
    exit /b 1
)

echo Tesseract OCR installed successfully.

:: Verify Tesseract installation
echo Verifying Tesseract installation...
check_command tesseract
if errorlevel 1 (
    echo Tesseract installation verification failed. Please check the installation.
    exit /b 1
)

echo Tesseract installation verified successfully.

echo Setup completed successfully!