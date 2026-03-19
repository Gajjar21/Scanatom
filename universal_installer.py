import os
import platform
import subprocess
import sys

# Function to print messages in color
def print_color(text, color):
    colors = {'red': '\033[91m', 'green': '\033[92m', 'yellow': '\033[93m', 'blue': '\033[94m', 'reset': '\033[0m'}
    print(f"{colors[color]}{text}{colors['reset']}")

# Function to check Python installation
def check_python():
    try:
        subprocess.check_output(['python', '--version'], stderr=subprocess.STDOUT)
        print_color('Python is installed.', 'green')
    except subprocess.CalledProcessError:
        print_color('Python is not installed. Please install Python.', 'red')
        sys.exit(1)

# Function to check Homebrew (for Mac)
def check_homebrew():
    try:
        subprocess.check_output(['brew', '--version'], stderr=subprocess.STDOUT)
        print_color('Homebrew is installed.', 'green')
    except subprocess.CalledProcessError:
        print_color('Homebrew is not installed. Installing Homebrew.', 'yellow')
        subprocess.run(['/bin/bash', '-c', "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"])

# Function to install requirements
def install_requirements():
    print_color('Installing requirements from requirements.txt', 'blue')
    subprocess.run(['pip', 'install', '-r', 'requirements.txt'])
    print_color('Requirements installed successfully.', 'green')

# Function to install Tesseract OCR
def install_tesseract():
    if platform.system() == 'Darwin':  # macOS
        print_color('Installing Tesseract using Homebrew...', 'blue')
        subprocess.run(['brew', 'install', 'tesseract'])
    elif platform.system() == 'Windows':
        print_color('Please install Tesseract from https://github.com/UB-Mannheim/tesseract/wiki', 'yellow')
    else:
        print_color('Unsupported OS for Tesseract installation.', 'red')

# Main function
if __name__ == '__main__':
    os_type = platform.system()
    print_color(f'Detecting OS: {os_type}', 'blue')

    check_python()
    if os_type == 'Darwin':
        check_homebrew()
        # Add logic for generating app launchers on macOS
    elif os_type == 'Windows':
        # Add logic for generating app launchers on Windows
        pass
    else:
        print_color('Unsupported operating system.', 'red')
        sys.exit(1)

    install_requirements()
    install_tesseract()
    print_color('Universal installation completed.', 'green')
