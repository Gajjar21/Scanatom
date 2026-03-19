#!/bin/bash

# Setup script for Mac

# Function to print messages in color
print_colored() {
    local color=$1
    local message=$2
    case $color in
        "green") echo "\033[0;32m$message\033[0m" ;;  # Green
        "red") echo "\033[0;31m$message\033[0m" ;;  # Red
        "yellow") echo "\033[0;33m$message\033[0m" ;;  # Yellow
        "blue") echo "\033[0;34m$message\033[0m" ;;  # Blue
        *) echo "$message" ;;  # No color
    esac
}

# Check Homebrew installation
print_colored "blue" "Checking for Homebrew..."
if ! command -v brew &> /dev/null
then
    print_colored "red" "Homebrew not found! Installing..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
    print_colored "green" "Homebrew is already installed."
fi

# Check Python installation
print_colored "blue" "Checking for Python..."
if ! command -v python3 &> /dev/null
then
    print_colored "red" "Python not found! Installing..."
    brew install python
else
    print_colored "green" "Python is already installed."
fi

# Check pip installation
print_colored "blue" "Checking for pip..."
if ! command -v pip3 &> /dev/null
then
    print_colored "red" "pip not found! Installing..."
    brew install pip
else
    print_colored "green" "pip is already installed."
fi

# Install dependencies
print_colored "yellow" "Installing dependencies..."
brew install tesseract
pip3 install -r requirements.txt

# Generating icons (example functionality)
print_colored "blue" "Generating application icon..."
# Placeholder for icon generation script here
# e.g., convert logo.png -resize 512x512 app_icon.png

# Creating app launchers
print_colored "yellow" "Creating application launcher..."
# Example functionality to create a launcher
echo "#!/bin/bash
open /path/to/your/app" > /usr/local/bin/your_app
chmod +x /usr/local/bin/your_app

print_colored "green" "Setup completed successfully!"