from PIL import Image, ImageDraw, ImageFont
import os
import subprocess

# Constants for dimensions and colors
sizes = [16, 32, 64, 128, 256, 512, 1024]
background_color = "#662D91"  # FedEx purple
accent_color = "#FF9900"       # FedEx orange
text_color = "white"
text = "GJ21-Scan"

def create_icon(size):
    image = Image.new('RGBA', (size, size), background_color)
    draw = ImageDraw.Draw(image)

    try:
        # Load a font
        font = ImageFont.truetype("Arial.ttf", size // 10)
    except IOError:
        # Fallback if the font is not available
        font = ImageFont.load_default()

    # Calculate text size and position
    text_size = draw.textsize(text, font=font)
    text_position = ((size - text_size[0]) // 2, (size - text_size[1]) // 2)
    
    draw.text(text_position, text, fill=text_color, font=font)
    
    return image

def save_icons():
    icns_path = "GJ21-Scan.iconset"
    os.makedirs(icns_path, exist_ok=True)

    for size in sizes:
        icon = create_icon(size)
        icon.save(f"{icns_path}/{size}x{size}.png")

    # Convert to ICNS using iconutil
    subprocess.run(["iconutil", "-c", "icns", icns_path])
    # Optional: Cleanup the iconset directory
    # os.rmdir(icns_path) # Uncomment if you want to delete after creation

if __name__ == "__main__":
    save_icons()