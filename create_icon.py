from PIL import Image, ImageDraw, ImageFont

# Create a purple icon
icon_size = (300, 300)
image = Image.new('RGB', icon_size, (102, 45, 145))  # FedEx purple

# Initialize ImageDraw
draw = ImageDraw.Draw(image)

# Draw an orange rectangle
rectangle_position = [(50, 100), (250, 250)]  # Position of rectangle
rectangle_color = (255, 153, 0)  # FedEx orange
draw.rectangle(rectangle_position, fill=rectangle_color)

# Load a font
# Note: The following line assumes a default font. For custom fonts, the font file path needs to be added.
try:
    font = ImageFont.truetype('Arial.ttf', size=24)
except IOError:
    font = ImageFont.load_default()

# Add text to the image
text = "GJ21-Scan"
text_position = (75, 150)
text_color = (255, 255, 255)  # White text
draw.text(text_position, text, fill=text_color, font=font)

# Save the icon
image.save('create_icon.png')