import base64
import os
import re

# Load your HTML file
with open('C:\Users\hm3878\Downloads\Migrato.AI\Migration Application\Architecture Diagram\functional_flow_1.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Function to replace image src with base64
def replace_img_src(match):
    img_path = match.group(1)
    if not os.path.isfile(img_path):
        print(f"Warning: {img_path} not found.")
        return match.group(0)
    ext = os.path.splitext(img_path)[1].lower()
    mime = {
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml'
    }.get(ext, 'application/octet-stream')
    with open(img_path, 'rb') as img_f:
        b64_data = base64.b64encode(img_f.read()).decode('utf-8')
    return f'src="data:{mime};base64,{b64_data}"'

# Regex to find img src attributes (assuming simple quotes or double quotes)
pattern = r'src=["\']([^"\']+)["\']'

# Replace all local image src with base64 data URIs
new_html = re.sub(pattern, replace_img_src, html)

# Save the new single HTML file
with open('C:\Users\hm3878\Downloads\Migrato.AI\Migration Application\Architecture Diagram\output_single_file.html', 'w', encoding='utf-8') as f:
    f.write(new_html)

print("Conversion completed! Single HTML file generated as output_single_file.html")