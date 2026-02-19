import os
import requests
import re

# 설정: 다운로드할 자원 및 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, 'static')
VENDOR_DIR = os.path.join(STATIC_DIR, 'vendor')
FONTS_DIR = os.path.join(STATIC_DIR, 'fonts')

ASSETS = {
    'fontawesome': {
        'css': [
            'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
        ],
        'base_url': 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/',
        'sub_dirs': ['webfonts']
    },
    'codemirror': {
        'css': [
            'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/codemirror.min.css',
            'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/theme/dracula.min.css'
        ],
        'js': [
            'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/codemirror.min.js',
            'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/mode/python/python.min.js'
        ]
    }
}

def ensure_dirs():
    for d in [VENDOR_DIR, FONTS_DIR]:
        if not os.path.exists(d):
            os.makedirs(d)

def download_file(url, target_path):
    print(f"Downloading {url} -> {target_path}")
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    response = requests.get(url)
    if response.status_code == 200:
        with open(target_path, 'wb') as f:
            f.write(response.content)
        return True
    else:
        print(f"Failed to download {url}: {response.status_code}")
        return False

def download_assets():
    ensure_dirs()
    
    # 1. Font Awesome
    fa_dir = os.path.join(VENDOR_DIR, 'fontawesome')
    for css_url in ASSETS['fontawesome']['css']:
        target = os.path.join(fa_dir, 'css', os.path.basename(css_url))
        if download_file(css_url, target):
            # Parse webfonts from CSS
            with open(target, 'r') as f:
                content = f.read()
                # Find ../webfonts/fa-solid-900.woff2 etc
                fonts = re.findall(r'url\(\.\.\/webfonts\/(.+?)\)', content)
                for font in set(fonts):
                    font_url = ASSETS['fontawesome']['base_url'] + 'webfonts/' + font
                    font_target = os.path.join(fa_dir, 'webfonts', font)
                    download_file(font_url, font_target)

    # 2. CodeMirror
    cm_dir = os.path.join(VENDOR_DIR, 'codemirror')
    for css_url in ASSETS['codemirror']['css']:
        download_file(css_url, os.path.join(cm_dir, 'css', os.path.basename(css_url)))
    for js_url in ASSETS['codemirror']['js']:
        # Keep directory structure for modes
        if 'mode/' in js_url:
            path_parts = js_url.split('/')
            mode_idx = path_parts.index('mode')
            filename = "/".join(path_parts[mode_idx:])
            download_file(js_url, os.path.join(cm_dir, 'js', filename))
        else:
            download_file(js_url, os.path.join(cm_dir, 'js', os.path.basename(js_url)))

    # 3. Google Fonts (Inter) - Simplified
    # Note: Downloading Google Fonts properly is complex via script. 
    # Usually better to provide a link to a packaged version or download ttf.
    print("\n[NOTE] Google Fonts (Inter) download is complex via script due to dynamic CSS.")
    print("Please download 'Inter' font from https://fonts.google.com/specimen/Inter and place in static/fonts/")

if __name__ == "__main__":
    try:
        import requests
    except ImportError:
        print("Error: 'requests' library is required. Run 'pip install requests'")
    else:
        download_assets()
        print("\nAsset download complete.")
