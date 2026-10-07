import requests
import re

session = requests.Session()
r = session.get('http://127.0.0.1:8050/')
for s in re.findall(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL):
    if 'state-success' in s:
        with open('scratch/js_dump.js', 'w', encoding='utf-8') as f:
            f.write(s)
        print("Dumped JS to scratch/js_dump.js, length:", len(s))
