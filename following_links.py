
from bs4 import BeautifulSoup
import urllib.request, urllib.parse, urllib.error
import ssl
import re

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
url = "https://py4e-data.dr-chuck.net/known_by_Maryk.html"


for i in range(7):
    html = urllib.request.urlopen(url, context=ctx).read()
    soup = BeautifulSoup(html, 'html.parser')
    tags = soup('a')
    count = 0
    for tag in tags:
        count = count +1
   
        
        if count>18:
            break
        url = tag.get('href', None)

print(url)