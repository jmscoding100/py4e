
import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET
import ssl
sum = 0

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = "https://py4e-data.dr-chuck.net/comments_2284392.xml"

serviceurl = urllib.request.Request(url)
uh = urllib.request.urlopen(url, context=ctx)
data = uh.read()

tree = ET.fromstring(data)
lst = tree.findall('comments/comment')

for item in lst:
    sum = sum + int(item.find('count').text)
print("Total:",sum)