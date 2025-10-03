import urllib.request, urllib.parse, urllib.error
import json
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = input('Enter - ')
print('Retrieving', url)
uh = urllib.request.urlopen(url, context=ctx)
data = uh.read()

info = json.loads(data)


count = 0
sum = 0

for item in info['comments']:
    num = item['count']
    sum = sum + int(num)
    count = count + 1

print('Count: ', count)
print('Sum: ', sum)