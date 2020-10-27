#coding=utf-8
import requests
import time
import base64

t1 = time.time()
s = requests
with open('text3.txt') as f:
    imgbase64 = f.read()
data={'addstr':imgbase64}
r = s.post('http://192.168.132.151:8879/updatatxts', data)

print(r.text)
print('time cost:', time.time() - t1)
