#coding=utf-8
import requests
import time
import base64

t1 = time.time()
s = requests
with open('text4.txt') as f:
    str1 = f.read()
data={'strtemp':str1}
r = s.post('http://192.168.132.151:8879/txtsimilar', data)

print(r.text)
print('time cost:', time.time() - t1)
