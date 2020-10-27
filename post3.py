#coding=utf-8
import requests
import time
import base64

t1 = time.time()
s = requests
data={'delname':'ah35m1idb0z4lxywg2cj.txt'}
r = s.post('http://192.168.132.151:8879/deltxts', data)

print(r.text)
print('time cost:', time.time() - t1)
