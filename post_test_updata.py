#coding=utf-8
import requests
import time
import base64
import json

t1 = time.time()
s = requests
data={'sign':0}
#r = s.post('http://192.168.132.151:8879/txtsimilar', data)
r = s.post('http://0.0.0.0:8880/uptxt', data)
