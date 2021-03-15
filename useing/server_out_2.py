#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 16:59:56 2021

@author: kpinfo

服务一：
接受text文本，返回相似的id,title,titleurl,source,pushtime等
"""
from elasticsearch import Elasticsearch
# from kafka import KafkaConsumer, TopicPartition
from urllib3.connectionpool import xrange
from flask import Flask,request
import time, json, re
import numpy as np
import requests
from simhash import Simhash, SimhashIndex
import pickle
import fcntl

# ## 初始化kafka队列，监控待监测文章
# consumer = KafkaConsumer(bootstrap_servers=['192.168.132.111:9092'], group_id='image_proccess')
# consumer.assign([
#     TopicPartition(topic="customer_news", partition=0)
# ])
# consumer.seek(TopicPartition(topic="customer_news", partition=0), 0)
# """
# for msg in consumer:
#     info = json.loads(msg.value)
#     info2 = info.get('data')[0]
#     create_time = info2.get('create_time')
#     print(create_time)
# """

## 初始化数据库，提供总数量查询
es_host = "192.168.132.152"
port = 9200
timeout = 15000
index = "fwnews"
es = Elasticsearch(hosts=es_host,port=port,timeout=timeout)

es_db = Elasticsearch(["192.168.132.152"], http_auth=('elastic', 'Ynwy778123'), port=9200)
"""
count = es.count(index=index)['count']
"""

def get_features(s):
    width = 3
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]

def get_detais_byid(id_list, text):
    text_hash = Simhash(get_features(text))
    out_txtpaths = []
    scores = []
    in_title_url = []
    for id_title in id_list:
        body = {
            "query":{
                "term":{
                    "id.keyword":id_title
                }
            }
        }
        info_org = es_db.search(index='fwnews',body=body)
        try:
            info_dst = info_org.get('hits').get('hits')[0].get('_source')
        except:
            continue
        title = info_dst.get('title')
        title_url = info_dst.get('source_url')
        if not title_url in in_title_url:
            in_title_url.append(title_url)
        else:
            continue
        web_source = info_dst.get('source')
        pushtime = info_dst.get('publish_time')
        temp_text = info_dst.get('content')
        temp_feature_text = Simhash(get_features(temp_text))
        d = text_hash.distance(temp_feature_text) # 为了使d0～9的id检测出来，在基础服务时需要设置k=10
        similar_sorce = (100-d)/100
        scores.append(similar_sorce)
        out_txtpaths.append({'title_id':id_title, 'title':title, 'title_url':title_url, 'similar_sorce':similar_sorce, 'web_source':web_source, 'pushtime':pushtime})
    try:
        return out_txtpaths, max(scores)
    except:
        return out_txtpaths, 0
## server
app = Flask(__name__)

@app.route("/upsimhash",methods = ['GET', 'POST'])
def upsimhash():
    if request.method == "POST":
        cnt = 0
        while True:
            time.sleep(1)
            if cnt%600==0:
                f4 = open(simhash_index_path)
                with open(simhash_index_path, 'rb') as f:
                    simhash_index = pickle.load(f)
                fcntl.flock(f4,fcntl.LOCK_UN) # 解除锁
                now_time2 = time.strftime("%F") ##年月日格式
                with open('update_simhash.log', 'a') as ff:
                    ff.write('simihash 正常更新 ' + str(time.time())+'\n')
                    ff.close()
                fcntl.flock(f4,fcntl.LOCK_UN) # 解除锁
            cnt = cnt + 1
    else:
        pass


@app.route("/txtsimilar",methods = ['GET', 'POST'])
def txtsimilar():
    if request.method == "POST":
        text = request.form.get('strtemp')
        if not text:
            return json.dumps({'sign':-1, 'similar_value_max':0, 'similar_infos':[]})
        
        # 核心代码
        s1 = Simhash(get_features(text))
        result = simhash_index.get_near_dups(s1)
        result = list(set(result)) # 去重复
        id_list = []
        for n in result:
            id_list.append(n.split('---')[0])
        sign = -1
        similar_value_max = 0
        similar_infos = []
        # try:
        similar_infos, similar_value_max = get_detais_byid(id_list, text)
        if len(similar_infos) > 0: # 存在相似文本
            sign = 1
            return json.dumps({'sign':sign, 'similar_value_max':similar_value_max, 'similar_infos':similar_infos})
        else: # 不存在相似文本
            sign = 0
            return json.dumps({'sign':sign, 'similar_value_max':similar_value_max, 'similar_infos':similar_infos}) # orgtxtpaths similar up limit value
        # except: # 运行出错，结果无效
            # return json.dumps({'sign':sign, 'similar_value_max':similar_value_max, 'similar_infos':similar_infos}) # len(str) is too short

    else: # 无视get请求
        pass
    
if __name__ == "__main__":
    simhash_index_path = 'simhash_index.pickle'
    f3 = open(simhash_index_path)
    with open(simhash_index_path, 'rb') as f:
        simhash_index = pickle.load(f)
    fcntl.flock(f3,fcntl.LOCK_UN) # 解除锁
    host = '0.0.0.0'
    port = '8881'
    app.run(debug=True, host=host, port=port, threaded=True)
