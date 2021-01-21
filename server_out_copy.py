#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 15:56:38 2021

@author: kpinfo
"""
from elasticsearch import Elasticsearch
from urllib3.connectionpool import xrange
from flask import Flask,render_template,request
import time, re, json, os
from simhash import Simhash, SimhashIndex
import numpy as np

def get_features(s):
    width = 3
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]

## 初始化
# 初始化数据库
es_host = "192.168.132.152"
port = 9200
timeout = 15000
index = "fwnews"
es = Elasticsearch(hosts=es_host,port=port,timeout=timeout)
# 初始化simhashIndex
data = {
    'test_id': u'How are you? I Am fine. blar blar blar blar blar Thanks.',
}
objs = [(k, Simhash(get_features(v))) for k, v in data.items()]
simhash_index = SimhashIndex(objs, k=3)

## 记录log
log_es_counts_path = 'es_counts.log' # 记录当前操作到es第N条数据，当重载数据时，初始化到此处，再自动add新数据(counts+id)
log_loadsucces_path = 'loadsucces.log' # 记录加载成功的ID、和pushtime
log_loadfailed_path = 'loadfailed.log' # 记录加载出错的counts
save_txtnpy_path = 'txt_feature.npy' # 记录simhash特征，id+pushtime，便于在后期重新加载simhashIndex对象时再次提取特征，而浪费时间
test_text_path = 'test_text.txt' # 记录前10个contens，用于测试
txt_data_path = 'txt_data.npy'
last_cnt = 15000000 # 20210119更新到1500w条

## server
app = Flask(__name__)

@app.route("/matchtxt",methods = ['GET', 'POST'])
def matchtxt():
    if request.method == "POST":
        text = request.form.get('text')
        text_hash = Simhash(get_features(text))
        result = simhash_index.get_near_dups(text_hash)
        return json.dumps({'result':result})
    else:
        pass
    
@app.route("/uptxt",methods = ['GET', 'POST'])
def uptxt():
    if request.method == "POST":
        '''
        sign==1时，从save_txtnpy_path处加载simhashindex，当npy加载完毕后再从log_es_counts_path记录的最新counts处自动顺序加载；
        sign==0时，删除log_es_counts_path，从es数据库counts==0处加载；
        
        '''
        sign = request.form.get('sign')
        sign = int(sign)
        if sign == 0:
            page = 0
            page_line = 1000 # 每页条数
            while True:
                count = es.count(index=index)['count']

                if count > (page+1)*page_line: # 更新，当数量大于单页数量再更新
                    with open(log_es_counts_path,'a') as f1:
                        f1.write(str(page*page_line) + ',' + str(count))
                        f1.write('\n')
                        f1.close()
                    temp_id_list = []
                    temp_simhash_value_list = []
                    for x in xrange(page,page+1):
                        rs = es.search(index=index,body={
                          "query":{
                        "match_all":{}
                          },
                          "from":x*page_line,
                          "size":page_line
                        })
                        # 更新simhashindex
                        cnt1 = 0
                        for info in rs['hits']['hits']:
                            try: # 加载成功
                                info_dict = info.get('_source')
                                _id = info_dict.get('id') # 获取文章id
                                publish_time = info_dict.get('publish_time') # 获取文章发表时间
                                content = info_dict.get('content') # 获取正文本
                                text_feature = Simhash(get_features(content))
                                text_feature_value = text_feature.value
                                text_key = _id + '---' + publish_time
                                simhash_index.add(text_key, text_feature)
                                temp_id_list.append(text_key)
                                temp_simhash_value_list.append(text_feature_value)
                                with open(log_loadsucces_path, 'a') as f3:
                                    f3.write(text_key)
                                    f3.write('\n')
                                    f3.close
                            except: # 加载失败
                                with open(log_loadfailed_path,'a') as f2: # 仅记录条数
                                    f2.write(str(page*page_line+cnt1))
                                    f2.write('\n')
                                    f2.close()
                            cnt1 = cnt1 + 1
                    # 存txtdata
                    if os.path.exists(txt_data_path): # 存在
                        txtdata = np.load(txt_data_path, allow_pickle=True).item()
                        id_list = txtdata.get('id')
                        simhash_value_list = txtdata.get('simhash_value')
                        id_list.extend(temp_id_list)
                        simhash_value_list.extend(temp_simhash_value_list)
                        np.save(txt_data_path,{'id':id_list, 'simhash_value':simhash_value_list})
                    else: # 不存在，则新建
                        np.save(txt_data_path,{'id':temp_id_list,'simhash_value':temp_simhash_value_list})
                                    

                    page = page + 1
                
                else:# 等待爬虫60s
                    time.sleep(20)
        elif sign == 1: # 中断后加载
            pass
        else:
            pass
    else:
        pass
    
if __name__ == "__main__":
    host = '0.0.0.0'
    port = '8889'
    app.run(debug=True, host=host, port=port, threaded=True)
