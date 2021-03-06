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
import pickle
import fcntl

def get_features(s):
    width = 3
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]

    
def uptxt():

    page_line = 1000 # 每页条数
    cnt2 = 0
    try:
        os.system('cat es_counts.log | tail -n 1 > read_es_count.txt')
        f5 = open('read_es_count.txt','r')
        cnt1 = int(f5.read().split(',')[0])
        page = int(cnt1/1000)
    except:
        cnt1 = 0
        page = 0
    print('开始数量与页数：', cnt1, page)
    while True:
        count = es.count(index=index, body={
                          "query": {
                            "match": {
                              "applicationid": "开屏"
                            }
                          }
                        })['count']

        if count > (page+1)*page_line: # 更新，当数量大于单页数量再更新
            with open(log_es_counts_path,'a') as f1:
                f1.write(str(cnt1) + ',' + str(count))
                f1.write('\n')
                f1.close()
            temp_id_list = []
            temp_simhash_value_list = []
            for x in xrange(page,page+1):
                rs = es.search(index=index,body={
                  "query": {
                    "match": {
                      "applicationid": "开屏"
                    }
                  },
                  "from":x*page_line,
                  "size":page_line
                })
                # 更新simhashindex
                
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
                        cnt1 = cnt1 + 1
                        with open(log_loadsucces_path, 'a') as f3:
                            f3.write(text_key)
                            f3.write('\n')
                            f3.close
                    except: # 加载失败
                        with open(log_loadfailed_path,'a') as f2: # 仅记录条数
                            f2.write(str(page*page_line+cnt1))
                            f2.write('\n')
                            f2.close()
                    cnt2 = cnt2 + 1
                    
                    if cnt2%100==0 and cnt2>=900001:
                        f4 = open(simhash_index_path)
                        fcntl.flock(f4,fcntl.LOCK_EX) # 获取锁
                        with open(simhash_index_path, 'wb') as simhash_save:
                            pickle.dump(simhash_index, simhash_save)
                        fcntl.flock(f4,fcntl.LOCK_UN) # 解除锁
                    # 存对象
                    elif cnt2%10000==0 and cnt2<900001:
                        try:
                            f4 = open(simhash_index_path)
                            fcntl.flock(f4,fcntl.LOCK_EX) # 获取锁
                            with open(simhash_index_path, 'wb') as simhash_save:
                                pickle.dump(simhash_index, simhash_save)
                            fcntl.flock(f4,fcntl.LOCK_UN) # 解除锁
                        except:
                            with open(simhash_index_path, 'wb') as simhash_save:
                                pickle.dump(simhash_index, simhash_save)
                    else:
                        pass

            page = page + 1
        else:
            time.sleep(60)
                

if __name__ == "__main__":
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
    # # 本地快速加载测试，不需要激活
    # with open('simhash_index.pickle', 'rb') as f:
    #     simhash_index = pickle.load(f)
    ## 记录log
    log_es_counts_path = 'es_counts.log' # 记录当前操作到es第N条数据，当重载数据时，初始化到此处，再自动add新数据(counts+id)
    log_loadsucces_path = 'loadsucces.log' # 记录加载成功的ID、和pushtime
    log_loadfailed_path = 'loadfailed.log' # 记录加载出错的counts
    save_txtnpy_path = 'txt_feature.npy' # 记录simhash特征，id+pushtime，便于在后期重新加载simhashIndex对象时再次提取特征，而浪费时间
    test_text_path = 'test_text.txt' # 记录前10个contens，用于测试
    txt_data_path = 'txt_data.npy'
    simhash_index_path = 'simhash_index.pickle'
    last_cnt = 15000000 # 20210119更新到1500w条
    print('更新simhash')
    uptxt()

