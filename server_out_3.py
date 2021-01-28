#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 15:23:26 2021
程序重启时，要删除本地文件：pre_news.npy
功能：更新监控文章相似列表
1. 遍历监控列表id；
2. 获取文本数据；
3. 遍历文本数据，查找相似id及相似度；
4. 解析id，填写信息；
5. 获取传播力指数，计算参数；
@author: kpinfo
"""
import pymysql
import os, time, json
import numpy as np
import requests
import fcntl
import re

def select_fromsql(_id, pre_news_path='pre_news.npy'):
    
    outlist = []
    # 加载本地缓存
    if os.path.exists(pre_news_path):
        outlist = np.load(pre_news_path, allow_pickle=True)
        outlist = list(outlist)
    else:
        outlist = []
    
    # 更新缓存

    # sql="select id,link FROM wx_gzh_articles where rich_content is null"
    sql="""
    SELECT
     id,
     news_id,
     customer_id,
     content,
     from_unixtime( publish_time ) as publish_time
    FROM
     ccwb_customer_news 
    WHERE
     publish_time >= unix_timestamp(
     DATE_SUB( CURDATE(), INTERVAL 6 MONTH )) 
     and id > {}
    """.format(_id)
    
    cursor.execute(sql)
    news=cursor.fetchall()

    
    # print('dsadasdasdsa',news)
    print('缓存长度：', len(outlist))
    # 列表拓展
    outlist.extend(news)
    
    # 更新_id
    _id = str(int(_id) + len(news))
    
    print('本次加载长度', len(news))
    
    # 过滤掉半年前的news
    _outlist = []
    _now_time = time.strftime("%F") ##年月日格式
    now_time = int(_now_time.split('-')[0])*365 + int(_now_time.split('-')[1]) * 30 + int(_now_time.split('-')[2])
    for n in outlist:
        publishtime = str(n.get('publish_time'))
        _publishtime = publishtime.split(' ')[0]
        __publishtime = int(_publishtime.split('-')[0])*365 + int(_publishtime.split('-')[1]) * 30 + int(_publishtime.split('-')[2])
        
        if (now_time-__publishtime) < 181:
            _outlist.append(n)
    # 保存在本地
    try:
        f1 = open(pre_news_path)
        fcntl.flock(f1,fcntl.LOCK_EX) # 获取锁
        np.save(pre_news_path, _outlist)
        fcntl.flock(f1,fcntl.LOCK_UN) # 解除锁
    except: # 首次加载
        np.save(pre_news_path, _outlist)
    
    try:
        return news[-1].get('id')
    except:
        return _id

if __name__ == "__main__":
    pre_news_path = 'pre_news.npy'
    id_ = '244234'
    # id_ = '307892'
    conn = pymysql.Connect(host='192.168.132.160', port=3306, user='root', password='Ca8thivwadFic#Python', db='fusion_media',charset='UTF8MB4')
    cursor=conn.cursor(pymysql.cursors.DictCursor)
    
    while True:
        t1 = time.time()
        cnt3 = 0 # 插入次数
        # 更新监控news列表
        id_ = select_fromsql(id_)
        
        # 从本地加载并遍历监控news列表
        news = list(np.load(pre_news_path, allow_pickle=True))
        for news_i in news:
            jk_news_id = news_i.get('news_id')
            content = news_i.get('content')
            if not content:
                continue
            if len(content) < 20: # 文本过短的new直接跳过
                continue
            is_content = re.findall('[\u4e00-\u9fa5]+', content)
            # print('>>>>>>>>>>>>>>>',jk_news_id)
            if not is_content:
                continue
            data={'strtemp':content}
            info = json.loads(requests.post('http://0.0.0.0:8881/txtsimilar', data).text)
            info = info.get('similar_infos')
            similar_title_id_list = []
            if len(info) > 0:
                print('开始插入')

            for similar_title in info:
                title = similar_title.get('title')
                # print('================================')
                print('11111111111',content,jk_news_id)
                
                temp_similar_title_id = similar_title.get('title_id')
                print('222222222222',title,temp_similar_title_id)
                temp_similar_score = similar_title.get('similar_sorce')
                web_source = similar_title.get('web_source')
                pushtime = similar_title.get('pushtime')
                source_url = similar_title.get('title_url')
                # print('dasdsa',web_source,pushtime)
                # 插入数据库
                sql = """
                insert into t_spread_similar_news(news_id, spread_news_id, similarity,source,source_url,publish_time) values('{}', '{}', {},'{}','{}','{}')
                """.format(jk_news_id,temp_similar_title_id,temp_similar_score,web_source,source_url,pushtime)
                try:
                    # print(sql)
                    cursor.execute(sql)
                    conn.commit()
                    cnt3 = cnt3 + 1
                except Exception as e:
                    pass

            # print(content)
            # break
        print('本次更新耗时：(s)', time.time()-t1)
        print('本次更新插入数据条数：',cnt3)
        print(id_)
        time.sleep(600)
    cursor.close()
    conn.close()