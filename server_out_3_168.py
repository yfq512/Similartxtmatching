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

def select_fromsql(publish_time_, pre_news_path='../logs/pre_news_168.npy'):
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
         news.item_id as news_id,
         news.source_id as customer_id,
         cont.content,
         news.publish_time as publish_time
        FROM
         t_province_news news LEFT JOIN t_province_news_content cont on news.item_id = cont.item_id and news.source_id=cont.source_id
        WHERE
      news.news_status = 1 and 
      publish_time  > {}
    """.format(publish_time_)
    
    cursor.execute(sql)
    news=cursor.fetchall()
    
    print('缓存长度：', len(outlist))
    # 列表拓展
    outlist.extend(news)
    
    print('本次加载长度', len(news))

    # 过滤掉半年前的news
    _outlist = []
    _now_time = time.strftime("%F") ##年月日格式
    now_time = int(_now_time.split('-')[0])*365 + int(_now_time.split('-')[1]) * 30 + int(_now_time.split('-')[2])
    for n in outlist:
        publishtime = n.get('publish_time')
        _publishtime = time.strftime("%Y-%m-%d", time.localtime(publishtime)) # 时间戳转格式化日期
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
        return news[-1].get('publish_time')
    except:
        return publish_time_

if __name__ == "__main__":
    pre_news_path = '../logs/pre_news_168.npy'
    try:
        os.system('cat news_id_load_count_168.log | tail -n 1 > read_news_id_load_count_168.txt')
        f2 = open('read_news_id_load_count_168.txt','r')
        publish_time_ = f2.read().split('\n')[0]
        if publish_time_ == '':
            publish_time_ = 1601563744
    except:
        publish_time_ = 1601563744 # 2020年10月1日
    print('==================', publish_time_, type(publish_time_))
    # id_ = '307892'
    conn = pymysql.Connect(host='192.168.132.168', port=4000, user='root', password='ccwb@2021)#)#', db='fusion_media',charset='UTF8MB4')
    cursor=conn.cursor(pymysql.cursors.DictCursor)
    
    while True:
        t1 = time.time()
        cnt3 = 0 # 插入次数
        # 更新监控news列表
        publish_time_ = int(select_fromsql(publish_time_))
        with open('news_id_load_count_168.log','a') as f1:
            f1.write(str(publish_time_)+'\n')
            f1.close()
        'a' + 1
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
            try:
                info = json.loads(requests.post('http://0.0.0.0:8881/txtsimilar', data).text)
                info = info.get('similar_infos')
            except:
                info = []
            similar_title_id_list = []
            if len(info) > 0:
                print('开始插入')

            for similar_title in info:
                title = similar_title.get('title')
                # print('================================')
                #print('11111111111',content,jk_news_id)
                
                temp_similar_title_id = similar_title.get('title_id')
                print('222222222222',title,temp_similar_title_id)
                temp_similar_score = similar_title.get('similar_sorce')
                web_source = similar_title.get('web_source')
                pushtime = similar_title.get('pushtime')
                source_url = similar_title.get('title_url')
                create_time = time.strftime("%F %H:%M:%S") ##24小时格式
                update_time = time.strftime("%F %H:%M:%S") ##24小时格式
                # print('dasdsa',web_source,pushtime)
                # 插入数据库
                sql = """
                insert into t_spread_similar_news(news_id, spread_news_id, similarity,source,source_url,publish_time, create_time, update_time) values('{}', '{}', {},'{}','{}','{}',now(), now())
                """.format(jk_news_id,temp_similar_title_id,temp_similar_score,web_source,source_url,pushtime)
                try:
                # print(sql)
                    cursor.execute(sql)
                    conn.commit()
                    cnt3 = cnt3 + 1
                    print('插入成功')
                except Exception as e:
                    conn.rollback()
                    try:
                        sql1 = """
                        update t_spread_similar_news set similarity='{}',source='{}',source_url='{}',publish_time='{}',update_time=now() where news_id='{}' and spread_news_id='{}'
                        """.format(temp_similar_score,web_source,source_url,pushtime, jk_news_id, temp_similar_title_id)
                        cursor.execute(sql1)
                        conn.commit()
                        cnt3 = cnt3 + 1
                        print('更新成功')
                    except:
                        conn.rollback()
                        print('更新失败')

        print('本次更新耗时：(s)', time.time()-t1)
        print('本次更新插入数据条数：',cnt3)
        print(publish_time_)
        time.sleep(600)
    cursor.close()
    conn.close()
