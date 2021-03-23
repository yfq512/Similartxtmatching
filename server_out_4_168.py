#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 15:44:54 2021
功能：计算传播力

@author: kpinfo
"""
from urllib3.connectionpool import xrange
import pymysql
import os, time, json
import numpy as np
import requests
import fcntl
import math
import re

## 获取监控news
def get_jk_news(jk_news_path):
    f1 = open(jk_news_path)
    fcntl.flock(f1,fcntl.LOCK_EX) # 获取锁
    out_news = list(np.load(jk_news_path, allow_pickle=True))

    fcntl.flock(f1,fcntl.LOCK_UN) # 解除锁
    return out_news

## 获取评论/阅读/点赞数
def get_news_info(appid,id):
    read,collection,good,share=(0,0,0,0)
    data=[{'appid': 8, 'ip': '192.168.132.145', 'port': '3307', 'user': 'fusion', 'pwd': 'TuDH5aCRPDOQAn3V'},{'appid': 13, 'ip': '192.168.132.171', 'port': '3307', 'user': 'fusion', 'pwd': 'eGk6boi2ni1nKHqU'}, {'appid': 14, 'ip': '192.168.132.172', 'port': '3307', 'user': 'fusion', 'pwd': '36QA0VBflAlDwEYW'}, {'appid': 15, 'ip': '192.168.132.173', 'port': '3307', 'user': 'fusion', 'pwd': '5CwjaXyV03hA9s3W'}, {'appid': 16, 'ip': '192.168.132.174', 'port': '3307', 'user': 'fusion', 'pwd': 'YUGMAS3MQRZggMLY'}, {'appid': 17, 'ip': '192.168.132.175', 'port': '3307', 'user': 'fusion', 'pwd': 'um2WetskT54DdnDt'}, {'appid': 18, 'ip': '192.168.132.176', 'port': '3307', 'user': 'fusion', 'pwd': 'i6r7RJvralfJAktb'}, {'appid': 19, 'ip': '192.168.132.177', 'port': '3307', 'user': 'fusion', 'pwd': 'R12KVFLXfCizhRoU'}, {'appid': 20, 'ip': '192.168.132.178', 'port': '3307', 'user': 'fusion', 'pwd': 'iM8IGrUL62QqaOgh'}, {'appid': 21, 'ip': '192.168.132.179', 'port': '3307', 'user': 'fusion', 'pwd': 'lzRa6OpCzDVPCo3D'}, {'appid': 22, 'ip': '192.168.132.180', 'port': '3307', 'user': 'fusion', 'pwd': 'frV0TevZd4v29DAe'}, {'appid': 23, 'ip': '192.168.132.181', 'port': '3307', 'user': 'fusion', 'pwd': 'GuaRDml9vo6ZL6cC'}, {'appid': 24, 'ip': '192.168.132.182', 'port': '3307', 'user': 'fusion', 'pwd': 'Wl1HwZVaYNRkRh9U'}, {'appid': 25, 'ip': '192.168.132.183', 'port': '3307', 'user': 'fusion', 'pwd': 'JR0sQZWrNtrLpgSL'}, {'appid': 26, 'ip': '192.168.132.186', 'port': '3307', 'user': 'fusion', 'pwd': 'JR0sQZWrNtrLpgSL'}, {'appid': 27, 'ip': '192.168.132.187', 'port': '3307', 'user': 'fusion', 'pwd': '8hI8xRiQprTOGFED'}, {'appid': 28, 'ip': '192.168.132.188', 'port': '3307', 'user': 'fusion', 'pwd': '9guDnVxUr928Kbo3'}, {'appid': 29, 'ip': '192.168.132.189', 'port': '3307', 'user': 'fusion', 'pwd': 'VLLXd1BKm426XkkA'}, {'appid': 30, 'ip': '192.168.132.190', 'port': '3307', 'user': 'fusion', 'pwd': '5VFfeWze2JCLlXDM'}, {'appid': 35, 'ip': '192.168.132.191', 'port': '3307', 'user': 'fusion', 'pwd': 'xia1izap6IrDJJZS'}, {'appid': 36, 'ip': '192.168.132.192', 'port': '3307', 'user': 'fusion', 'pwd': 'vkuAdzMkKKC7oyIM'}]
    for i in data:
        if i.get("appid")==int(appid):
            import time
            conn=pymysql.connect(host=i.get("ip"), user=i.get("user"), password=i.get("pwd"),
                                    database='cloud_news', port=int(i.get("port")), unix_socket=None,
                                    charset='utf8mb4',)

            cursor=conn.cursor()
            sql="select show_click_num,collection_num,good_num,share_num from t_news_meta where id=%s"
            cursor.execute(sql,[id,])
            read,collection,good,share=cursor.fetchone()
            cursor.close()
            conn.close()
            break

    return read,collection,good,share

## 计算转载次数
def get_webshare_count(similar_source_list, weights_list_org): # 相似列表，权重列表
    weight_list = []
    count_list = []
    for n in weights_list_org:
        temp_weignt = n.get('weight')
        weight_list.append(temp_weignt)
        temp_cnt = 0
        for m in similar_source_list:
            if m in n:
                temp_cnt = temp_cnt + 1
            else:
                pass
        count_list.append(min(2, temp_cnt))
    return weight_list, count_list

## 计算new传播力
def compute_spread_value(app_id, jk_news_id, push_time, similar_info_list): # app_id,自有文章id， 相似文章列表
    log_err = 2.7182818284

    ## 获取相似文章信息
    t1 = time.time()
    web_source_list = [] # 相似新闻来源list
    similar_id_list = []
    for similar_i in similar_info_list:
        web_source_list.append(similar_i.get('web_source'))
        similar_id_list.append(similar_i.get('title_id'))
    ## 网站权重
    ## 网站转载次数
    t2 = time.time()
    sql1="""SELECT name, weight FROM t_spread_important_channel"""
    cursor.execute(sql1)
    Weight_list_org=cursor.fetchall()
    t4 = time.time()
    Weight_list, Count_list = get_webshare_count(web_source_list, Weight_list_org)
    ## 参与转载的网站总数
    TracedsiteCount = len(list(set(web_source_list)))
    ## 获取评论数/阅读数/点赞数
    t3 = time.time()
    try:
        read,collection,good,_ = get_news_info(app_id, jk_news_id)
    except:
        read,collection,good,_ = 0,0,0,0
    comments_2_sum = 0
    reads_2_sum = 0
    agrees_2_sum = 0
    
    Channels_num = 0 # 重点频道刊登本文次数
    ChannelTime_sum = 0 # 重点频道刊登本文总时长
    for m in similar_id_list:
        try:
            sql2="""SELECT comment_num, read_num, good_num FROM ccwb_collected_kind_number WHERE news_id='{}'""".format(m)
            cursor.execute(sql2)
            c_r_a_nums_org=cursor.fetchone()
            if not c_r_a_nums_org:
                try:
                    comments_2_sum = comments_2_sum + int(c_r_a_nums_org.get('comment_num'))
                except:
                    pass
                try:
                    reads_2_sum = reads_2_sum + int(c_r_a_nums_org.get('read_num'))
                except:
                    pass
                try:
                    agrees_2_sum = agrees_2_sum + int(c_r_a_nums_org.get('good_num'))
                except:
                    pass
            print('爬虫评论数生效')
        except:
            print('从ccwb_collected_kind_number获取数据：评论数，阅读数，点赞数失败')
        try:
            sql3 = """SELECT * FROM t_spread_important_channel_news WHERE news_id='{}'""".format(m)
            cursor.execute(sql3)
            important_nums_org=cursor.fetchone()
            if not important_nums_org:
                if important_nums_org==None:
                    continue
                Channels_num = Channels_num + 1
                ChannelTime_sum = ChannelTime_sum + int(important_nums_org.get('publish_hour')) # 单位：分钟
            print('爬虫刊登时常生效')
        except:
            print('从t_spread_important_channel_news获取：刊登时常失败')
    Comments = comments_2_sum
    Reads = int(read) + reads_2_sum
    Agrees = int(good) + agrees_2_sum
    ## 重点频道刊登本文次数
    Channels = Channels_num
    ## 重点频道刊登本文总时长
    ChannelTime = ChannelTime_sum
    
    ## 计算M：媒体覆盖指数
    w_c_sum = 0
    for n in xrange(len(Weight_list)):
        w_c_sum = w_c_sum + Weight_list[n]*Count_list[n]
    M = (0.8*math.log(w_c_sum+1,log_err) + 0.2*math.log(5*TracedsiteCount+1,log_err))**2*10
    ## 计算C：重点频道刊登指数
    C = (0.6*math.log(10*Channels+1,log_err) + 0.4*math.log(ChannelTime+1,log_err))**2*10
    ## 计算R：网民反响指数
    R = (0.4*math.log(10*Comments+1,log_err) + 0.4*math.log(Reads+1,log_err) + 0.1*math.log(Agrees+1,log_err))**2*10
    spread_value = 0.3*M + 0.4*C + 0.3*R
    
    'a' + 1
    # 写传播力到数据库
    '''
      `news_id` : '新闻ID',
      `spread_num` : '传播指数',
      `media_num` : '媒体覆盖指数',
      `important_num` : '重点频道指数',
      `comment_num` : '网民反响指数',
      `read_num` : '阅读数',
      `publish_time` : '发稿时间',
      `status` : '0,表示不在进行追踪，1，表示继续追踪',
      `customer_id` : '县融id',
      `create_time` : '创建时间',
      `update_time` :'最后更新时间',
      'good_num':'点赞数',
      'review_num':'评论数'
    '''
    good_num = Agrees
    print('>>>>>>', good_num,type(good_num))
    review_num = Comments
    now_time = time.strftime("%F %H:%M:%S") ##24小时格式
    sql = """
    insert into t_spread_news(news_id, spread_num, media_num, important_num, 
                              comment_num, read_num, publish_time, 
                              customer_id, create_time, update_time, good_num, review_num) values('{}','{}','{}','{}','{}','{}','{}','{}',now(),now(),'{}','{}')
    """.format(jk_news_id,spread_value,M,C,R,Reads,push_time,app_id,good_num,review_num)
    sql11 = """update t_spread_news set spread_num='{}', media_num='{}', important_num='{}', 
                              comment_num='{}', read_num='{}', publish_time='{}', 
                              customer_id='{}', update_time=now(), good_num='{}', review_num='{}' where news_id='{}' """.format(spread_value,M,C,R,Reads,push_time,app_id,good_num,review_num,jk_news_id)
    try:
        cursor.execute(sql)
        conn.commit()
        print('插入成功',jk_news_id)
    except:
        conn.rollback()
        try:
            cursor.execute(sql11)
            conn.commit()
            print('更新成功',jk_news_id)
        except:
            conn.rollback()
            print('插入或更新数据库失败',jk_news_id)


if __name__ == "__main__":
    
    jk_news_path = '../logs/pre_news_168.npy'
    
    ## 连接数据库
    conn = pymysql.Connect(host='192.168.132.168', port=4000, user='root', password='ccwb@2021)#)#', db='fusion_media',charset='UTF8MB4')
    cursor=conn.cursor(pymysql.cursors.DictCursor)
    
    while True:
        ## 获取被监控news
        jk_news = get_jk_news(jk_news_path)
        print('获取监控新闻成功', len(jk_news))
        ## 计算new的传播力，并写入数据
        for jk_news_i in jk_news:
            jk_news_id = jk_news_i.get('news_id')
            app_id = jk_news_i.get('customer_id')
            push_time = jk_news_i.get('publish_time')
            jk_news_content = jk_news_i.get('content')
            if not jk_news_content:
                continue
            if len(jk_news_content) < 20: # 文本过短的new直接跳过
                continue
            is_content = re.findall('[\u4e00-\u9fa5]+', jk_news_content)
            # print('>>>>>>>>>>>>>>>',jk_news_id)
            if not is_content:
                continue
            data={'strtemp':jk_news_content}
            try:
                info = json.loads(requests.post('http://0.0.0.0:8881/txtsimilar', data).text)
                info = info.get('similar_infos') # 得到相似文章信息列表
            except:
                info = []
            # 限制相似文章个数6
            if len(info) == 0:
                continue
            compute_spread_value(app_id, jk_news_id, push_time, info) # app_id,自有文章id， 相似文章列表
        time.sleep(600)

            
            
    cursor.close()
    conn.close()
            
