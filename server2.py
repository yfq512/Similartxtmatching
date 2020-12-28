import cv2, os, time, random, base64, shutil, fcntl, json
from flask import Flask,render_template,request
from winnowing import winnowing
from winnowing import comparison
from elasticsearch import Elasticsearch

def getRandomSet(bits):
    num_set = [chr(i) for i in range(48,58)]
    char_set = [chr(i) for i in range(97,123)]
    total_set = num_set + char_set
    value_set = "".join(random.sample(total_set, bits))
    return value_set

def load_txt(orgtxtroot):
    hash_str_list = []
    txtpath_list = []
    # orgtxt_list = os.listdir(orgtxtroot)
    for n_ in open(orgtxtroot):
        n = n_[:-1]
        # txtpath = os.path.join(orgtxtroot, n)
        txtpath = n.split(',')[0]
        try:
            txt_hash = winnowing(txtpath)
        except:
            print('read txt error: ', txtpath)
            txt_hash = None
        hash_str_list.append(txt_hash)
        txtpath_list.append(n.split(',')[1])
    return hash_str_list, txtpath_list

def uptxts(uptxtroot, orgtxtroot):
    _hash_strs = []
    _txtpaths = []
    uptxt_list = os.listdir(uptxtroot)
    for n in uptxt_list:
        txtpath = os.path.join(uptxtroot, n)
        copy_path = os.path.join(orgtxtroot, n)
        shutil.copyfile(txtpath, copy_path)
        os.remove(txtpath)
        try:
            txt_hash = winnowing(copy_path)
        except:
            print('read txt error:', copy_path)
            txt_hash = None
        _hash_strs.append(txt_hash)
        _txtpaths.append(copy_path)
        print('adding: ', copy_path)
    return _hash_strs, _txtpaths

def get_limit_txtpath(dsttxtpath, _hash_strs, _txtpaths):
    limit = 0.9
    t1 = time.time()
    dst_hash_str = winnowing(dsttxtpath)
    t2 = time.time()
    out_txtpaths = []
    scores = []
    print('len', len(_hash_strs))
    for n in range(len(_hash_strs)):
        t3 = time.time()
        temp_value = comparison(dst_hash_str, _hash_strs[n])
        #print('5444646464646464',temp_value)
        if temp_value > limit:
            id_title = _txtpaths[n]
            body = {
                "query":{
                    "term":{
                        "id.keyword":id_title
                    }
                }
            }
            info_org = es_db.search(index='fwnews',body=body)
            info_dst = info_org.get('hits').get('hits')[0].get('_source')
            
            title = info_dst.get('title')
            title_url = title_url = info_dst.get('source_url')
            similar_sorce = temp_value
            web_source = info_dst.get('source')
            scores.append(temp_value)
            out_txtpaths.append({'title':title, 'title_url':title_url, 'similar_sorce':similar_sorce, 'web_source':web_source})
    try:
        return out_txtpaths, max(scores)
    except:
        return out_txtpaths, 0

def get_list_index(org_list, new_list):
    nums_list = []
    cnt = 0
    for n in org_list:
        if n in new_list:
            nums_list.append(cnt)
        cnt = cnt + 1
    return nums_list

def deltxt_online(orgtxtroot, deltxtsign_path, hash_strs, txtpaths):
    deltxtlist = []
    for n in open(deltxtsign_path):
        deltxtlist.append(os.path.join(orgtxtroot,n[:-1]))
    numslist = get_list_index(txtpaths, deltxtlist)
    print('dasdsdasdasdsadasdsadsaasdas', txtpaths, numslist)
    for index_ in numslist:
        del hash_strs[index_]
        del txtpaths[index_]
    return hash_strs, txtpaths

orgtxtroot = 'orgtxts'
uptxtroot = 'uporgtxts'
uptxtsign_path = 'uptxtsign.txt'
deltxtsign_path = 'deltxt.txt'

# 链接es数据库
es_db = Elasticsearch(["192.168.132.152"], http_auth=('elastic', 'Ynwy778123'), port=9200)

app = Flask(__name__)
hash_strs, txtpaths = load_txt('txts_record.txt') # 初始化加载本地txt

@app.route("/txtsimilar",methods = ['GET', 'POST'])
def get_similar_txt():
    if request.method == "POST":
        if os.path.exists(uptxtsign_path):
            new_hash_strs, new_txtpaths = uptxts(uptxtroot, orgtxtroot)
            os.remove(uptxtsign_path)
            for n in new_hash_strs:
                hash_strs.append(n)
            for m in new_txtpaths:
                txtpaths.append(m)
        if os.path.exists(deltxtsign_path):
            deltxt_online(orgtxtroot, deltxtsign_path, hash_strs, txtpaths)
            os.remove(deltxtsign_path)

        #print(txtpaths)
        temp_txt_str = request.form.get('strtemp')
        rand_txt_name = getRandomSet(20) + '.txt'
        temp_txt_path = os.path.join(orgtxtroot, rand_txt_name)
        with open(temp_txt_path, 'w') as f:
            f.write(temp_txt_str)
            f.close()
        # 初始化返回值
        sign = -1
        similar_value_max = 0
        similar_infos = []
        
        try:
            similar_infos, similar_value_max = get_limit_txtpath(temp_txt_path, hash_strs, txtpaths)
            os.remove(temp_txt_path)
            if len(similar_infos) > 0: # 存在相似文本
                sign = 1
                return json.dumps({'sign':sign, 'similar_value_max':similar_value_max, 'similar_infos':similar_infos})
            else: # 不存在相似文本
                sign = 0
                return json.dumps({'sign':sign, 'similar_value_max':similar_value_max, 'similar_infos':similar_infos}) # orgtxtpaths similar up limit value
        except: # 运行出错，结果无效
            os.remove(temp_txt_path)
            return json.dumps({'sign':sign, 'similar_value_max':similar_value_max, 'similar_infos':similar_infos}) # len(str) is too short
    else:
        return "<h1>Get similar txt, please use post !</h1>"

@app.route("/updatatxts",methods = ['GET', 'POST'])
def updatatxts():
    if request.method == "POST":
        sign_Mandatory_add = request.form.get('sign_add')
        str_add = request.form.get('addstr')
        randname = getRandomSet(20) + '.txt'
        txtpath = os.path.join(uptxtroot ,randname)
        with open(txtpath, 'w') as f:
            f.write(str_add)
            f.close()
        sign = -1
        similar_value_max = 0
        similar_infos = []
        try:
            uplimit_txtpaths = get_limit_txtpath(txtpath, hash_strs, txtpaths)
            if len(uplimit_txtpaths) > 0: # 有信息
                pass   
            else: # 无信息
                pass
        except: # 执行出错
            os.remove(txtpath)
            return {'sign':sign, 'similar_value_max':similar_value_max, 'similar_infos':similar_infos} # len(str) is too short
    else:
        return "<h1>Updata txt, please use pust !</h1>"

@app.route("/deltxts",methods = ['GET', 'POST'])
def _deltxts():
    if request.method == "POST":
        delname = request.form.get('delname')
        orgtxt_list = os.listdir(orgtxtroot)
        if not delname in orgtxt_list:
            return {'sign':-1, 'text':orgtxt_list} # failed,return orgtxt_list
        with open(deltxtsign_path,'a') as f:
            fcntl.flock(f,fcntl.LOCK_EX)
            f.write(delname)
            f.write('\n')
            fcntl.flock(f,fcntl.LOCK_UN)
            f.close()
        os.remove(os.path.join(orgtxtroot, delname))
        return {'sign':0, 'text':None} # del scuess
    else:
        return "<h1>Delete txt, please use pust !</h1>"

@app.route("/com2txts",methods = ['GET', 'POST'])
def com2txts():
    if request.method == "POST":
        str_1 = request.form.get('str_1')
        str_2 = request.form.get('str_2')
        randname1 = getRandomSet(20) + '.txt'
        randname2 = getRandomSet(20) + '.txt'
        with open(randname1, 'w') as f:
            f.write(str_1)
            f.close()
        with open(randname2, 'w') as f:
            f.write(str_2)
            f.close()
        try:
            hash1 = winnowing(randname1)
            hash2 = winnowing(randname2)
            score = comparison(hash1, hash2)
            os.remove(randname1)
            os.remove(randname2)
            return {'sign':1, 'score':score}
        except:
            os.remove(randname1)
            os.remove(randname2)
            return {'sign':-1, 'score':None}
    else:
        return "<h1>compare txt, please use post !</h1>"


if __name__ == "__main__":
    host = '0.0.0.0'
    port = '8879'
    #app.run(debug=True, host=host, port=port)
    app.run(debug=True, host=host, port=port, threaded=True)
