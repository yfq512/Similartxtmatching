import cv2, os, time, random, base64, shutil
from flask import Flask,render_template,request
from winnowing import winnowing
from winnowing import comparison

def getRandomSet(bits):
    num_set = [chr(i) for i in range(48,58)]
    char_set = [chr(i) for i in range(97,123)]
    total_set = num_set + char_set
    value_set = "".join(random.sample(total_set, bits))
    return value_set

def load_txt(orgtxtroot):
    hash_str_list = []
    txtpath_list = []
    orgtxt_list = os.listdir(orgtxtroot)
    for n in orgtxt_list:
        txtpath = os.path.join(orgtxtroot, n)
        try:
            txt_hash = winnowing(txtpath)
        except:
            print('read txt error: ', txtpath)
            txt_hash = None
        hash_str_list.append(txt_hash)
        txtpath_list.append(txtpath)
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
    dst_hash_str = winnowing(dsttxtpath)
    out_txtpaths = []
    for n in range(len(_hash_strs)):
        temp_value = comparison(dst_hash_str, _hash_strs[n])
        print('5444646464646464',temp_value)
        if temp_value > limit:
            out_txtpaths.append(_txtpaths[n])
    return out_txtpaths

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

app = Flask(__name__)
hash_strs, txtpaths = load_txt(orgtxtroot)

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

        print(txtpaths)
        temp_txt_str = request.form.get('strtemp')
        rand_txt_name = getRandomSet(20) + '.txt'
        temp_txt_path = os.path.join(orgtxtroot, rand_txt_name)
        with open(temp_txt_path, 'w') as f:
            f.write(temp_txt_str)
            f.close()
        try:
            uplimit_txtpaths = get_limit_txtpath(temp_txt_path, hash_strs, txtpaths)
            os.remove(temp_txt_path)
            if len(uplimit_txtpaths) == 0:
                return {'sign':0, 'text':None} # notxt similar this txt by limit value
            return {'sign':1, 'text':uplimit_txtpaths} # orgtxtpaths similar up limit value
        except:
            os.remove(temp_txt_path)
            return {'sign':-2, 'text':None} # len(str) is too short
    else:
        return "<h1>Get similar txt, please use post !</h1>"

@app.route("/updatatxts",methods = ['GET', 'POST'])
def updatatxts():
    if request.method == "POST":
        str_add = request.form.get('addstr')
        randname = getRandomSet(20) + '.txt'
        txtpath = os.path.join(uptxtroot ,randname)
        with open(txtpath, 'w') as f:
            f.write(str_add)
            f.close()
        try:
            uplimit_txtpaths = get_limit_txtpath(txtpath, hash_strs, txtpaths)
            if not len(uplimit_txtpaths) == 0:
                os.remove(txtpath)
                return {'sign':-1, 'savename':uplimit_txtpaths} # add txt similar up limit ,return similar up orgtxtpath_list

            if not os.path.exists(uptxtsign_path):
                with open(uptxtsign_path,'w') as f:
                    f.write('sign')
                    f.close()
            return {'sign':1, 'savename':randname} # add scuessful, return name in orgdata
        except:
            os.remove(txtpath)
            return {'sign':-2, 'savename':None} # len(str) is too short
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
            f.write(delname)
            f.write('\n')
            f.close()
        os.remove(os.path.join(orgtxtroot, delname))
        return {'sign':0, 'text':None} # del scuess
    else:
        return "<h1>Delete txt, please use pust !</h1>"

if __name__ == "__main__":
    host = '0.0.0.0'
    port = '8879'
    #app.run(debug=True, host=host, port=port)
    app.run(debug=True, host=host, port=port, threaded=True)
