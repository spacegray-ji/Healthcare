"""
[KNIH] Data download auto-ver 4.3.py
author: Geonwoo Ji
ver 4.3
Device type:  iOS

Acclerometer (mAcc) - 15Hz
Gyroscope (mGyr) - 15Hz
Pressure (mPre) - 1Hz
"""

# Import modules
from concurrent.futures import thread
from distutils import core
import os
import math
import time
import json
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import datetime
from datetime import timedelta


# Server address
IoT_1 = 'SERVER_IP'

# For pandas dataframe columns
columnsName1 = ['UT_mAcc', 'x1', 'y1', 'z1', 'x2', 'y2', 'z2', 'x3', 'y3', 'z3', 'x4', 'y4', 'z4', 'x5', 'y5', 'z5', 'x6', 'y6', 'z6', 'x7', 'y7', 'z7', 'x8', 'y8', 'z8', 'x9', 'y9', 'z9', 'x10', 'y10', 'z10', 'x11', 'y11', 'z11', 'x12', 'y12', 'z12', 'x13', 'y13', 'z13', 'x14', 'y14', 'z14', 'x15', 'y15', 'z15']
columnsName2 = ['UT_mGyr', 'x1', 'y1', 'z1', 'x2', 'y2', 'z2', 'x3', 'y3', 'z3', 'x4', 'y4', 'z4', 'x5', 'y5', 'z5', 'x6', 'y6', 'z6', 'x7', 'y7', 'z7', 'x8', 'y8', 'z8', 'x9', 'y9', 'z9', 'x10', 'y10', 'z10', 'x11', 'y11', 'z11', 'x12', 'y12', 'z12', 'x13', 'y13', 'z13', 'x14', 'y14', 'z14', 'x15', 'y15', 'z15']
columnsName3 = ['UT_mPre', 'val']

# Function - print users info
def print_users(userInfo_path):
    
    with open(userInfo_path) as f:
        users = f.read().splitlines()

    cnt1 = 0
    st_date = []
    usr_cnt = []
    usr_list = []


    for i in users:
        if int(i) > 1000:
            st_date.append(i)
            usr_cnt.append(cnt1)
            cnt1 = 0
        else:
            usr_list.append(i)
            cnt1 += 1

    usr_cnt.append(cnt1)
    for i, j in enumerate(st_date):
        print("Started on " + str(j) + ": " + str(usr_cnt[i+1]))
    
    print("Total: " + str(len(usr_list)))

# Function - get users dictionary
def getUsers(userInfo_path):
    
    with open(userInfo_path) as f:
        users = f.read().splitlines()

    st_date = 0
    usr_dict = {}

    for i in users:
        if int(i) > 1000:
            st_date = i
        else:
            usr_dict[str(i)] = st_date

    return usr_dict

# Function - Checking directory
def makedirs(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except OSError:
        print("Error: Failed to create the directory!")
        print(path)


# Function - download data and validation
def downloadData(aeName, date):
    makedirs("./data/err")
    makedirs("./data/" + aeName)
    f = open("./data/" + aeName + ".txt", 'a')
    f2 = open("./data/err/E" + aeName + ".txt", 'a')

    cra = datetime.datetime.strptime(date, "%Y%m%d")
    cra = cra - timedelta(1)
    cra = '&cra=' + cra.strftime("%Y%m%d") + 'T150000'
    crb = '&crb=' + date + 'T145959'

    ret_val = []

    payload={}
    headers = {
        'Accept': 'application/json',
        'X-M2M-RI': '12345',
        'X-M2M-Origin': 'ubicomp_super'
    }

    url1 = IoT_1 + "/Mobius/" + aeName + "/mobile/mAcc?fu=2&ty=4&rcn=4" + cra + crb
    url2 = IoT_1 + "/Mobius/" + aeName + "/mobile/mGyr?fu=2&ty=4&rcn=4" + cra + crb
    url3 = IoT_1 + "/Mobius/" + aeName + "/mobile/mPre?fu=2&ty=4&rcn=4" + cra + crb

    # mAcc download
    try:        
        data1 = json.loads(requests.request("GET", url1, headers=headers, data=payload).text)
        data1 = data1['m2m:rsp']['m2m:cin']
        mAcc_cin = len(data1)

        # mAcc data pre-processing
        mAcc_data = ""
        n_cnt = 0

        try:
            while data1:
                buf1 = data1.pop()
                if buf1['con'] != 'None':
                    mAcc_data = mAcc_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mAcc_data = mAcc_data.split(',')
            mAcc_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mAcc parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        mAcc_data = [v for v in mAcc_data if v]
        lst_mAcc = []

        try:
            for i in range(0, len(mAcc_data) - 1, 46):
                if int(float(mAcc_data[i])) > 1600000000:
                    ext_data = mAcc_data[i:i+46]
                    lst_mAcc.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mAcc format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_mAcc = pd.DataFrame(lst_mAcc, columns = columnsName1)
        df_mAcc = df_mAcc.astype('float')
        df_mAcc = df_mAcc.astype({'UT_mAcc':'int'})
        df_mAcc = df_mAcc.sort_values('UT_mAcc')
        df_mAcc = df_mAcc.reset_index(drop=True)

        ret_val.append((len(df_mAcc) - list(df_mAcc.duplicated(['UT_mAcc'])).count(True))/864)
        df_mAcc.to_csv("./data/" + aeName + "/" + aeName + "_mAcc_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mAcc download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # mGyr download
    try:
        data2 = json.loads(requests.request("GET", url2, headers=headers, data=payload).text)
        data2 = data2['m2m:rsp']['m2m:cin']
        mGyr_cin = len(data2)

        # mGyr data pre-processing
        mGyr_data = ""
        n_cnt = 0

        try:
            while data2:
                buf2 = data2.pop()
                if buf2['con'] != 'None':
                    mGyr_data = mGyr_data + buf2['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mGyr_data = mGyr_data.split(',')
            mGyr_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mGyr parsing')
            f2.write('  Time:' + date)
            f2.write('  User code:' + aeName)
            f2.write('  Error info:' + str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        mGyr_data = [v for v in mGyr_data if v]
        lst_mGyr = []

        try:
            for i in range(0, len(mGyr_data) - 1, 46):
                if int(float(mGyr_data[i])) > 1600000000:
                    ext_data = mGyr_data[i:i+46]
                    lst_mGyr.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mGyr format error')
            f2.write('  Time:' + date)
            f2.write('  User code:' + aeName)
            f2.write('  Error info:' + str(e))
            f2.write('\n')

        df_mGyr = pd.DataFrame(lst_mGyr, columns = columnsName2)
        df_mGyr = df_mGyr.astype('float')
        df_mGyr = df_mGyr.astype({'UT_mGyr':'int'})
        df_mGyr = df_mGyr.sort_values('UT_mGyr')
        df_mGyr = df_mGyr.reset_index(drop=True)

        ret_val.append((len(df_mGyr) - list(df_mGyr.duplicated(['UT_mGyr'])).count(True))/864)
        df_mGyr.to_csv("./data/" + aeName + "/" + aeName + "_mGyr_" + str(date) + ".csv")
    
    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mGyr')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')

    try:
        data3 = json.loads(requests.request("GET", url3, headers=headers, data=payload).text)
        data3 = data3['m2m:rsp']['m2m:cin']
        mPre_cin = len(data3)

        # mPre data pre-processing
        mPre_data = ""
        n_cnt = 0

        try:
            while data3:
                buf3 = data3.pop()
                if buf3['con'] != 'None':
                    mPre_data = mPre_data + buf3['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mPre_data = mPre_data.split(',')
            mPre_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mPre parsing')
            f2.write('  Time:' + date)
            f2.write('  User code:' + aeName)
            f2.write('  Error info:' + str(e))
            f2.write('\n')

        # print("Number of none CIN is ->", n_cnt)
        mPre_data = [v for v in mPre_data if v]
        lst_mPre = []

        try:
            for i in range(0, len(mPre_data) - 1, 2):
                if int(float(mPre_data[i])) > 1600000000:
                    ext_data = mPre_data[i:i+2]
                    lst_mPre.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mPre format error')
            f2.write('  Time:' + date)
            f2.write('  User code:' + aeName)
            f2.write('  Error info:' + str(e))
            f2.write('\n')

        df_mPre = pd.DataFrame(lst_mPre, columns = columnsName3)
        df_mPre = df_mPre.astype('float')
        df_mPre = df_mPre.astype({'UT_mPre':'int'})
        df_mPre = df_mPre.sort_values('UT_mPre')
        df_mPre = df_mPre.reset_index(drop=True)

        ret_val.append((len(df_mPre) - list(df_mPre.duplicated(['UT_mPre'])).count(True))/864)
        df_mPre.to_csv("./data/" + aeName + "/" + aeName + "_mPre_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mPre')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # print('mAcc data must have ->', mAcc_cin * 600)
    # print('Time interval: ', int(df_mAcc.loc[len(df_mAcc)-1][0] - df_mAcc.loc[0][0]))
    # print('Number of instance: ', len(df_mAcc))
    # print('Number of duplicated data: ', list(df_mAcc.duplicated(['UT_mAcc'])).count(True))
    # print('')
    # print('Number of missing instance: ', (int(df_mAcc.loc[len(df_mAcc)-1][0] - df_mAcc.loc[0][0])) - (len(df_mAcc) - list(df_mAcc.duplicated(['UT_mAcc'])).count(True)))
    # print((len(df_mAcc) - list(df_mAcc.duplicated(['UT_mAcc'])).count(True))/864)
    # print('')
    # print('')
    
    # print('mGyr data must have ->', mGyr_cin * 600)
    # print('Time interval: ', int(df_mGyr.loc[len(df_mGyr)-1][0] - df_mGyr.loc[0][0]))
    # print('Number of instance: ', len(df_mGyr))
    # print('Number of duplicated data: ', list(df_mGyr.duplicated(['UT_mGyr'])).count(True))
    # print('')
    # print('Number of missing instance: ', (int(df_mGyr.loc[len(df_mGyr)-1][0] - df_mGyr.loc[0][0])) - (len(df_mGyr) - list(df_mGyr.duplicated(['UT_mGyr'])).count(True)))
    # print((len(df_mGyr) - list(df_mGyr.duplicated(['UT_mGyr'])).count(True))/864)
    # print('')
    # print('')
    
    # print('mPre data must have ->', mPre_cin * 600)
    # print('Time interval: ', int(df_mPre.loc[len(df_mPre)-1][0] - df_mPre.loc[0][0]))
    # print('Number of instance: ', len(df_mPre))
    # print('Number of duplicated data: ', list(df_mPre.duplicated(['UT_mPre'])).count(True))
    # print('')
    # print('Number of missing instance: ', (int(df_mPre.loc[len(df_mPre)-1][0] - df_mPre.loc[0][0])) - (len(df_mPre) - list(df_mPre.duplicated(['UT_mPre'])).count(True)))
    # print((len(df_mPre) - list(df_mPre.duplicated(['UT_mPre'])).count(True))/864)

    try:
        if math.floor(min(ret_val)) >= 0:
            f.write(date + ' ')

        f.write(str(math.floor(min(ret_val))))
        f.write(' ' + str(ret_val))
        f.write('\n')
        f.close()
    except Exception as e:
        f2.write(date + ' ')
        f2.write('[Error report]: ret_val')
        f2.write(str(e))
        f2.write('\n')
        f2.close()

    return ret_val

def downloadFull(aeName, date_param_st):
    aeName = 'S' + str(aeName)
    value = []
    now = datetime.datetime.now()
    date_param_ed = now.strftime("%Y%m%d")
    st_time = datetime.datetime.strptime(date_param_st, "%Y%m%d")
    ed_time = datetime.datetime.strptime(date_param_ed, "%Y%m%d")

    while st_time != ed_time:
        try:
            value.append(math.floor(min(downloadData(aeName, datetime.datetime.strftime(st_time, "%Y%m%d")))))
            st_time = st_time + timedelta(1)
        except Exception as e:
            st_time = st_time + timedelta(1)
            print(str(e))

    print(aeName + ': ' + value)


if __name__ == '__main__':
    import concurrent.futures
    import multiprocessing

    if multiprocessing.cpu_count() < 3:
        cores = 1
    else:
        cores = (multiprocessing.cpu_count() - 2)
        cores = 5
        
    print("Start")
    prgm_st = time.time()

    userInfo_path = "./1.txt"
    users = getUsers(userInfo_path)
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=cores)

    threads = []

    try:
        for user in users.keys():
            threads.append(pool.submit(downloadFull, str(user), users[str(user)]))
    except:
        print('process error')
    
    concurrent.futures.wait(threads)

    prgm_ed = time.time()

    print("running time: %f sec" % (prgm_ed - prgm_st))

    
    