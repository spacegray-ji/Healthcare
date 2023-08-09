"""
[KNIH] Data download health-daily.py
author: Geonwoo Ji
ver 4.0
Device type:  iOS


[iPhone]
    - calories
    - distance
    - HR
    - sleep
    - steps

[Watch]
    - calories
    - distance
    - HR
    - sleep
    - steps

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
columnsName1 = ['UTs_mCal', 'UTe_mCal', 'device', 'val']
columnsName2 = ['UTs_mDis', 'UTe_mDis', 'device', 'val']
columnsName3 = ['UTs_mHR', 'UTe_mHR', 'device', 'val']
columnsName4 = ['UTs_mSle', 'UTe_mSle', 'device', 'val']
columnsName5 = ['UTs_mStp', 'UTe_mStp', 'device', 'val']

columnsName6 = ['UTs_wCal', 'UTe_wCal', 'device', 'val']
columnsName7 = ['UTs_wDis', 'UTe_wDis', 'device', 'val']
columnsName8 = ['UTs_wHR', 'UTe_wHR', 'device', 'val']
columnsName9 = ['UTs_wSle', 'UTe_wSle', 'device', 'val']
columnsName10 = ['UTs_wStp', 'UTe_wStp', 'device', 'val']

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

    st_date = '20221127'

    for i in users:
        if int(i) > 1000:
            pass
            # st_date = i
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


# Function - download data and validation
def downloadData(aeName, date):
    makedirs("../data/err")
    makedirs("../data/" + aeName)
    f = open("../data/" + aeName + ".txt", 'a')
    f2 = open("../data/err/E" + aeName + ".txt", 'a')

    cra = datetime.datetime.strptime(date, "%Y%m%d")
    cra = cra - timedelta(1)
    cra = '&cra=' + cra.strftime("%Y%m%d") + 'T150000'
    crb = '&crb=' + date + 'T145959'

    # cra = '&cra=20221128T150000'
    # crb = '&crb=20221129T145959'

    ret_val = []

    payload={}
    headers = {
        'Accept': 'application/json',
        'X-M2M-RI': '12345',
        'X-M2M-Origin': 'ubicomp_super'
    }

    url1 = IoT_1 + "/Mobius/" + aeName + "/health/iphone/calories?fu=2&ty=4&rcn=4" + cra + crb
    url2 = IoT_1 + "/Mobius/" + aeName + "/health/iphone/distance?fu=2&ty=4&rcn=4" + cra + crb
    url3 = IoT_1 + "/Mobius/" + aeName + "/health/iphone/HR?fu=2&ty=4&rcn=4" + cra + crb
    url4 = IoT_1 + "/Mobius/" + aeName + "/health/iphone/sleep?fu=2&ty=4&rcn=4" + cra + crb
    url5 = IoT_1 + "/Mobius/" + aeName + "/health/iphone/steps?fu=2&ty=4&rcn=4" + cra + crb
    url6 = IoT_1 + "/Mobius/" + aeName + "/health/watch/calories?fu=2&ty=4&rcn=4" + cra + crb
    url7 = IoT_1 + "/Mobius/" + aeName + "/health/watch/distance?fu=2&ty=4&rcn=4" + cra + crb
    url8 = IoT_1 + "/Mobius/" + aeName + "/health/watch/HR?fu=2&ty=4&rcn=4" + cra + crb
    url9 = IoT_1 + "/Mobius/" + aeName + "/health/watch/sleep?fu=2&ty=4&rcn=4" + cra + crb
    url10 = IoT_1 + "/Mobius/" + aeName + "/health/watch/steps?fu=2&ty=4&rcn=4" + cra + crb

    # health/iphone/calories data download
    try:        
        data1 = json.loads(requests.request("GET", url1, headers=headers, data=payload).text)
        data1 = data1['m2m:rsp']['m2m:cin']
        mCal_cin = len(data1)

        # data pre-processing
        mCal_data = ""
        n_cnt = 0

        try:
            while data1:
                buf1 = data1.pop()
                if buf1['con'] != 'None':
                    mCal_data = mCal_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mCal_data = mCal_data.split(',')
            mCal_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mCal parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        mCal_data = [v for v in mCal_data if v]
        lst_mCal = []

        try:
            for i in range(0, len(mCal_data) - 1, 4):
                if int(float(mCal_data[i])) > 1600000000:
                    ext_data = mCal_data[i:i+4]
                    lst_mCal.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mCal format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_mCal = pd.DataFrame(lst_mCal, columns = columnsName1)
        df_mCal = df_mCal.astype({'UTs_mCal':'int'})
        df_mCal = df_mCal.astype({'UTe_mCal':'int'})
        df_mCal = df_mCal.astype({'device':'str'})
        df_mCal = df_mCal.astype({'val':'float'})
        df_mCal = df_mCal.sort_values('UTs_mCal')
        df_mCal = df_mCal.reset_index(drop=True)

        ret_val.append((len(df_mCal) - list(df_mCal.duplicated(['UTs_mCal'])).count(True))/864)
        df_mCal.to_csv("../data/" + aeName + "/" + aeName + "_mCal_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mCal download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')

    # health/iphone/distance data download
    try:        
        data2 = json.loads(requests.request("GET", url2, headers=headers, data=payload).text)
        data2 = data2['m2m:rsp']['m2m:cin']
        mDis_cin = len(data2)

        # data pre-processing
        mDis_data = ""
        n_cnt = 0

        try:
            while data2:
                buf1 = data2.pop()
                if buf1['con'] != 'None':
                    mDis_data = mDis_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mDis_data = mDis_data.split(',')
            mDis_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mDis parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        mDis_data = [v for v in mDis_data if v]
        lst_mDis = []

        try:
            for i in range(0, len(mDis_data) - 1, 4):
                if int(float(mDis_data[i])) > 1600000000:
                    ext_data = mDis_data[i:i+4]
                    lst_mDis.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mDis format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_mDis = pd.DataFrame(lst_mDis, columns = columnsName2)
        df_mDis = df_mDis.astype({'UTs_mDis':'int'})
        df_mDis = df_mDis.astype({'UTe_mDis':'int'})
        df_mDis = df_mDis.astype({'device':'str'})
        df_mDis = df_mDis.astype({'val':'float'})
        df_mDis = df_mDis.sort_values('UTs_mDis')
        df_mDis = df_mDis.reset_index(drop=True)

        ret_val.append((len(df_mDis) - list(df_mDis.duplicated(['UTs_mDis'])).count(True))/864)
        df_mDis.to_csv("../data/" + aeName + "/" + aeName + "_mDis_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mDis download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/iphone/HR data download
    try:        
        data3 = json.loads(requests.request("GET", url3, headers=headers, data=payload).text)
        data3 = data3['m2m:rsp']['m2m:cin']
        mHR_cin = len(data3)

        # data pre-processing
        mHR_data = ""
        n_cnt = 0

        try:
            while data3:
                buf1 = data3.pop()
                if buf1['con'] != 'None':
                    mHR_data = mHR_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mHR_data = mHR_data.split(',')
            mHR_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mHR parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        mHR_data = [v for v in mHR_data if v]
        lst_mHR = []

        try:
            for i in range(0, len(mHR_data) - 1, 4):
                if int(float(mHR_data[i])) > 1600000000:
                    ext_data = mHR_data[i:i+4]
                    lst_mHR.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mHR format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_mHR = pd.DataFrame(lst_mHR, columns = columnsName3)
        df_mHR = df_mHR.astype({'UTs_mHR':'int'})
        df_mHR = df_mHR.astype({'UTe_mHR':'int'})
        df_mHR = df_mHR.astype({'device':'str'})
        df_mHR = df_mHR.astype({'val':'float'})
        df_mHR = df_mHR.sort_values('UTs_mHR')
        df_mHR = df_mHR.reset_index(drop=True)

        ret_val.append((len(df_mHR) - list(df_mHR.duplicated(['UTs_mHR'])).count(True))/864)
        df_mHR.to_csv("../data/" + aeName + "/" + aeName + "_mHR_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mHR download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/iphone/sleep data download
    try:        
        data4 = json.loads(requests.request("GET", url4, headers=headers, data=payload).text)
        data4 = data4['m2m:rsp']['m2m:cin']
        mSle_cin = len(data4)

        # data pre-processing
        mSle_data = ""
        n_cnt = 0

        try:
            while data4:
                buf1 = data4.pop()
                if buf1['con'] != 'None':
                    mSle_data = mSle_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mSle_data = mSle_data.split(',')
            mSle_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mSle parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        mSle_data = [v for v in mSle_data if v]
        lst_mSle = []

        try:
            for i in range(0, len(mSle_data) - 1, 4):
                if int(float(mSle_data[i])) > 1600000000:
                    ext_data = mSle_data[i:i+4]
                    lst_mSle.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mSle format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_mSle = pd.DataFrame(lst_mSle, columns = columnsName4)
        df_mSle = df_mSle.astype({'UTs_mSle':'int'})
        df_mSle = df_mSle.astype({'UTe_mSle':'int'})
        df_mSle = df_mSle.astype({'device':'str'})
        df_mSle = df_mSle.astype({'val':'float'})
        df_mSle = df_mSle.sort_values('UTs_mSle')
        df_mSle = df_mSle.reset_index(drop=True)

        ret_val.append((len(df_mSle) - list(df_mSle.duplicated(['UTs_mSle'])).count(True))/864)
        df_mSle.to_csv("../data/" + aeName + "/" + aeName + "_mSle_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mSle download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/iphone/steps data download
    try:        
        data5 = json.loads(requests.request("GET", url5, headers=headers, data=payload).text)
        data5 = data5['m2m:rsp']['m2m:cin']
        mStp_cin = len(data5)

        # data pre-processing
        mStp_data = ""
        n_cnt = 0

        try:
            while data5:
                buf1 = data5.pop()
                if buf1['con'] != 'None':
                    mStp_data = mStp_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            mStp_data = mStp_data.split(',')
            mStp_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mStp parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        mStp_data = [v for v in mStp_data if v]
        lst_mStp = []

        try:
            for i in range(0, len(mStp_data) - 1, 4):
                if int(float(mStp_data[i])) > 1600000000:
                    ext_data = mStp_data[i:i+4]
                    lst_mStp.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: mStp format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_mStp = pd.DataFrame(lst_mStp, columns = columnsName5)
        df_mStp = df_mStp.astype({'UTs_mStp':'int'})
        df_mStp = df_mStp.astype({'UTe_mStp':'int'})
        df_mStp = df_mStp.astype({'device':'str'})
        df_mStp = df_mStp.astype({'val':'float'})
        df_mStp = df_mStp.sort_values('UTs_mStp')
        df_mStp = df_mStp.reset_index(drop=True)

        ret_val.append((len(df_mStp) - list(df_mStp.duplicated(['UTs_mStp'])).count(True))/864)
        df_mStp.to_csv("../data/" + aeName + "/" + aeName + "_mStp_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: mStp download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/watch/calories data download
    try:        
        data6 = json.loads(requests.request("GET", url6, headers=headers, data=payload).text)
        data6 = data6['m2m:rsp']['m2m:cin']
        wCal_cin = len(data6)

        # data pre-processing
        wCal_data = ""
        n_cnt = 0

        try:
            while data6:
                buf1 = data6.pop()
                if buf1['con'] != 'None':
                    wCal_data = wCal_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            wCal_data = wCal_data.split(',')
            wCal_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wCal parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        wCal_data = [v for v in wCal_data if v]
        lst_wCal = []

        try:
            for i in range(0, len(wCal_data) - 1, 4):
                if int(float(wCal_data[i])) > 1600000000:
                    ext_data = wCal_data[i:i+4]
                    lst_wCal.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wCal format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_wCal = pd.DataFrame(lst_wCal, columns = columnsName6)
        df_wCal = df_wCal.astype({'UTs_wCal':'int'})
        df_wCal = df_wCal.astype({'UTe_wCal':'int'})
        df_wCal = df_wCal.astype({'device':'str'})
        df_wCal = df_wCal.astype({'val':'float'})
        df_wCal = df_wCal.sort_values('UTs_wCal')
        df_wCal = df_wCal.reset_index(drop=True)

        ret_val.append((len(df_wCal) - list(df_wCal.duplicated(['UTs_wCal'])).count(True))/864)
        df_wCal.to_csv("../data/" + aeName + "/" + aeName + "_wCal_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: wCal download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/watch/Distance data download
    try:        
        data7 = json.loads(requests.request("GET", url7, headers=headers, data=payload).text)
        data7 = data7['m2m:rsp']['m2m:cin']
        wDis_cin = len(data7)

        # data pre-processing
        wDis_data = ""
        n_cnt = 0

        try:
            while data7:
                buf1 = data7.pop()
                if buf1['con'] != 'None':
                    wDis_data = wDis_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            wDis_data = wDis_data.split(',')
            wDis_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wDis parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        wDis_data = [v for v in wDis_data if v]
        lst_wDis = []

        try:
            for i in range(0, len(wDis_data) - 1, 4):
                if int(float(wDis_data[i])) > 1600000000:
                    ext_data = wDis_data[i:i+4]
                    lst_wDis.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wDis format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_wDis = pd.DataFrame(lst_wDis, columns = columnsName7)
        df_wDis = df_wDis.astype({'UTs_wDis':'int'})
        df_wDis = df_wDis.astype({'UTe_wDis':'int'})
        df_wDis = df_wDis.astype({'device':'str'})
        df_wDis = df_wDis.astype({'val':'float'})
        df_wDis = df_wDis.sort_values('UTs_wDis')
        df_wDis = df_wDis.reset_index(drop=True)

        ret_val.append((len(df_wDis) - list(df_wDis.duplicated(['UTs_wDis'])).count(True))/864)
        df_wDis.to_csv("../data/" + aeName + "/" + aeName + "_wDis_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: wDis download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/watch/HR data download
    try:        
        data8 = json.loads(requests.request("GET", url8, headers=headers, data=payload).text)
        data8 = data8['m2m:rsp']['m2m:cin']
        wHR_cin = len(data8)

        # data pre-processing
        wHR_data = ""
        n_cnt = 0

        try:
            while data8:
                buf1 = data8.pop()
                if buf1['con'] != 'None':
                    wHR_data = wHR_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            wHR_data = wHR_data.split(',')
            wHR_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wHR parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        wHR_data = [v for v in wHR_data if v]
        lst_wHR = []

        try:
            for i in range(0, len(wHR_data) - 1, 4):
                if int(float(wHR_data[i])) > 1600000000:
                    ext_data = wHR_data[i:i+4]
                    lst_wHR.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wHR format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_wHR = pd.DataFrame(lst_wHR, columns = columnsName8)
        df_wHR = df_wHR.astype({'UTs_wHR':'int'})
        df_wHR = df_wHR.astype({'UTe_wHR':'int'})
        df_wHR = df_wHR.astype({'device':'str'})
        df_wHR = df_wHR.astype({'val':'float'})
        df_wHR = df_wHR.sort_values('UTs_wHR')
        df_wHR = df_wHR.reset_index(drop=True)

        ret_val.append((len(df_wHR) - list(df_wHR.duplicated(['UTs_wHR'])).count(True))/864)
        df_wHR.to_csv("../data/" + aeName + "/" + aeName + "_wHR_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: wHR download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/watch/sleep data download
    try:        
        data9 = json.loads(requests.request("GET", url9, headers=headers, data=payload).text)
        data9 = data9['m2m:rsp']['m2m:cin']
        wSle_cin = len(data9)

        # data pre-processing
        wSle_data = ""
        n_cnt = 0

        try:
            while data9:
                buf1 = data9.pop()
                if buf1['con'] != 'None':
                    wSle_data = wSle_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            wSle_data = wSle_data.split(',')
            wSle_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wSle parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        wSle_data = [v for v in wSle_data if v]
        lst_wSle = []

        try:
            for i in range(0, len(wSle_data) - 1, 4):
                if int(float(wSle_data[i])) > 1600000000:
                    ext_data = wSle_data[i:i+4]
                    lst_wSle.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wSle format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_wSle = pd.DataFrame(lst_wSle, columns = columnsName9)
        df_wSle = df_wSle.astype({'UTs_wSle':'int'})
        df_wSle = df_wSle.astype({'UTe_wSle':'int'})
        df_wSle = df_wSle.astype({'device':'str'})
        df_wSle = df_wSle.astype({'val':'float'})
        df_wSle = df_wSle.sort_values('UTs_wSle')
        df_wSle = df_wSle.reset_index(drop=True)

        ret_val.append((len(df_wSle) - list(df_wSle.duplicated(['UTs_wSle'])).count(True))/864)
        df_wSle.to_csv("../data/" + aeName + "/" + aeName + "_wSle_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: wSle download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')


    # health/watch/steps data download
    try:        
        data10 = json.loads(requests.request("GET", url10, headers=headers, data=payload).text)
        data10 = data10['m2m:rsp']['m2m:cin']
        wStp_cin = len(data10)

        # data pre-processing
        wStp_data = ""
        n_cnt = 0

        try:
            while data10:
                buf1 = data10.pop()
                if buf1['con'] != 'None':
                    wStp_data = wStp_data + buf1['con'] + ','
                else:
                    n_cnt = n_cnt + 1

            wStp_data = wStp_data.split(',')
            wStp_data.pop()
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wStp parsing')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: '+ str(e))
            f2.write('\n')
            
        # print("Number of none CIN is ->", n_cnt)
        wStp_data = [v for v in wStp_data if v]
        lst_wStp = []

        try:
            for i in range(0, len(wStp_data) - 1, 4):
                if int(float(wStp_data[i])) > 1600000000:
                    ext_data = wStp_data[i:i+4]
                    lst_wStp.append(ext_data)
        except Exception as e:
            f2.write(date + ' ')
            f2.write('\n[Error report]: wStp format error')
            f2.write('  Time: ' + date)
            f2.write('  User code: ' + aeName)
            f2.write('  Error info: ' + str(e))
            f2.write('\n')

        df_wStp = pd.DataFrame(lst_wStp, columns = columnsName10)
        df_wStp = df_wStp.astype({'UTs_wStp':'int'})
        df_wStp = df_wStp.astype({'UTe_wStp':'int'})
        df_wStp = df_wStp.astype({'device':'str'})
        df_wStp = df_wStp.astype({'val':'float'})
        df_wStp = df_wStp.sort_values('UTs_wStp')
        df_wStp = df_wStp.reset_index(drop=True)

        ret_val.append((len(df_wStp) - list(df_wStp.duplicated(['UTs_wStp'])).count(True))/864)
        df_wStp.to_csv("../data/" + aeName + "/" + aeName + "_wStp_" + str(date) + ".csv")

    except Exception as e:
        f2.write(date + ' ')
        f2.write('\n[Error report]: wStp download')
        f2.write('  Time:' + date)
        f2.write('  User code:' + aeName)
        f2.write('  Error info:' + str(e))
        f2.write('\n')

############################################################################################################
############################################################################################################

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
        cores = (multiprocessing.cpu_count() - 1)
    print("Start")
    prgm_st = time.time()

    userInfo_path = "./fin.txt"
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

    
    