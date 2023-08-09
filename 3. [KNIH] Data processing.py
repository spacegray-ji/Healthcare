"""
Data processing.py
author: Geonwoo Ji
ver 1.0
"""

def makedirs(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except OSError:
        print("Error: Failed to create the directory!")

def get_file_list(user_code, src_path):
    file_path = src_path + 'S' + str(user_code) + '/*.csv'
    file_list = [f for f in glob.glob(file_path)]

    mAcc_lst = []
    mGyr_lst = []
    mPre_lst = []

    for i in file_list:
        if i[35:39] == 'mAcc':
            mAcc_lst.append(i)
        elif i[35:39] == 'mGyr':
            mGyr_lst.append(i)
        elif i[35:39] == 'mPre':
            mPre_lst.append(i)
        else:
            print('Index Error')
            print(i)

    return mAcc_lst, mGyr_lst, mPre_lst

def combile_mAcc(user_code, mAcc_lst, trg_path):
    mAcc_columns = ['UT_mAcc', 'x1', 'y1', 'z1', 'x2', 'y2', 'z2', 'x3', 'y3', 'z3', 'x4', 'y4', 'z4', 'x5', 'y5', 'z5', 'x6', 'y6', 'z6', 'x7', 'y7', 'z7', 'x8', 'y8', 'z8', 'x9', 'y9', 'z9', 'x10', 'y10', 'z10', 'x11', 'y11', 'z11', 'x12', 'y12', 'z12', 'x13', 'y13', 'z13', 'x14', 'y14', 'z14', 'x15', 'y15', 'z15']
    df_mAcc = pd.DataFrame(columns = mAcc_columns)
    
    for buf in mAcc_lst:
        df_buf = pd.read_csv(buf, header=None, skiprows=1, names=mAcc_columns)
        df_mAcc = pd.concat([df_mAcc, df_buf])
    
    df_mAcc = df_mAcc.astype({'UT_mAcc':'int'})
    df_mAcc = df_mAcc.sort_values(by='UT_mAcc')
    df_mAcc = df_mAcc.set_index('UT_mAcc',drop=True)
    df_mAcc.to_csv(trg_path + str(user_code) + "/S" + str(user_code) + "_mAcc_" + datetime.strftime(datetime.fromtimestamp(df_mAcc.index[0]), '%Y%m%d') + "_" + datetime.strftime(datetime.fromtimestamp(df_mAcc.index[-1]), '%Y%m%d') + ".csv")
    

def combile_mGyr(user_code, mGyr_lst, trg_path):
    mGyr_columns = ['UT_mGyr', 'x1', 'y1', 'z1', 'x2', 'y2', 'z2', 'x3', 'y3', 'z3', 'x4', 'y4', 'z4', 'x5', 'y5', 'z5', 'x6', 'y6', 'z6', 'x7', 'y7', 'z7', 'x8', 'y8', 'z8', 'x9', 'y9', 'z9', 'x10', 'y10', 'z10', 'x11', 'y11', 'z11', 'x12', 'y12', 'z12', 'x13', 'y13', 'z13', 'x14', 'y14', 'z14', 'x15', 'y15', 'z15']
    df_mGyr = pd.DataFrame(columns = mGyr_columns)
    
    for buf in mGyr_lst:
        df_buf = pd.read_csv(buf, header=None, skiprows=1, names=mGyr_columns)
        df_mGyr = pd.concat([df_mGyr, df_buf])
    
    df_mGyr = df_mGyr.astype({'UT_mGyr':'int'})
    df_mGyr = df_mGyr.sort_values(by='UT_mGyr')
    df_mGyr = df_mGyr.set_index('UT_mGyr',drop=True)
    df_mGyr.to_csv(trg_path + str(user_code) + "/S" + str(user_code) + "_mGyr_" + datetime.strftime(datetime.fromtimestamp(df_mGyr.index[0]), '%Y%m%d') + "_" + datetime.strftime(datetime.fromtimestamp(df_mGyr.index[-1]), '%Y%m%d') + ".csv")


def combile_mPre(user_code, mPre_lst, trg_path):
    mPre_columns = ['UT_mPre', 'val']
    df_mPre = pd.DataFrame(columns = mPre_columns)
    
    for buf in mPre_lst:
        df_buf = pd.read_csv(buf, header=None, skiprows=1, names=mPre_columns)
        df_mPre = pd.concat([df_mPre, df_buf])
    
    df_mPre = df_mPre.astype({'UT_mPre':'int'})
    df_mPre = df_mPre.sort_values(by='UT_mPre')
    df_mPre = df_mPre.set_index('UT_mPre',drop=True)
    df_mPre.to_csv(trg_path + str(user_code) + "/S" + str(user_code) + "_mPre_" + datetime.strftime(datetime.fromtimestamp(df_mPre.index[0]), '%Y%m%d') + "_" + datetime.strftime(datetime.fromtimestamp(df_mPre.index[-1]), '%Y%m%d') + ".csv")


def combine_files(user_code, src_path, trg_path):

    path_lst = get_file_list(user_code, src_path)
    mAcc_lst = path_lst[0]
    mGyr_lst = path_lst[1]
    mPre_lst = path_lst[2]

    combile_mAcc(user_code, mAcc_lst, trg_path)
    combile_mGyr(user_code, mGyr_lst, trg_path)
    combile_mPre(user_code, mPre_lst, trg_path)

def get_fin_user(fin_txt):
    fp = fin_txt

    user_code = []

    with open(fp) as f:
        fin_user = f.read().split('\n')
        for i in fin_user:
            user_code.append(i)
    
    return user_code

def main_merge():
    for user_code in get_fin_user(fin_txt):
        makedirs(trg_path + str(user_code) + '/')
        combine_files(str(user_code), src_path, trg_path)


if __name__ == '__main__':
    import os
    import glob
    import time
    import pandas as pd
    from datetime import datetime

    import multiprocessing
    import concurrent.futures

    # src_path = '../../data/'
    src_path = '../Data downloading/data/'
    trg_path = '../Data downloading/NAS/'
    fin_txt = './fin.txt'
    # fin_txt = '../../healthwear-admin/admin/fin-1130.txt'

    makedirs(src_path)
    makedirs(trg_path)

    if multiprocessing.cpu_count() < 3:
        cores = 1
    elif multiprocessing.cpu_count() < 10:
        cores = 1
    else:
        cores = 1
    
    # cores = (multiprocessing.cpu_count() - 2)
    
    
    print("Start")
    prgm_st = time.time()

    main_merge()

    prgm_ed = time.time()


    print("running time: %f sec" % (prgm_ed - prgm_st))