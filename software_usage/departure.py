# -*- coding:utf-8 -*-
# Author: illusion_Shen
# Time  : 2019-12-10 18:47


import numpy as np
import happybase
import pymongo
import json
import time

from thriftpy2.transport.socket import TTransportException

"""
数据请求逻辑：
    1. dkt前序计算完毕，发出request并提供"uid","correct","semesterId","publisherId"信息；
    2. 使用"uid","semesterId","publisherId"组成query，去缓存表(线上环境使用hbase，调试阶段暂用MongoDB)中查找是否存在对应记录：
        2.1 若存在，则读取该记录中的"last_n_dkt"作为history，读取hbase缓存表"dkt_result"获取当前最近一条dkt输出作为current;(为何要启用新的缓存表？因为从hbase取出的数据还要进行预处理，减少数据预处理耗时)
        2.2 若不存在，仅读取hbase缓存表"dkt_result"的最近window+1条记录，第一条作为current，其余作为history.
    3. 获取得到数据后，进行计算，再将计算结果存入缓存表中(实际上是给下游计算模块)
"""


def fetch_data_from_hbase(row_key, num):
    conn = happybase.Connection('118.31.49.42', 9099)
    tbl = conn.table('dkt_result')

    while True:  # 防止从hbase请求数据的过程中连接中断
        try:
            tmp = []
            for k, v in tbl.scan(row_start=row_key + '_00000000000000', row_stop=row_key + '_99999999999999',
                                 limit=num):
                tmp.append(eval(v[b'info:result'])['goalScore'])
            break
        except BrokenPipeError as e1:
            conn = happybase.Connection('118.31.49.42', 9099)
            tbl = conn.table('dkt_result')
            print(e1, "hbase connecting......")
        except TTransportException as e2:
            conn = happybase.Connection('118.31.49.42', 9099)
            tbl = conn.table('dkt_result')
            print(e2, "hbase connecting......")
    return tmp


def dicts_to_matrix(arr):
    """
    传入的arr是list of dicts，dict的key即goal_id,value即对应goal_id的dkt输出。本函数将传入数组arr的每个元素都转成numpy.ndarray的一列。且保证goal_id顺序不变
    :param arr: hbase数据
    :return:
    """
    cnt = len(arr)
    saver = np.zeros((len(arr[0]), cnt))
    for item, num in zip(arr, range(cnt)):
        d, sort_li = {}, []
        [d.update(i) for i in item]
        if num == 0:
            sort_keys = sorted(d.keys())
        [sort_li.append(d[i]) for i in sort_keys]
        saver[:, num] = sort_li
    return saver, sort_keys


def get_data(row_key, window, my_cache):
    # 先看看MongoDB缓存中有没有这个用户，如果没有，再去hbase去找
    if not my_cache.count_documents({"user_id": row_key}):  # 在mongo中没找到这个user,去hbase取window+1条数据
        print("This is a new user")
        hbase_data = fetch_data_from_hbase(row_key, window + 1)
        # 将list嵌套dict的格式转成DataFrame
        if len(hbase_data) == window + 1:
            matrix, goal_id_list = dicts_to_matrix(hbase_data)
            dkt_curr, last_n_dkt = matrix[:, 0], matrix[:, 1:]
        else:
            print("hbase['dkt_result']中做题记录共有%s条，少于%s条" % (len(hbase_data), window), "暂时没有做任何改变")  # todo:处理一下冷启动问题?
            dkt_curr, goal_id_list = dicts_to_matrix(fetch_data_from_hbase(row_key, 1))

            return dkt_curr.reshape((-1, 1)), None, goal_id_list

    else:  # 在mongo中找到了，取hbase取最近的1条数据
        print("There is a log in my cache")
        dkt_curr, goal_id_list = dicts_to_matrix(fetch_data_from_hbase(row_key, 1))
        last_n_dkt = np.array(
            eval(my_cache.find({"user_id": row_key}).next()['matrix']))  # read json then convert to numpy.ndarray

    return dkt_curr.reshape((-1, 1)), last_n_dkt, goal_id_list


def departure_shooter(dkt_curr, last_n_dkt, correct):
    # dkt_curr, last_n_dkt = dkt_curr.values, last_n_dkt.values
    correct = (correct - 0.5) * 2
    dkt_last = last_n_dkt[:, 0].reshape((-1, 1))
    departure = (dkt_curr - dkt_last) * correct
    mask = (departure < 0).astype(int).reshape((-1, 1))
    diff = (last_n_dkt[:, :-1] - last_n_dkt[:, 1:]) * mask

    baseline = np.min([abs(departure), abs(dkt_last), abs(dkt_last - 1)], axis=0)
    dist_weight = [4, 3, 2, 1]
    alpha = np.sum(np.clip(diff * correct, a_max=1, a_min=0) * dist_weight, axis=1) / 10
    beta = 1 - (np.exp(np.clip(alpha / 0.9, a_max=1, a_min=0)) - 1) / (np.e - 1)
    dkt_modify = np.clip(beta.reshape((-1, 1)), a_max=1, a_min=0.01) * baseline * correct + dkt_last
    return dkt_modify


def save_to_mongo(res, my_cache, key):
    condition = {"user_id": key}
    user_log = my_cache.find_one(condition)
    if user_log is None:
        condition["matrix"] = json.dumps(res.tolist())
        my_cache.insert_one(condition)
    else:
        user_log["matrix"] = json.dumps(res.tolist())
        my_cache.update_one(condition, {'$set': user_log})
    return 0


if __name__ == "__main__":
    t0 = time.time()
    # 连接缓存表
    my_client = pymongo.MongoClient("10.8.8.71")
    my_coll = my_client["shenfei"]["evaluation_departure"]

    """
        if receive a POST request, then do get_data()
    """
    # test_new_user_time = ['5d137550780f6b0f46a0ed59', '5a75919b63fdf1146f5ce041', '5d1a1eb71f8c79135eac1421',
    #                       '5b62e1514b189856f717dfbc', '5a46228a52561642ce153a95',
    #                       '5c3b49c5571435070b49df5a', '5d1a203295e4e51fb10afbd4', '5d1a106ba784d512f832b48f',
    #                       '5b6c1c0342145d06696a4516', '5d1a196321f33b12e91c9a22']

    request_info = {"uid": "5d1a203295e4e51fb10afbd4", "semesterId": '17', "publisherId": '1',
                    "correct": 1}  # these are what we got from POST, please pay attention to the type of data
    user_id, semester_id, publisher_id, correct = request_info["uid"], request_info["semesterId"], request_info[
        "publisherId"], request_info["correct"]
    row_key = user_id + "_" + semester_id + "_" + publisher_id
    print(row_key)
    curr, last_n, goal_ids = get_data(row_key=row_key, window=5, my_cache=my_coll)  # 注意，`goal_ids`是二级目标次序
    if last_n is not None:
        dkt_mdf = departure_shooter(curr, last_n, correct)
        res = np.concatenate((dkt_mdf.reshape((-1, 1)), last_n), axis=1)


        save_to_mongo(res[:, :-1], my_coll, row_key)
        print("we have saved a log with uid = `%s` in %s\n" % (row_key, my_coll))
    else:
        # 原样输出`curr`
        pass
    t1 = time.time()
    print("耗时%s秒" % round(t1 - t0, 4))
