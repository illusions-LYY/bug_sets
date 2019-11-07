import pandas as pd
import requests
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np


def post_tast2(s1, s2):
    headers = {'content-type': "application/json"}
    url = "http://10.8.8.17:11002/cacu_sim"
    payload = {}
    payload['text1'] = s1
    payload['text2'] = s2

    while True:
        try:
            response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
            print(response)
            if response.status_code != 200:
                print(response, '\n')
            x = json.loads(response.text)
            print("process %s !" % os.getpid())
            break
        except json.JSONDecodeError:
            print("500 has happened, please wait......")
    return float(x['sim_value'])


def run(sentences):
    saver = np.zeros((len(title), len(title)))
    print("ThreadPoolExecutor will be starting...")
    with ThreadPoolExecutor() as executor:
        res = []
        for idx in idx_list:  # 第一个for循环，记录多进程的每个executor执行所得的结果
            task = executor.submit(post_tast2, sentences[idx[0]], sentences[idx[1]])  # 子任务提交（非阻塞，这时候submit的任务还没执行）
            res.append([task, idx])  # 子任务入库（其实入库的是个future对象，该对象在不久的将来会产生res）
        for future in as_completed(res):  # 第二个for循环，使用as_completed()函数，对于”库“里task.done()==True的future对象进行下一步操作
            data = future.result()  # 用.result()方法取task的结果
            idx, task_res = data[1], data[0]
            saver[idx] = task_res
            print(idx, ":", task_res)
    return saver


if __name__ == "__main__":
    title = pd.read_csv('/Users/shen-pc/Desktop/WORK/NLU/interface/df_try_interface.csv')['title'].tolist()
    print("============already begin!!!============")
    idx_list = []
    for i in range(len(title)):
        for j in range(i + 1, len(title), 1):
            idx_list.append((i, j))

    matrix = pd.DataFrame(run(title))
    matrix.to_csv('./title_res.csv', index=False)
