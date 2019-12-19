import sys
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd


def post_tast2(s, p1, p2):
    if s[p1] == '' or s[p1] == ' ' or s[p1] == '\n' or pd.isna(s[p1]) or s[p2] == '' or s[p2] == ' ' or s[
        p2] == '\n' or pd.isna(s[p2]):
        return -1, (p1, p2)
    headers = {'content-type': "application/json"}
    url = "http://10.8.8.17:11002/cacu_sim"
    payload = {}
    payload['text1'] = s[p1]
    payload['text2'] = s[p2]

    while True:
        try:
            response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
            if response.status_code != 200:
                print([p1, p2], response, response.text, flush=True)
            x = json.loads(response.text)
            break
        except json.JSONDecodeError:
            print("payload is: ", payload, "500 has happened, please wait......", flush=True)
    return float(x['sim_value']), (p1, p2)


def run(sentences):
    saver = np.zeros((len(sentences), len(sentences)))
    print("ThreadPoolExecutor will be starting...", flush=True)
    with ThreadPoolExecutor() as executor:
        res = []
        cnt = 0
        for idx in idx_list:  # 第一个for循环，记录多进程的每个executor执行所得的结果
            task = executor.submit(post_tast2, sentences, idx[0], idx[1])  # 子任务提交（非阻塞，这时候submit的任务还没执行）
            res.append(task)  # 子任务入库（其实入库的是个future对象，该对象在不久的将来会产生res）
        for future in as_completed(res):  # 第二个for循环，使用as_completed()函数，对于”库“里task.done()==True的future对象进行下一步操作
            data = future.result()  # 用.result()方法取task的结果
            idx, task_res = data[1], data[0]
            saver[idx] = task_res

            cnt += 1
            if cnt % 100 == 0:
                print("========saver当前记录在案的总槽位: %s , 其中saver共有槽位67528个========" % np.sum(saver != 0))

            print(idx, ":", task_res, flush=True)
    return saver


if __name__ == "__main__":
    col = sys.argv[1]
    title = pd.read_csv('/Users/shen-pc/Desktop/WORK/NLU/interface/df_try_interface.csv')[col].tolist()
    print("============already begin!!!============", flush=True)
    idx_list = []
    for i in range(len(title)):
        for j in range(i + 1, len(title), 1):
            idx_list.append((i, j))

    matrix = pd.DataFrame(run(title))
    matrix.to_csv(col + "_res.csv", index=False)
