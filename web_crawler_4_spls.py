# -*- coding: utf-8 -*-
# @Time    : 2020/4/17 15:09
# @Author  : LTstrange

import os
import traceback
from urllib.request import urlopen
import re
from multiprocessing import Pool

from tqdm import tqdm

# 用来匹配setID的正则表达式
setID_comp = re.compile(r'(?<=\')[a-zA-Z0-9\-]+?(?=\')')

# 下载setID对应的说明书（spl）的网址
url = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/{setID}.xml'


# 根据setID下载SPL说明书
def download_SPL(setID):
    content = ''
    # 尝试连接，并设置为超过10秒就断开连接
    try:
        for line in urlopen(url.format(setID=setID), timeout=10):
            line = line.decode('utf-8')
            content += line
    # 只要出错，就暂时放弃这个连接，等待下次手动重启时，再重新下载
    except:
        print(traceback.format_exc().split('\n')[-2])
        return
    # 每下载完毕一个SPL说明书，就进行保存
    with open('spls/{setID}.xml'.format(setID=setID), 'w', encoding='utf-8') as file:
        file.write(content)


if __name__ == '__main__':
    # 整理所有需要下载的setID
    print('getting setIDs...')
    setIDs = []
    with open('setIDs.txt', 'r') as file:
        content = file.read()
        setIDs = re.findall(setID_comp, content)

    # 排除掉已经下载过的setID
    files = os.listdir('spls/')
    print('files len:', len(files))
    print('setIDs len:(before)', len(setIDs))
    for ind, SPL_file in tqdm(enumerate(files), total=len(files)):
        ID = SPL_file[:-4]
        try:
            setIDs.remove(ID)
        except ValueError:
            print('{ID} not in setIDs'.format(ID=ID))
            exit()
    print('setIDs len:(after)', len(setIDs))

    # 开启多线程，并下载对应SPL说明书
    with Pool(60) as p:
        p.map(download_SPL, setIDs)


