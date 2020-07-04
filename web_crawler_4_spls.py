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

# 设置本文件的绝对地址，防止文件地址与运行地址不统一的问题
file_dir = os.path.dirname(os.path.abspath(__file__))
file_path = file_dir+r'\setIDs.txt'
storge_dir = file_dir + r'\spls'

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
        print(traceback.format_exc().split('\n')[-2], end=' ->>> 未成功的setID为:')
        print(setID)
        return
    # 每下载完毕一个SPL说明书，就进行保存
    with open(storge_dir+r'\{setID}.xml'.format(setID=setID), 'w', encoding='utf-8') as file:
        file.write(content)


if __name__ == '__main__':
    if not os.path.exists(file_path):
        print("请先运行 'read_setIDs.py' 文件。再运行此程序。")
        exit()

    # 整理所有需要下载的setID
    print('整理所有需要下载的setID中...')
    setIDs = []
    with open(file_path, 'r') as file:
        content = file.read()
        setIDs = re.findall(setID_comp, content)
    # 检查是否为首次运行，并完成初始化
    if not os.path.exists(storge_dir):
        print("'spls' 文件夹不存在。\n正在初始化，并建立文件夹....")
        os.mkdir(storge_dir)
    # 排除掉已经下载过的setID
    files = os.listdir(storge_dir)
    print('已下载的SPL文件个数:', len(files))
    print(f'共有{len(setIDs)}个setID。')
    for ind, SPL_file in tqdm(enumerate(files), total=len(files)):
        ID = SPL_file[:-4]
        try:
            setIDs.remove(ID)
        except ValueError:
            print('{ID}在所有setIDs.txt中不存在'.format(ID=ID))
            exit()
    print(f'还需要下载{len(setIDs)}个SPL')
    print("开始下载SPL文件....")

    # 开启多线程，并下载对应SPL说明书
    with Pool(60) as p:
        p.map(download_SPL, setIDs)


