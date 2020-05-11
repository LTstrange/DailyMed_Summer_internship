# -*- coding: utf-8 -*-
# @Time    : 2020/4/17 15:33
# @Author  : LTstrange

from urllib.request import urlopen
from multiprocessing import Pool
import re
import os

# 设置全局变量，将TOTAL_PAGE改为当时的总页数。BATCH的大小根据网络状况自行调整
TOTAL_PAGE = 1143
BATCH = 20

# 下载所有setID的网址链接
url = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml'
# 匹配setID的正则表达式
setID_comp = re.compile(r"(?<=<setid>).*?(?=</setid>)")
# 匹配已下载文档中setID的正则表达式
file_setID_comp = re.compile(r'(?<=\')[a-zA-Z0-9\-]+?(?=\')')


# 获取本地文档中，已经下载的setID
def get_all_setIDs(address):
    all_setIDs = set()
    # start_ind：本次下载应从start_ind对应的页码开始下载
    start_ind = 1 - BATCH
    # 找到所有存储setID的文件
    files = os.listdir(address)
    for each in files:
        # 更新start_ind
        if int(each[7:-4]) > start_ind:
            start_ind = int(each[7:-4])
        # 打开文档，并找出所有已下载的setID
        with open(address + '/' + each, 'r') as file:
            content = file.read()
            content = re.findall(file_setID_comp, content)
            all_setIDs.update(content)

    return all_setIDs, start_ind


# 下载并提取，对应的页码（page_index）的setID
def get_one_page(page_index):
    print(page_index, end='; ')
    content = ''
    for line in urlopen(
            'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml?page={}&pagesize=100'.format(page_index)):
        line = line.decode('utf-8')
        content += line
    setIDs = re.findall(setID_comp, content)
    return setIDs


if __name__ == '__main__':
    print('starting..')
    all_setIDs, start_ind = get_all_setIDs("setIDs")
    # 修正start_ind
    start_ind += BATCH

    print('len of all_setIDs: {}'.format(len(all_setIDs)))
    print("already get previous data, start from {}".format(start_ind))
    # 从start_ind开始下载
    for start_page in range(start_ind, TOTAL_PAGE, BATCH):  # TOTAL_PAGE = 1142
        # 对一个batch的数据进行收集和处理
        batch_setIDs = set()
        # 开始下载数据
        with Pool(5) as p:
            pool_result = p.map(get_one_page, [page_index for page_index in range(start_page, start_page + BATCH)])
            # 收集一个batch的数据
            print('\ncollecting pool result....')
            for setIDs in pool_result:
                batch_setIDs.update(setIDs)
                all_setIDs.update(setIDs)
        print("{:.2f}%, num of setID:{}".format((len(all_setIDs) / 114249)*100, len(all_setIDs)))
        # 保存一个batch的setID数据
        print('saving...')
        with open('setIDs/setIDs_{}.txt'.format(start_page), 'w') as file:
            file.write(str(batch_setIDs))



