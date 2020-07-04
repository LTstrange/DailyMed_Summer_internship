# -*- coding: utf-8 -*-
# @Time    : 2020/4/17 15:33
# @Author  : LTstrange

from urllib.request import urlopen
from multiprocessing import Pool
import re
import os

# 设置全局变量，将TOTAL_PAGE改为当时的总页数。BATCH的大小根据网络状况自行调整
TOTAL_ELEM = 124834
TOTAL_PAGE = 1249
BATCH = 20

# 设置相对于文件的存储位置
file_dir = os.path.dirname(os.path.abspath(__file__))
storge_dir = file_dir + r'\setIDs'

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
    all_setIDs = set()
    # 检查是否是首次运行
    if not os.path.exists(storge_dir):
        print("'setIDs' 文件夹不存在. \n正在初始化，并建立文件夹....")
        # 根据程序文件地址建立存储文件夹
        os.mkdir(storge_dir)
        print('开始爬取setIDs..')
        start_ind = 1
    else:
        print('程序恢复中....')
        all_setIDs, start_ind = get_all_setIDs(storge_dir)
        # 修正start_ind
        start_ind += BATCH

        print('已下载的setIDs总数为: {}'.format(len(all_setIDs)))
        print("已经收集完已下载数据, 开始从 {} 页下载".format(start_ind))
    # 从start_ind开始下载
    for start_page in range(start_ind, TOTAL_PAGE, BATCH):
        # 对一个batch的数据进行收集和处理
        batch_setIDs = set()
        # 开始下载数据
        with Pool(5) as p:
            pool_result = p.map(get_one_page, [page_index for page_index in range(start_page, start_page + BATCH)])
            # 收集一个batch的数据
            print('\n收集线程池数据中....')
            for setIDs in pool_result:
                batch_setIDs.update(setIDs)
                all_setIDs.update(setIDs)
        print("已下载{:.2f}%, 共 {} 个setID".format((len(all_setIDs) / TOTAL_ELEM)*100, len(all_setIDs)))
        # 保存一个batch的setID数据
        print('保存该批次中...')
        with open(storge_dir+r'/setIDs_{}.txt'.format(start_page), 'w') as file:
            file.write(str(batch_setIDs))

    print("所有setID已经下载完毕。下一步，请运行'read_setIDs.py'文件。")



