# -*- coding: utf-8 -*-
# @Time    : 2020/5/11 19:54
# @Author  : LTstrange

import re
import os

# 用于匹配文件内setID的正则表达式
setID_comp = re.compile(r'(?<=\')[a-zA-Z0-9\-]+?(?=\')')

# 设置相对于文件的存储位置
file_dir = os.path.dirname(os.path.abspath(__file__))
storge_dir = file_dir + r'\setIDs'

if not os.path.exists(file_dir+r'\setIDs'):
    print("Please run 'web_crawler_4_setIDs.py' first. Then run this program.")
    exit()

# 遍历给定目录下的所有文件名
files = os.listdir(storge_dir)

all_setIDs = set()

# 遍历给定的文件夹，汇总出已经下载的setIDs
for file in files:
    with open(os.path.join(storge_dir, file), 'r') as file:
        content = file.read()
        setIDs = re.findall(setID_comp, content)
        all_setIDs.update(setIDs)

print(f'Sorted out {len(all_setIDs)} setIDs')

# 创建setIDs.txt文件并保存
with open(file_dir+r'\setIDs.txt', 'w') as file:
    file.write(str(all_setIDs))







