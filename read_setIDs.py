# -*- coding: utf-8 -*-
# @Time    : 2020/5/11 19:54
# @Author  : LTstrange

import re
import os

# 用于匹配文件内setID的正则表达式
setID_comp = re.compile(r'(?<=\')[a-zA-Z0-9\-]+?(?=\')')

# 遍历给定目录下的所有文件名
files = os.listdir('setIDs')

all_setIDs = set()

# 遍历给定的文件夹，汇总出已经下载的setIDs
for file in files:
    with open('setIDs/' + file, 'r') as file:
        content = file.read()
        setIDs = re.findall(setID_comp, content)
        all_setIDs.update(setIDs)

print(len(all_setIDs))

# 创建setIDs.txt文件并保存
with open('setIDs.txt', 'w') as file:
    file.write(str(all_setIDs))







