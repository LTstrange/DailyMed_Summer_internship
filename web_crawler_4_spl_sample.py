# -*- coding: utf-8 -*-
# @Time    : 2020/5/11 19:54
# @Author  : LTstrange

from urllib.request import urlopen

content = ""
# 直接通过url获取数据，并保存在content里面。
print("linking to the url....")
for line in urlopen('https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/8c3c252c-d9eb-4bbd-b13a-271b87abdffb.xml'):
    line = line.decode('utf-8')
    print('downloading data...')
    content += line
# 将下载的数据，保存在spl_sample.xml文件中。
print('already download the data.')
with open('spl_sample.xml', 'w', encoding='utf-8') as file:
    file.write(content)





