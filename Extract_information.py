# -*- coding: utf-8 -*-
# @Time    : 2020/7/5 8:55
# @Author  : LTstrange

import os
from xml.dom.minidom import parse
import tqdm
from multiprocessing import Pool, Lock
import traceback
import json

# 确定文件的绝对位置
file_path = os.path.abspath(__file__)
file_dir = os.path.dirname(file_path)
SPL_dir = os.path.join(file_dir, 'spls')
json_dir = os.path.join(file_dir, 'jsons')

BATCH = 5000
spls = os.listdir(SPL_dir)


def trans_xml_2_json(ind):
    if ind > len(spls) - 1:
        return
    spl = spls[ind]
    if ind % (BATCH // 10) == 0:
        print(ind, end='; ')
    js_dict = {'setID': spl[:-4], }
    DOMtree = parse(os.path.join(SPL_dir, spl))
    document = DOMtree.documentElement
    if document.hasChildNodes():
        doc_childs = [each for each in document.childNodes if
                      each.nodeType == each.ELEMENT_NODE and each.tagName == 'component']
        if len(doc_childs) > 0:
            for component in doc_childs:
                sub_comps = [section for section in component.childNodes[1].childNodes if
                             section.nodeType == section.ELEMENT_NODE and section.tagName == 'component']
                if len(sub_comps) == 0:
                    break
                for ind, sub_comp in enumerate(sub_comps):
                    sections = [section for section in sub_comp.childNodes if
                                section.nodeType == section.ELEMENT_NODE]
                    childs = [child for child in sections[0].childNodes if
                              child.nodeType == child.ELEMENT_NODE]
                    sectionName = 'N/A'
                    for each in childs:
                        if each.tagName == 'code':
                            if sectionName == 'N/A':
                                sectionName = each.getAttribute('displayName')
                            else:
                                print("Duplicate SectionName")
                    sectionName.replace('.', '_')
                    text = ''
                    for paragraph in sub_comp.getElementsByTagName('paragraph'):
                        text += ''.join([text_node.data.strip() for text_node in paragraph.childNodes if
                                         text_node.nodeType == text_node.TEXT_NODE])
                        text += '\n'
                    js_dict[sectionName] = text
        else:
            print('empty document')
    else:
        print(spl[:-4])
        print(document)
        exit(-1)

    return js_dict


if __name__ == '__main__':
    count = 25
    for start_ind in range(120000, len(spls), BATCH):
        with Pool(5) as p:
            js_list = p.map(trans_xml_2_json, [ind for ind in range(start_ind, start_ind + BATCH)])

        js_str = json.dumps(js_list)
        print(f'\nsaving SPL{count}.json...')
        with open(os.path.join(json_dir, f'SPL{count}.json'), 'w', encoding='utf-8') as f:
            f.write(js_str)
            count += 1



