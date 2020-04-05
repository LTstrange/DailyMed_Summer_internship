import os
import traceback
import urllib.error
from urllib.request import urlopen
import re
from multiprocessing import Pool
import socket
import time

from tqdm import tqdm

setID_comp = re.compile(r'(?<=\')[a-zA-Z0-9\-]+?(?=\')')

url = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/{setID}.xml'


def download_SPL(setID):
    content = ''
    try:
        for line in urlopen(url.format(setID=setID), timeout=10):
            line = line.decode('utf-8')
            content += line
    # except urllib.error.HTTPError as e:
    #     print(e)
    #     return
    # except urllib.error.URLError as e:
    #     print('{setID} TIMEOUT!!!'.format(setID=setID))
    #     return
    # except socket.timeout as e:
    #     print('{setID} TIMEOUT!!!'.format(setID=setID))
    #     return
    # except ConnectionResetError as e:
    #     print(e)
    #     return
    except:
        print(traceback.format_exc().split('\n')[-2])
        return
    # print('saving {setID}...'.format(setID=setID))
    with open('spls/{setID}.xml'.format(setID=setID), 'w', encoding='utf-8') as file:
        file.write(content)


if __name__ == '__main__':
    print('getting setIDs...')
    setIDs = []
    with open('setIDs.txt', 'r') as file:
        content = file.read()
        setIDs = re.findall(setID_comp, content)

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

    with Pool(60) as p:
        p.map(download_SPL, setIDs)


