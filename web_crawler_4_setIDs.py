from urllib.request import urlopen
from multiprocessing import Pool
import re
import os

TOTAL_PAGE = 1143
BATCH = 20

url = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml'
setID_comp = re.compile(r"(?<=<setid>).*?(?=</setid>)")
file_setID_comp = re.compile(r'(?<=\')[a-zA-Z0-9\-]+?(?=\')')


def get_all_setIDs(address):
    all_setIDs = set()
    start_ind = 1 - BATCH
    files = os.listdir(address)
    for each in files:
        if int(each[7:-4]) > start_ind:
            start_ind = int(each[7:-4])
        with open(address + '/' + each, 'r') as file:
            content = file.read()
            content = re.findall(file_setID_comp, content)
            all_setIDs.update(content)

    return all_setIDs, start_ind


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
    start_ind += BATCH
    print('len of all_setIDs: {}'.format(len(all_setIDs)))
    print("already get previous data, start from {}".format(start_ind))
    for start_page in range(start_ind, TOTAL_PAGE, BATCH):  # TOTAL_PAGE = 1142
        batch_setIDs = set()
        with Pool(5) as p:
            pool_result = p.map(get_one_page, [page_index for page_index in range(start_page, start_page + BATCH)])
            print('\ncollecting pool result....')
            for setIDs in pool_result:
                batch_setIDs.update(setIDs)
                all_setIDs.update(setIDs)
        print("{:.2f}%, num of setID:{}".format((len(all_setIDs) / 114249)*100, len(all_setIDs)))
        print('saving...')
        with open('setIDs/setIDs_{}.txt'.format(start_page), 'w') as file:
            file.write(str(batch_setIDs))



