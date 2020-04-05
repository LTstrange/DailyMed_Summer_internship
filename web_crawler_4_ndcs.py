import time
from urllib.request import urlopen
import re
from multiprocessing import Pool

TOTAL_PAGE = 2965

url_comp = re.compile(r"(?<=<next_page_url>).*?(?=</next_page_url>)")
ndc_comp = re.compile(r"(?<=<ndc>).*?(?=</ndc>)")
current_page_comp = re.compile(r"(?<=<current_page>).*?(?=</current_page>)")

ndc_all = []


def run(start_page):
    print(start_page)
    ndc_all = []
    for i in range(start_page, start_page+50):
        current_url = "https://dailymed.nlm.nih.gov/dailymed/services/v2/ndcs.xml" + "?page={}&pagesize=100".format(i)
        for line in urlopen(current_url):
            line = line.decode('utf-8')
        ndcs = re.findall(ndc_comp, line)

        ndc_all.extend(ndcs)
        # print(len(ndc_all))

        next_url = re.findall(url_comp, line)
        url = next_url[0]
        if url == 'null':
            break
    with open('ndcs{}.txt'.format(start_page), 'w') as file:
        file.write(str(ndc_all))


if __name__ == '__main__':
    with Pool(10) as p:
        p.map(run, [i for i in range(2601, 2701, 50)])
    print("finish")



