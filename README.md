# 大数据采集与存储实训——指导文档与代码

[![GitHub stars](https://img.shields.io/github/stars/LTstrange/DailyMed_Summer_internship.svg)](https://github.com/LTstrange/DailyMed_Summer_internship/stargazers) [![GitHub forks](https://img.shields.io/github/forks/LTstrange/DailyMed_Summer_internship.svg)](https://github.com/LTstrange/DailyMed_Summer_internship/network) [![GitHub license](https://img.shields.io/github/license/LTstrange/DailyMed_Summer_internship.svg)](https://github.com/LTstrange/DailyMed_Summer_internship/blob/master/LICENSE)

## 仓库说明

本仓库保存了华中农业大学，数据科学与大数据技术专业，暑期实训的 DailyMed 项目的全部代码。

如果对程序或者文档，有任何疑问，均可在[issue](https://github.com/LTstrange/DailyMed_Summer_internship/issues)中提出。

本项目的实训指导书详见：[tutorial文档](#doc)。

DailyMed 是知名的药品说明书数据库，由美国国家药品图书馆管理，刊登最新的同时是精确的药物标签，给医疗行业从业者和大众提供药品知识服务。

注意：read_nds.py 和 web_crawler_4_ndcs.py 已经弃用。详细原因，请见tutorial文档最后部分。

## <span id="doc" >tutorial文档 </span>

### 实训内容引导 

#### 题目：DailyMed药物说明文档下载和分析

#### 第一部分：数据收集

打开[DailyMed](https://dailymed.nlm.nih.gov)网站，搜索条的下面是左半边的新闻和右半边的各种下载链接。

> 因网页更新及网站内容变化，网站页面可能与本文档描述有所出入。

在右半边中，找到APPLICATION DEVELOPMENT SUPPORT，也就是 应用开发支持。其中的[Web Services](https://dailymed.nlm.nih.gov/dailymed/app-support-web-services.cfm)，就是介绍如何通过访问对应的网址，找到你所需要的数据。

进入web services后，页面中给出了相关的说明，以及资源网址:https://dailymed.nlm.nih.gov/dailymed/services/

>注意：在爬取对应数据时，一定要在网址的后面，加上v2，例：https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml 。这一点在Web Sevices页面中，也进行了强调。

在网站给出的所有可用连接中，只有两个是我们需要的/spls 和 /spls/{SETID}。

> SPL是structured Product Labeling的缩写，意为***结构化产品标签***。也就是对应药品的说明书。SPL格式的内容，与在搜索框中直接搜索药品时所给出的说明书，是同一格式，以及排版

前面的 网页后缀：`/spls/`会给出网站所有药物的setID号码。在爬取完所有的setID后，就可以用每一个setID，替换`/spls/{SETID}`的对应位置，就可以下载该setID对应药物的说明书全文了。(详见代码)

> 还有NDC（国家药品验证号）也是药物号码。但是，首先是一个药物对应多个NDC号码，在后面处理起来十分麻烦。二是没有通过NDC直接下载药物说明的连接，只能从搜索框中搜索，也是处理起来十分麻烦。
>
> 而setID是与药品一一对应的，不仅可以从搜索框中搜索到对应的药物，还可以直接用连接下载对应药物的SPL。

##### step1：爬取setID

在连接到spls.xml的对应url时，网址不会直接给出全部的setID，而是会每次发送100个，分多次发送。其中，xml格式的文件会告诉你当前页url是什么，下一页url是什么。url是有规律的，都是在后面加上“page={}&pagesize=100”，只需要在大括号处（去除大括号）填入页码即可。

> 例：https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml?page=23&pagesize=100

通过程序得到了当前页的xml后，用正则表达式就能很轻松的获取到当前页的所有setID号。本样例的程序中，使用的正则表达式是`(?<=<setid>).*?(?=</setid>)`。

在爬取的过程中，由于对面的网络极其不稳定，所以一定要一步一存。防止网络连接丢失，导致前功尽弃。又因为对面的网络不稳定，下载速度也是极其的慢。需要开多进程或者多线程提高网络占用率。在爬取过程中，性能瓶颈并不是处理速度，而是网络速度（小于0.1Mbps）。多线程或者多进程都可以在同时开启多个下载连接，提高下载速度。

但是，开多进程时，有多个需要注意的点：首先是python没法开太多的进程，在进程数太多时，很容易引起程序崩溃。其次是在开启多进程时，由于网络不稳定，更可能导致连接丢失。同时多进程也会拉大保存周期，导致数据更易丢失。所以，在开进程时，需要根据当前网速，选择合适的进程数。再有就是多进程在处理完成任务后，会等待其他进程结束任务后，才能输出所有内容。就会导致在结束时，大量进程等待少数进程。所以每次开启多进程时，尽量一次派发更多的任务。

我在尝试过后，推荐在下载setID这种每个xml只包含少量内容的文件时，最好分批次（batch）下载。同时开5个进程，一次下载20页内容（也就是一次下载20*100=2000个setID）。每下载完一个batch后，进行一次数据的整理和保存。保存完毕后，再下载下一个batch。

如果程序正确，且下载顺利。所有下载下来的setID数量，应该与下载下来的xml文件中的<metadata>中的<total_elements>中的数字一样（也可能会少几个，且网站每天都会更新，数量会有变化）。

##### <span id="step2">Step2：爬取SPL </span>

在获取到所有的setID后，就可以下载每种药物的SPL了。同样，由于对面网络极差，所以一步一存是非常有必要的。但是，对于SPL这种，每次下载的xml本身，就是我们所需要的内容，信息的密度很大。而且每个xml只包含一个药物的说明书，很明显需要分开存储。所以，在下载SPL的时候，更适合每一个连接存储一次，也就是每下载一个SPL就存起来。

这样带来一个好处，因为存储方式的改变，我们不需要分batch下载了。而且因为每个文件之间的独立性很强，使得我们不需要按顺序来存放。也就是说，如果有一个连接失效了，我们可以直接放弃它，释放进程去下载下一个连接。只需要在全部完成后，再去重新下载丢失的连接。

在经过尝试后，可以直接开启60个进程（是我电脑能开启的最多进程数，再多就会引起崩溃）。每个进程执行的任务，都有独立的下载，超时断开和保存功能。其中，超时断开只要在urlopen函数中加上timeout。

> 我设置为10s，在实际使用中，要根据网速，保守的选择尽可能长的时间。在大多数连接顺利运行的同时，尽量提高整体的下载效率

并且套上try except抓取超时异常，以及各种其他异常，如*404 not found*，*ConnectionResetError*。针对对应的异常，可以选择不同的处理方式。

或者发生异常后，就直接释放进程。只要正常连接占据所有连接的大多数，就不会有太大影响。只要能在所有setID都尝试下载过一次以后，在对未能正常下载的setID重新下载即可。

#### 第二部分：数据分析

### 代码细节讲解

> 代码具体细节讲解可能与实际代码有所出入，一律以实际代码和代码注释为准

#### web_crawler_4_setIDs.py

##### 全局变量:

在程序中，首先定义了TOTAL_PAGE和BATCH两个全局变量。其中，TOTAL_PAGE需要在实际爬取数据的时候，更换为实际的 总页数。

> 因为网站内容变化，所以TOTAL_PAGE的大小可能会有所变化，需要以实际数目为准。
>
> 总页数可以通过浏览器打开爬取setID的url，并在<metadata>中找到。

之后定义了需要爬取的url，和匹配setID的两个正则表达式。分别用来提取网页中的setID和已经下载了的setID。

##### 主程序：

首先在本地查找已经下载的setID，并根据已经下载的setID修改本次下载的起始页码。（如果是首次下载，可忽略此代码段，start_ind=1）

```python
if __name__ == '__main__':
    print('starting..')
    all_setIDs, start_ind = get_all_setIDs("setIDs")
    # 修正start_ind
    start_ind += BATCH

    print('len of all_setIDs: {}'.format(len(all_setIDs)))
    print("already get previous data, start from {}".format(start_ind))
```

其次，程序从`start_ind`开始，一次下载`BATCH`个页面，一直下载到`TOTAL_PAGE`。并在建立集合`batch_setIDs = set()`，收集当前batch的所有setID数据。之后开启多进程，轮流下载当前batch内的对应页码的数据。在下载完整个batch以后，将数据存储在`batch_setIDs`中，并更新到`all_setIDs`中。

```python
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
```

之后，在每下载完成一个batch以后，将当前batch的数据存在本地的setIDs文件夹中，并以"setIDs_[开始页码].txt"命名。

```python
# 保存一个batch的setID数据
        print('saving...')
        with open('setIDs/setIDs_{}.txt'.format(start_page), 'w') as file:
            file.write(str(batch_setIDs))
```

##### 函数定义：

###### get_one_page(page_index):

该函数是为了实现多进程，而单独定义出来。该函数就是简单地将对应页码的数据爬取出来，并返回给主进程。

使用for循环，遍历爬取到的每一行内容，解码为utf8，并连接到`content`中。在用正则表达式，提取出所有的setID，返回给主进程。

```python
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
```

###### get_all_setIDs(address):

该函数实现了将本地对应目录的，已经下载了的setID依次找出，并存放在`all_setIDs`中。并根据已下载的内容，计算出本次下载的开始页码。

```python
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
```

***

#### read_setIDs.py

##### 全局变量：

在程序中，定义了用于在已下载文件中，匹配setID的正则表达式。

和用于收集数据的all_setIDs集合。

##### 主程序：

该程序实现了将`web_crawler_4_setIDs.py`下载的，由多个文件保存的setID，进行整理和整合。

程序首先根据给出的保存地址，将所有依batch保存的文件名读出。

再分别打开文件，读取文件内的内容，并用正则表达式找出保存的setIDs，保存在all_setIDs这个集合内。

读取完毕后，将all_setIDs这个集合，转换为字符，保存在当前目录下的setIDs.txt中。

```python
files = os.listdir('setIDs')

all_setIDs = set()

for file in files:
    with open('setIDs/' + file, 'r') as file:
        content = file.read()
        setIDs = re.findall(setID_comp, content)
        all_setIDs.update(setIDs)

print(len(all_setIDs))

with open('setIDs.txt', 'w') as file:
    file.write(str(all_setIDs))
```

***

#### web_crawler_4_spl_sample.py

##### 主程序：

该程序用于尝试下载一个特定的setID，所对应的的SPL说明书。并将该说明书，以"spl_sample.xml"为名，保存在本地。

```python
from urllib.request import urlopen

content = ""

print("linking to the url....")
for line in urlopen('https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/8c3c252c-d9eb-4bbd-b13a-271b87abdffb.xml'):
    line = line.decode('utf-8')
    print('downloading data...')
    content += line

print('already download the data.')
with open('spl_sample.xml', 'w', encoding='utf-8') as file:
    file.write(content)
```

***

#### web_crawler_4_spls.py

##### 全局变量：

在程序中，首先定义了用于匹配已下载的文件中，setID的正则表达式。并定义了需要爬取的页面的url。

##### 主程序：

由于主程序不需要分batch处理，所以整个程序结构相对比较简单。

首先是整理出所有需要下载的setID，也就是在`web_crawler_4_setIDs.py`中下载的，并交由`read_setIDs.py`整理汇总过，保存在setIDs.txt文件中的所有setIDs。

```python
# 整理所有需要下载的setID
print('getting setIDs...')
setIDs = []
with open('setIDs.txt', 'r') as file:
    content = file.read()
    setIDs = re.findall(setID_comp, content)
```

之后，再读取已下载的SPL文件（以setID命名）的列表，去除掉已经被下载的setIDs。

```python
# 排除掉已经下载过的setID
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
```

最后，开启多进程，本样例开启了60个进程，具体原因详见[Step2：爬取SPL](#step2)

```python
# 开启多线程，并下载对应SPL说明书
with Pool(60) as p:
    p.map(download_SPL, setIDs)
```

> 由于网站的内容会更新，所以有可能会使得setID失效。

##### 函数定义：

###### download_SPL(setID):

该函数是为了实现多进程，而单独定义出来。该函数实现了将SPL爬取下来，并保存在本地的功能。其中包括对于超时和连接出错的异常的处理。

```python
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
        print(traceback.format_exc().split('\n')[-2])
        return
    # 每下载完毕一个SPL说明书，就进行保存
    with open('spls/{setID}.xml'.format(setID=setID), 'w', encoding='utf-8') as file:
        file.write(content)
```

***

#### web_crawler_4_ndcs.py（已弃用）

因为ndc码无法有效直接的找到对应药品，故而弃用本程序。

##### 全局变量：

##### 主程序：

##### 函数定义：

***

#### read_ndcs.py（已弃用）

因为ndc码无法有效直接的找到对应药品，故而弃用本程序。

##### 全局变量：

##### 主程序：

##### 函数定义：