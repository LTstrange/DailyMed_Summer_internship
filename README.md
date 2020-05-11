# 大数据采集与存储实训——指导文档与代码

## 仓库说明

本仓库保存了华中农业大学，数据科学与大数据技术专业，暑期实训的 DailyMed 项目的全部代码。

本项目的实训指导书详见：[tutorial文档](#jump)。

## <span id="jump" >tutorial文档 </span>

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

如果程序正确，且下载顺利。所有下载下来的setID数量，应该与xml文件中的<metadata>中的<total_elements>中的数字一样（也可能会少几个，且网站每天都会更新，数量会有变化）。

##### Step2：爬取SPL

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

在程序中，首先定义了TOTAL_PAGE和BATCH两个全局变量。其中，TOTAL_PAGE需要在实际爬取数据的时候，更换为实际的 总页数。总页数可以通过浏览器打开爬取setID的url，并在<metadata>中找到。

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

其次，程序从`start_ind`开始，一次下载`BATCH`个页面，一直下载到`TOTAL_PAGE`。并在建立集合`batch_setIDs = set()`，收集当前batch的所有setID数据。之后开启多线程，轮流下载当前batch内的对应页码的数据。在下载完整个batch以后，将数据存储在`batch_setIDs`中，并更新到`all_setIDs`中。

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

该函数是为了实现多线程，而单独定义出来。该函数就是简单地将对应页码的数据爬取出来，并返回给主进程。

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

#### web_crawler_4_spls.py

#### web_crawler_4_spl_sample.py

#### web_crawler_4_ndcs.py
