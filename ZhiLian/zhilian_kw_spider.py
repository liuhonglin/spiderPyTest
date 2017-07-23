#
#-*-coding: utf-8 -*-
#_author: ""

"""
根据关键字、城市、以及页面编号生成需要爬取的网页链接
用requests获取相应的网页内容
用BeautifulSoup解析，获取需要的关键信息
将爬取的信息存入MongoDB数据库中，插入新记录或更新已有记录
用multiprocessing启动多进程进行爬取，提高运行效率
"""

from datetime import datetime
from urllib.parse import urlencode
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import pymongo
#import zhilian_kw_config
import time
from itertools import product

TOTAL_PAGE_NUMBER = 1; #PAGE_NUMBER: total number of pages，可进行修改

KEYWORDS = ["spark", "python", "java", "阿里", "腾讯", "京东", "爱奇艺"] #需爬取的关键字可以自己添加或修改

ADDRESS = ["北京", "上海", "深圳"]

MONGO_URI = "127.0.0.1"
MONGO_PORT = "27017"
MONGO_DB = "spider"

client = pymongo.MongoClient(MONGO_URI, int(MONGO_PORT))
db = client[MONGO_DB]
db.authenticate("spider", "spider@lhl")

# 设置代理服务器
proxies= {
          'http:':'http://121.232.146.184',
          'https:':'https://144.255.48.197'
         }

def download(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0'}
    response = requests.get(url, headers = headers, proxies = proxies)
    return response.text

def get_content(html):
    # 记录保存日期
    date = datetime.now().date()
    date = datetime.strftime(date, '%Y-%m-%d')

    soup = BeautifulSoup(html, 'lxml') # BeautifulSoup(html, 'html.parser')
    body = soup.body
    data_main = body.find("div", {"class":"newlist_list_content"})

    if data_main:
        tables = data_main.find_all('table')

        for i,table_info in enumerate(tables):
            if i == 0:
                continue
            tds = table_info.find("tr").find_all("td")
            zwmc = tds[0].find("a").get_text() # 职位名称
            zw_link = tds[0].find('a').get('href') # 职位连接
            fkl = tds[1].find('span').get_text() # 反馈率
            gsmc = tds[2].find('a').get_text() # 公司名称
            zwyx = tds[3].get_text() # 职位月薪
            gzdd = tds[4].get_text() # 工作地点
            gbsj = tds[5].find("span").get_text() # 发布日期

            tr_brief = table_info.find('tr', {'class':'newlist_tr_detail'})
            # 招聘简介
            brief = tr_brief.find('li', {'class':'newlist_deatil_last'}).get_text()

            # 用生成器获取信息
            """
                yield 的作用就是把一个函数变成一个 generator，带有 yield 的函数不再是一个普通函数，Python 解释器会将其视为一个 generator，
                调用 get_content(html) 不会执行 get_content 函数，而是返回一个 iterable 对象！
                在 for 循环执行时，每次循环都会执行 get_content 函数内部的代码，执行到 yield b 时，fab 函数就返回一个迭代值，
                下次迭代时，代码从 yield b 的下一条语句继续执行，而函数的本地变量看起来和上次中断执行前是完全一样的，于是函数继续执行，直到再次遇到 yield。
                
                当函数执行结束时，generator 自动抛出 StopIteration 异常，表示迭代完成。在 for 循环里，无需处理 StopIteration 异常，循环会正常结束。
                
                yield 的好处是显而易见的，把一个函数改写为一个 generator 就获得了迭代能力，比起用类的实例保存状态来计算下一个 next() 的值，不仅代码简洁，而且执行流程异常清晰。
            """
            yield {'zwmc': zwmc,
                'fkl': fkl,
                'gsmc': gsmc,
                'zwyx': zwyx,
                'gzdd': gzdd,
                'gbsj': gbsj,
                'brief': brief,
                'zw_link': zw_link,
                'save_date': date # 记录信息保存的日期
            }

def main(args):
    basic_url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?'

    table = db["zhilian"]

    for keyword in KEYWORDS:

        paras = {'jl':args[0],
                 'kw': keyword,
                 'p': args[1] # 第几页
            }
        url = basic_url + urlencode(paras)
        print(url)
        html = download(url)
        #print(html)
        if html:
            data = get_content(html)
            for item in data:
                if table.update({"zw_link":item['zw_link']}, {'$set':item}, True):
                    print('已保存记录：', item)

if __name__ == '__main__':
    start = time.time()
    number_list = list(range(TOTAL_PAGE_NUMBER))
    args = product(ADDRESS, number_list) # 笛卡儿集
    pool = Pool()
    pool.map(main, args) # 多进程运行
    end = time.time()
    print("Fininshed, task runs %s seconds." % (end - start))