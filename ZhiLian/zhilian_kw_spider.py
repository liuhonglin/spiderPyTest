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
#import pymongo
#import zhilian_kw_config
import time
from itertools import product

TOTAL_PAGE_NUMBER = 1; #PAGE_NUMBER: total number of pages，可进行修改

KEYWORDS = ["spark", "python", "java", "阿里", "腾讯", "京东", "爱奇艺"] #需爬取的关键字可以自己添加或修改

ADDRESS = ["北京", "上海", "深圳"]

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
                print('保存记录：', item)

if __name__ == '__main__':
    start = time.time()
    number_list = list(range(TOTAL_PAGE_NUMBER))
    args = product(ADDRESS, number_list)
    pool = Pool()
    pool.map(main, args) # 多进程运行
    end = time.time()
    print("Fininshed, task runs %s seconds." % (end - start))