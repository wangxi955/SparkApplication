import csv
import time
import os
import requests
from lxml import  etree
from hdfs import *
client = Client("http://s198hadoop:9870")
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
}
commodity=[]
for i in range(1,25,2):
    url = "https://search.jd.com/Search?keyword=t%E6%81%A4%E5%A5%B3&suggest=5.def.0.SAK7%7CMIXTAG_SAK7R%2CSAK7_M_AM_L5379%2CSAK7_M_COL_L19737%2CSAK7_S_AM_R%2CSAK7_SC_PD_R%2CSAK7_SM_PB_LC%2CSAK7_SS_PM_R%2Ctsabtest_base64_U2VhcmNobGlzdF80MzkyfGJhc2U_tsabtest%7C&wq=t%E6%81%A4%E5%A5%B3&pvid=dfab9f1170a542fea8d6636af24c010c&page=" + str(
        i)//i用于翻页操作
    res = requests.get(url, headers=headers)
    text = res.text
selector = etree.HTML(text)
//list数组存储着第i页的li标签，对应每页的各个商品
    list = selector.xpath('//*[@id="J_goodsList"]/ul/li')
    for i in list:
        title = i.xpath('.//div[@class="p-name p-name-type-2"]/a/em/text()')[1]
        price = i.xpath('.//div[@class="p-price"]/strong/i/text()')[0]
        sku = i.xpath('.//div[@class="p-price"]/strong/i/@data-price')[0]

        dic ={
            'sku':sku,
            '商品名称':title,
            '价格':price
        }
        commodity.append(dic)
        with open('/home/hadoop/streaming/'+sku+'.csv','w',encoding='utf-8-sig',newline='')as f:
            w = csv.DictWriter(f,commodity[0].keys())
            # w.writeheader()
            w.writerows(commodity)
        time.sleep(5)#每获取一条商品数据，休息5秒，实时读取。
        f.close()
        client.upload('/user/hadoop/lindefu/structuredstreaming', r'/home/hadoop/streaming/' + sku + '.csv')#存入hdfs
        commodity.clear()#清空数组，方便下次存储
