import requests
from bs4 import BeautifulSoup
import jieba
import time
import pymysql
import threading

def getTxt(url, value, forbidden_urls):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': f'http://tianqi.2345.com/wea_history/{value}.htm',
    }
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except requests.exceptions.HTTPError as err:
        if r.status_code == 403:
            print(f"403 Forbidden: {url}")
            forbidden_urls.append(url)
        raise err  # 对于其他状态码重新抛出异常

def getInfo(txt):
    soup = BeautifulSoup(txt, 'html.parser')
    divs = soup.find_all('tr')
    text1 = divs[1].text #数据为空会报错
    list1 = text1.split()
    return list1
        
    
def Info(list1):
    del list1[-1]
    del list1[-1]
    d = ""
    for i in range(len(list1)):
        a = list1[i].replace(r"<\/td>\n", "")
        a = a.encode('utf-8').decode("unicode_escape")
        d += "," + a
    list2 = d.split(",<\/tr>")
    for i in range(len(list2)):
        list2[i] = list2[i].replace("<\/span><\/td>", "")
        list2[i] = list2[i].replace("\n,\n,", "")
        list2[i] = list2[i].replace(",\n,", "")
    return list2

def insert_into_db(data_list, db_config, table):
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # 创建表格（如果不存在）
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            日期 VARCHAR(50) PRIMARY KEY,
            星期 VARCHAR(50),
            最高温 VARCHAR(50),
            最低温 VARCHAR(50),
            天气 VARCHAR(50),
            风向 VARCHAR(50),
            空气质量指数 VARCHAR(50),
            空气质量 VARCHAR(50)
        )
        """)

        # 插入数据
        for i in data_list:
            data = i.split(",")
            if len(data) == 8:  # 确保数据长度正确
                data2 = [data[0],data[1],data[2],data[3],data[4],jieba.lcut(data[5])[0],data[6],data[7]]
            elif len(data)<8 and data[-1]=="-":
                data2 = [data[0],data[1],data[2],data[3],data[4],jieba.lcut(data[5])[0],None,None]
            else:
                data2=data
            cursor.execute(f"""
            INSERT INTO {table} (日期, 星期, 最高温, 最低温, 天气, 风向, 空气质量指数, 空气质量)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, data2)

        # 提交事务
        conn.commit()

    except pymysql.MySQLError as err:
        print(f"Error: {err}")
    finally:
        if conn:
            cursor.close()
            conn.close()

class MyThread(threading.Thread):
    def __init__(self, region, area_id, db_config, forbidden_urls):
        threading.Thread.__init__(self)
        self.region = region
        self.area_id = area_id
        self.db_config = db_config
        self.forbidden_urls = forbidden_urls

    def run(self):
        list3 = []
        firstlist = ["日期", "星期", "最高温", "最低温", "天气", "风力风向", "空气质量指数", "空气质量"]
        for year in range(2016,2025):
            for month in range(1,13):
                url = f"http://tianqi.2345.com/Pc/GetHistory?areaInfo%5BareaId%5D={self.area_id}&areaInfo%5BareaType%5D=2&date%5Byear%5D={year}&date%5Bmonth%5D={month}"
                time.sleep(5)
                try:
                    txt = getTxt(url, self.area_id, self.forbidden_urls)
                    info = getInfo(txt)
                    result = Info(info)
                    insert_into_db(result, self.db_config, self.region)
                    print(f"page {self.region}-{year}-{month} processed over")
                except:
                    print(f"No data available for {self.region}-{year}-{month}")
def main():
    # 数据库连接信息
    db_config = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '123456',
        'database': 'www',
        'charset': 'utf8mb4'
    }
    gequ = {
        '60198': '昌平',
        '71141': '朝阳',
        '60205': '大兴',
        '71445': '东城',
        '60206': '房山',
        '71142': '丰台',
        '71144': '海淀',
        '60207': '怀柔',
        '60246': '门头沟',
        '60247': '密云',
        '60204': '平谷',
        '71143': '石景山',
        '60202': '顺义',
        '71071': '通州',
        '71446': '西城',
        '60199': '延庆'
    }
    
    forbidden_urls = []
    threads = []

    # 创建并启动线程
    for area_id, region in gequ.items():
        thread = MyThread(region, area_id, db_config, forbidden_urls)
        thread.start()
        threads.append(thread)

    # 等待所有线程完成
    for thread in threads:
        thread.join()

    # 处理403 Forbidden的URL重新爬取
    error_urls_file = 'error_urls.txt'  # 错误URL存储文件
    
    if forbidden_urls:
        print("重新尝试处理403 Forbidden的URL...")
        for url in forbidden_urls:
            start_index = url.find("areaInfo%5BareaId%5D=") + len("areaInfo%5BareaId%5D=")
            number = url[start_index:start_index+5]
            region = gequ.get(number)
            try:
                txt = getTxt(url, '', [])  # 重新尝试爬取
                info = getInfo(txt)
                result = Info(info)
                insert_into_db(result, db_config, region)  # 插入到原始表中
                print(f"重新处理 {url} 成功")
            except Exception as e:
                print(f"重新处理 {url} 出错: {e}")
                
                with open(error_urls_file, 'a') as f:  # 打开文件用于追加
                    f.write(url + '\n')  # 写入URL并添加换行符
                print(f"已将出错的URL {url} 存储到 {error_urls_file}")


if __name__ == "__main__":
    main()
    a=input()
