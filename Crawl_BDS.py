#Configure and library
                                                ##LIBRARY
import atexit
from multiprocessing import Process
import numpy as np
from selenium import webdriver
import time
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import random
from selenium.common.exceptions import NoSuchElementException , ElementNotInteractableException
from selenium.webdriver.common.by import By
import pandas as pd
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
import psycopg2
                                                ##CONFIGURE
nameCol_Maximum = ['Link','Diện tích', 'Mức giá','Hướng nhà','Hướng ban công', 'Số tầng', 'Số phòng ngủ', 'Số toilet', 'Pháp lý',
 'Nội thất','Title','Address','Ngày đăng','Ngày hết hạn','Loại tin','Mã tin','Check','Đường vào','Mặt tiền']
nameCol = ['Loại tin','Mã tin','Link','Diện tích', 'Mức giá','Hướng nhà', 'Số tầng', 'Số phòng ngủ', 'Số phòng tắm, vệ sinh', 'Pháp lý',
 'Nội thất','Title','Address','Đường vào','Mặt tiền']
# link_web = 'https://batdongsan.com.vn/ban-nha-dat-tp-hcm'
# link_web = 'https://batdongsan.com.vn/ban-can-ho-chung-cu-tp-hcm'
# link_web = 'https://batdongsan.com.vn/ban-nha-rieng-tp-hcm'
# link_web = 'https://batdongsan.com.vn/ban-dat-tp-hcm'
link_web = 'https://batdongsan.com.vn/ban-nha-mat-pho-tp-hcm'
# link_web = 'https://batdongsan.com.vn/ban-nha-biet-thu-lien-ke-tp-hcm'
column_mapping = {
    "Loại tin": "loai_tin",
    "Mã tin": "ma_tin",
    "Link": "link",
    "Diện tích": "dien_tich",
    "Mức giá": "muc_gia",
    "Hướng nhà": "huong_nha",
    "Số tầng": "so_tang",
    "Số phòng ngủ": "so_phong_ngu",
    "Pháp lý": "phap_ly",
    "Nội thất": "noi_that",
    "Title": "title",
    "Address": "address",
    "Đường vào": "duong_vao",
    "Mặt tiền": "mat_tien",
    "Hướng ban công" : "huong_ban_cong",
    "Ngày đăng" : "ngay_dang",
    "Ngày hết hạn" : "ngay_het_han",
    "Số phòng tắm, vệ sinh" : "so_toilet"
}
def clean_link(link):
    if link is None or (isinstance(link, float) and np.isnan(link)) or link == "":
        return ""
    return link
# Class Crawl
class batdongsan():
    def __init__(self,link,name=[]):
        self.link = link
        self.name = name
        self.result = pd.DataFrame(columns=name)
        self.next_page=[]
        options= Options()
        options= Options()
        options.page_load_strategy = 'eager'
        options.headless = True
        options.add_argument("--disable-javascript")
        options.add_argument("--blink-settings=imagesEnabled=false")
        self.driver = webdriver.Chrome(options=options)
    def Crawl(self):
        web_page=self.link
        while True :
            List_link , next_page = self.get_links(web_page)
            for i in range(len(List_link)-1):
                # if List_link[i] in link_have :
                #     continue
                self.getData(List_link[i])
            web_page = next_page
            if web_page == None:
                break
    def get_links(self,link):
        driver = self.driver
        driver.get(link)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 
            'a.js__product-link-for-product-id'))
        )

        text = driver.find_elements(By.CSS_SELECTOR,
        'a.js__product-link-for-product-id')
        links= [i.get_attribute('href') for i in text]

        next_page = driver.find_elements(By.CSS_SELECTOR,'a.re__pagination-icon')[-1]
        next_page = next_page.get_attribute('href')
        self.next_page.append(next_page)
        return links  ,next_page
    def getData(self,link):
        driver = self.driver
        test = pd.Series("",index=self.name,dtype="object")
        driver.get(link)


        # Các nội dung tổng quan về mẫu
        link = clean_link(link)
        test['Link'] = link
        test['Title'] = driver.find_element(By.CSS_SELECTOR,'h1.re__pr-title').text
        test['Address'] = driver.find_element(By.CSS_SELECTOR,'.js__pr-address').text

        # Thử lấy dữ liệu xác thực và số zalo liên hệ
        # try:
        #     test['Check'] = driver.find_element(By.CSS_SELECTOR,
        #     '.js__product-detail-web .re__pr-stick-listing-verified .re__text').text
        # except NoSuchElementException :
        #     test['Check']='Chưa Xác Thực'
        # Thử lấy dữ liệu chi tiêt về mẫu
        name_data = driver.find_elements(By.CSS_SELECTOR,

        '.js__li-specs  .js__section-body .js__other-info .re__pr-specs-content-item .re__pr-specs-content-item-title')
        value_data =  driver.find_elements(By.CSS_SELECTOR,

        '.js__li-specs  .js__section-body .js__other-info .re__pr-specs-content-item .re__pr-specs-content-item-value')
        data = pd.Series(value_data,index=name_data)

        for i in range(len(name_data)):
            test[name_data[i].text] = value_data[i].text

        name_data_2 = driver.find_elements(By.CSS_SELECTOR,
        '.js__pr-config .js__pr-config-item .title')

        value_data_2 = driver.find_elements(By.CSS_SELECTOR,
        '.js__pr-config .js__pr-config-item .value')

        for i in range(len(name_data_2)):
            test[name_data_2[i].text] = value_data_2[i].text

        self.result = pd.concat([self.result, test.to_frame().T], ignore_index=True)
def RUN(X):
    X.Crawl()
def store_data():
    X.result.columns = X.result.columns.to_series().replace(column_mapping, regex=True)
    data = X.result
    db_user = "postgres"
    db_password = "postgres"
    db_host = "127.0.0.1"
    db_port = "5432"
    db_name = "my_db"
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
    data.to_sql('HOUSE_HCM', engine, index=False, if_exists='append')
X = batdongsan(link=link_web,name=nameCol)
if __name__ == "__main__":
    atexit.register(store_data)
    process = Process(target=RUN(X))
    process.start()
    time.sleep(3600)
    process.terminate()
    process.join()
