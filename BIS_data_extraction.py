# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import pandas as pd
import csv
from bs4 import BeautifulSoup
import requests, zipfile, io
import os

class Scraper:
    def __init__(self):
        self.baseurl = 'https://www.bis.org/statistics/full_data_sets.htm'
        
    def make_dir(self, dir_name):
        if not os.path.isdir(dir_name):
            os.mkdir(dir_name)
        
        return True

    def extract_data(self, asin):
        try:
            data = requests.get(self.baseurl)
            soup = BeautifulSoup(data.text, 'html.parser')
            div = soup.find("div", {"id": "cmsContent"})
            
            lis = [li for li in div.findAll('li')]
            
            final_data = []
            
            for li in lis:
                name = li.find('a').get_text()
                link = "https://www.bis.org" + li.find('a', href=True)['href']
                
                final_data.append(
                    {
                        'NAME': name,
                        'LINK': link            
                    }
                )
            return final_data
        except:
            return 0
    
    def generate_csv(self, final_data, fieldnames):
        try:
            with open('master_data.csv', 'w', encoding='UTF8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(final_data)
            return "Master Data saved!"
        except:
            return "Error while saving master data"
    
    def extract_zip(self, zip_file_url):
        try:
            r = requests.get(zip_file_url)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall("Credit_to_GDP_gaps/")
            return "Zip file extracted!"
        except:
            return "Error while extracting zip"
    
    def save_extracted_data(self):
        try:
            credit_to_gdp = pd.read_csv('Credit_to_GDP_gaps/WS_CREDIT_GAP_csv_col.csv')
            timestamps = credit_to_gdp.columns.values.tolist()[7:]
            res = self.make_dir('csv')
            
            if res:
                for timestamp in timestamps:
                    credit_to_gdps = pd.pivot_table(credit_to_gdp, index=['Frequency', 'Borrowing sector', 'Lending sector', 'Credit gap data type'],columns="Borrowers' country",values=timestamp)
                    credit_to_gdps.to_csv('csv/' + timestamp + '.csv')
            
            return "Timestamp - Countries data saved!"
        except:
            return "Error while creatingn csv countries data"




if __name__ == '__main__':
    scrape = Scraper()
    final_data = scrape.extract_data('cmsContent')
    fieldnames = ['NAME', 'LINK']
    
    result1 = scrape.generate_csv(final_data, fieldnames)
    print(result1)
    
    zip_file_url = 'https://www.bis.org/statistics/full_credit_gap_csv.zip'
    result2 = scrape.extract_zip(zip_file_url)
    print(result2)
    
    result3 = scrape.save_extracted_data()
    print(result3)
    