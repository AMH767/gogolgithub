import os
import sys
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from bs4 import BeautifulSoup
import re
import psycopg2
from time import sleep
from datetime import datetime

# Настройки из окружения GitHub
DATABASE_URL = os.environ.get('DATABASE_URL')
QUERY = os.environ.get('QUERY', '')
MANY = int(os.environ.get('MANY', 20))
TASK_ID = os.environ.get('TASK_ID', 'github-task')
MAX_WORKERS = 15 # Максимальная скорость на GitHub

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def save_to_db(data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO results (task_id, name, category, address, phone, rating, reviews_count, working_hours, website, social_links, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (TASK_ID, data.get('name'), data.get('category'), data.get('address'), data.get('phone'), 
              data.get('rating'), data.get('reviews_count'), data.get('working_hours'), 
              data.get('website'), data.get('social_links'), data.get('url')))
        conn.commit()
        cursor.close(); conn.close()
    except Exception as e:
        print(f"DB Error: {e}")

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--window-size=1200,800")
    driver = webdriver.Chrome(options=options)
    stealth(driver, languages=["ru-RU", "ru"], vendor="Google Inc.", platform="Win32", webgl_vendor="Intel Inc.", renderer="Intel Iris OpenGL Engine", fix_hairline=True)
    return driver

def get_links(search_query, limit):
    driver = create_driver()
    links = set()
    try:
        url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}?hl=ru"
        driver.get(url)
        sleep(3)
        last_len = 0
        for _ in range(10): # Скроллим 10 раз
            found = driver.find_elements(By.XPATH, '//a[contains(@href, "/maps/place/")]')
            for f in found:
                try:
                    href = f.get_attribute('href')
                    if href: links.add(href)
                except: continue
            if len(links) >= limit: break
            driver.execute_script('try { document.querySelector("div[role=\'feed\']").scrollTop += 1000; } catch(e) {}')
            sleep(1.5)
            if len(links) == last_len: break
            last_len = len(links)
    finally:
        driver.quit()
    return list(links)

def parse_details(url):
    driver = create_driver()
    try:
        driver.get(url)
        sleep(2)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        data = {'url': url, 'name': 'N/A', 'category': 'N/A', 'address': 'N/A', 'phone': 'N/A', 'rating': 'N/A', 'reviews_count': 'N/A', 'working_hours': 'N/A', 'website': 'N/A', 'social_links': ''}
        
        title = soup.find('h1')
        if title: data['name'] = title.get_text(strip=True)
        
        cat = soup.select_one('button[jsaction*="category"]')
        if cat: data['category'] = cat.get_text(strip=True)
        
        # Упрощенный сбор остальных данных для скорости
        items = soup.find_all(['button', 'a'], attrs={'data-item-id': True})
        for item in items:
            iid = item.get('data-item-id', '')
            txt = item.get_text(strip=True)
            if 'address' in iid: data['address'] = txt
            elif 'phone' in iid: data['phone'] = txt
            elif 'authority' in iid: data['website'] = item.get('href', txt)

        save_to_db(data)
        print(f"Done: {data['name']}")
    finally:
        driver.quit()

if __name__ == "__main__":
    print(f"Starting Ultra Worker: {QUERY}")
    all_links = get_links(QUERY, MANY)
    print(f"Found {len(all_links)} links. Starting parallel parse...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(parse_details, all_links[:MANY])
    
    print("All done!")
