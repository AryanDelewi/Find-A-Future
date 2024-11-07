import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import time
from concurrent.futures import ThreadPoolExecutor

# URL_dict = {
#     'Politie': "https://kombijde.politie.nl/vacatures",
#     'AS Watson': "https://www.werkenbijaswatson.nl/vacatures",
#     'VodafoneZiggo': "https://careers.vodafoneziggo.com/vacatures",
#     'KPN':"https://jobs.kpn.com/vacatures",
#     "Odido":"https://werkenbij.odido.nl/vacatures",
#     "WerkenVoorNederland":"https://www.werkenvoornederland.nl/vacatures"
# }


class FindAJob:
    
  
    def __init__(self,keywords, URL_dict, time_sleep, concurrentworkers):
       
        self.keywords = keywords
        self.URLs = URL_dict
        self.concurrentworkers = concurrentworkers
        self.time_sleep = time_sleep
    
    def exploreURL(self,URL, delay = False):
        if delay:
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Headless mode
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

            # Go to the live URL of the careers page
            driver.get(URL)

            # Wait for the page to load completely
            time.sleep(self.time_sleep)
            # Return the page source (HTML content)
            html_content = driver.page_source

            # Close the driver after fetching the HTML
            driver.quit()
            return BeautifulSoup(html_content, "html.parser")

        else:
                
            req = requests.get(URL)
            if req.status_code != 200:
                print(f"Get request failed, using the URL: {URL}. Please check if the URL is still valid.")
                return None
            return BeautifulSoup(req.content,"html.parser")

    def fetch_combined_html(self, pages, serverSideDelay=False):
            # Function to fetch HTML content of each page
            def fetch_html(page):
                try:
                    response = requests.get(page)
                    response.raise_for_status()
                    return response.text  # Return raw HTML as a string
                except requests.RequestException as e:
                    print(f"Request failed for page: {page}. Error: {e}")
                    return ""
                
            def fetch_html_delay(page):
                try:
                    chrome_options = Options()
                    chrome_options.add_argument("--headless")  # Headless mode
                    chrome_options.add_argument("--disable-gpu")
                    chrome_options.add_argument("--no-sandbox")
                    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

                    # Go to the live URL of the careers page
                    driver.get(page)

                    # Wait for the page to load completely
                    time.sleep(self.time_sleep)

                    # Return the page source (HTML content)
                    html_content = driver.page_source

                    # Close the driver after fetching the HTML
                    driver.quit()

                    

                    return html_content
                except Exception as e:
                    # Handle any exceptions that occur during the request
                    print(f"Failed to fetch content from {page}. Error: {e}")
                    return ""
            
            fetching_html = fetch_html_delay if serverSideDelay else fetch_html
    
            # Using ThreadPoolExecutor to fetch all pages concurrently
            with ThreadPoolExecutor(max_workers=self.concurrentworkers) as executor:
                html_contents = executor.map(fetching_html, pages)
                
            # Combine all HTML into a single string
            combined_html = "".join(html_contents)
            
            # Parse the combined HTML into one BeautifulSoup object
            return BeautifulSoup(combined_html, "html.parser")

    def getNumberOfPages(self,URL, Company):
        req = requests.get(URL)
        if req.status_code != 200:
            print(f"Get request failed for {Company}, using the URL: {URL}. Please check if the URL is still valid.")
            return None
        html_req = BeautifulSoup(req.content, "html.parser")
        
        extractNrOfPagesHTML = html_req.find('title')
        if extractNrOfPagesHTML is None:
            print(f"Failed to extract number of pages from html. Please check if html code is adjusted for {Company} on URL: {URL}")
            #return None
        
        NrOfPages = re.search(r'/\d+', extractNrOfPagesHTML.text).group().replace('/','')
        
        pages = URL +'?page='+ pd.Series(np.arange(1,int(NrOfPages)+1)).astype(str)
        return pages
    
    
    # def getVacancyLocation(self,html,job_init, job_class, loc_init, loc_class):
    #     joblisting = html.find_all(job_init, class_=job_class)
    #     vacancies = pd.Series(joblisting).apply(lambda x: x.text).str.strip().rename('Vacancy')  

    #     locationslisting = html.find_all(loc_init, class_=loc_class)
    #     locations = pd.Series(locationslisting).apply(lambda x: x.text).str.strip().rename('Location')
    #     return vacancies,locations
    
    
    def getVacanciesPolitie(self):
        # Unusual html code
        Company = 'Politie'
        URL = self.URLs[Company]
        #URL = "https://kombijde.politie.nl/vacatures"
        
        req = requests.get(URL)
        if req.status_code != 200:
            print(f"Get request failed for {Company}, using the URL: {URL}. Please check if the URL is still valid.")
            return None
        
        html_req = BeautifulSoup(req.content, "html.parser")
        htmlList_vacancies = html_req.find_all('td')
        if htmlList_vacancies is None:
            print(f"Failed to extract vacancies from html. Please check if html code is adjusted for {Company} on URL: {URL}")
            return None
        identifyer_vacancy = pd.Series(htmlList_vacancies).apply(lambda x: x.find('a', class_='no-underline').get('href')).rename('identifyer')
        information_vacancy = pd.Series(htmlList_vacancies).apply(lambda x: x.text).rename('variable').str.replace(r'\s+',' ',regex=True)
        if identifyer_vacancy is None or information_vacancy is None:
            print(f"Failed to extract either identifyer or information of vacancy for {Company}.")
        df_vacanciesLong = pd.concat([identifyer_vacancy, information_vacancy],axis=1)
        df_vacanciesWide = df_vacanciesLong.groupby('identifyer')['variable'].apply(list).reset_index()
        df_vacancyName, df_vacancyLocation = zip(*df_vacanciesWide['variable'].apply(lambda x: (x[0], x[1])))
        df_PoliceVacancies = pd.concat([pd.Series(df_vacancyName).rename('Vacancy'),
                                        pd.Series(df_vacancyLocation).rename('Location').str.replace('Locatie(s):','')],axis=1)
        df_PoliceVacancies['Company'] = Company
        return df_PoliceVacancies
    
    


    def getVacanciesAswatson(self):
        # can do with simply a html request, no java processes
        Company = 'AS Watson'
        URL = self.URLs[Company]
        #URL = "https://www.werkenbijaswatson.nl/vacatures"
        
        pages = self.getNumberOfPages(URL,Company)
        
        
        html_all = self.fetch_combined_html(pages)
        
        vacanies_html = html_all.find_all('a',class_="js-href js-track")
        location_hrs_html = html_all.find_all('li', class_="list-ui__meta-item")
        
        vacanies = pd.Series(vacanies_html).apply(lambda x: x.text).str.replace(r'\s+',' ',regex=True).str.strip().rename('Vacancy')
        location_hrs = pd.Series(location_hrs_html).apply(lambda x: x.text).str.replace(r'\s+',' ',regex=True).str.strip()
        location = location_hrs[~location_hrs.str.endswith('uur per week')].reset_index(drop=True).rename('Location')
        
        df_ASwatsonVacancies =pd.concat([vacanies,location],axis=1)
        df_ASwatsonVacancies['Company'] = Company
        return df_ASwatsonVacancies
    
    
    

    def getVacanciesVodaphoneZiggo(self):
        # similar to AS watson. except now I require a pathway to get like the java processed vacancies.
        Company = 'VodafoneZiggo'
        URL = self.URLs[Company]
        #URL = "https://careers.vodafoneziggo.com/vacatures"
        
        pages = self.getNumberOfPages(URL,Company)
        
        html_all = self.fetch_combined_html(pages,serverSideDelay=True)

        extractedlists = html_all.find_all('div', class_='page page--gradient')
        combined_html = "".join([str(element) for element in extractedlists])
        soup = BeautifulSoup(combined_html, "html.parser")
        getlisting_details = soup.find_all('ul',class_='meta')
        locations = pd.Series(getlisting_details).apply(lambda x: x.find_all('li',class_='meta__item')[1].get_text(strip=True))
        
        vacancies = pd.Series(html_all.find_all('a', class_='linked-item__link')).apply(lambda x: x.get_text(strip=True)).rename('Vacancy')
        
        df_VodaphoneZiggoVacancies = pd.concat([vacancies,locations.rename('Location')],axis=1)
        df_VodaphoneZiggoVacancies['Company'] = Company

        return df_VodaphoneZiggoVacancies
    

    def getVacanciesKPN(self):
        # similar to AS watson. except now I require a pathway to get like the java processed vacancies.
        Company = 'KPN'
        URL = self.URLs[Company]
        #URL = "https://jobs.kpn.com/vacatures"
        
        getPages = self.exploreURL(URL, delay = True)
        NrOfPages = getPages.find('span', {'data-pagination-total': True}).text
        
        pages = URL + '/page/' + pd.Series(np.arange(1,int(NrOfPages)+1)).astype(str)
        html_all = self.fetch_combined_html(pages,serverSideDelay=True)
        joblisting = html_all.find_all('a',class_='favourite favorite-add favorite-vacancy')

        vacancies = pd.Series(joblisting).apply(
                lambda x: re.search(r'"jobName":"(.*?)","', str(x)).group(1) if re.search(r'"jobName":"(.*?)","', str(x)) else None
        ).rename('Vacancy')

        listingsdetails = html_all.find_all('ul',class_='list-unstyled')
        listingsdetails1 = pd.Series(listingsdetails).apply(lambda x: x.find('li',class_='joined'))#.iloc[-1:-len(vacanies)]#.apply(lambda x: x[2] if x is not None)
        locationsDirt = listingsdetails1.apply(lambda x: x.text.strip() if x is not None else None)
        locations = locationsDirt.dropna().reset_index(drop=True).rename('Location')
        
        df_KPN = pd.concat([vacancies,locations],axis=1)
        df_KPN['Company'] = Company

        return df_KPN
    
    
    def getVacanciesOdido(self):
        # similar to KPN.
        Company = 'Odido'
        URL = self.URLs[Company]
        #URL = "https://werkenbij.odido.nl/vacatures/page"
        
        getPages = self.exploreURL(URL, delay=True)
        pages = getPages.find_all('li',class_ = 'paginationjs-page J-paginationjs-page')
        NrOfPages = pd.Series(pages).apply(lambda x: x.text).astype(int).max()
        
        pages = URL + '/page/' + pd.Series(np.arange(1,int(NrOfPages)+1)).astype(str)
        html_all = self.fetch_combined_html(pages,serverSideDelay=True)


        joblisting = html_all.find_all('div', class_='title')
        vacancies = pd.Series(joblisting).apply(lambda x: x.text).str.strip().rename('Vacancy')  

        locationslisting = html_all.find_all('li', class_='location')
        locations = pd.Series(locationslisting).apply(lambda x: x.text).rename('Location')

        df_Odido = pd.concat([vacancies,locations],axis=1)
        df_Odido['Company'] = Company

        return df_Odido
    
    def getVacanciesWerkenVoorNederland(self):
        Company = 'WerkenVoorNederland'
        URL = self.URLs[Company]
        
        html_page = self.exploreURL(URL)
        joblistings = html_page.find_all('h2',class_='vacancy__title')
        
        NrOfListingsPerPage = len(joblistings)
        TotalListings = int(html_page.find('span',class_='vacancy-result-bar__totals-badge').text.replace('.',''))
        
        PagesApprox = int(np.ceil(TotalListings/NrOfListingsPerPage))
        
        html_page_total = self.exploreURL(URL+'?pagina='+str(PagesApprox))
                
        joblistings = html_page_total.find_all('h2',class_='vacancy__title')
        Vacancy = pd.Series(joblistings).apply(lambda x: x.text).str.strip().rename('Vacancy')
        
        employer = html_page_total.find_all('p',class_='vacancy__employer')
        Company = pd.Series(employer).apply(lambda x: x.text).str.strip().rename('Company')

        detailsListing = html_page_total.find_all('ul', class_='job-short-info--width')
        Locations = pd.Series(detailsListing).apply(lambda x: x.find('span',class_='job-short-info__value job-short-info__value-icon').text).str.strip().rename('Locations')
        df_WVN = pd.concat([Vacancy,Locations,Company],axis=1)
        
        return df_WVN
    
    def getVacanciesAhold(self):
        
        Company = 'Ahold'
        URL = self.URLs[Company]
        
        def classify_logo(src):
            if 'aholddelhaize' in src.lower():
                return 'Ahold'
            elif 'ah' in src.lower():
                return 'Albert Heijn'
            elif 'etos' in src.lower():
                return 'Etos'
            return Company
        
        html_page = self.exploreURL(URL,delay=True)
        NrOfPages = int(html_page.find('span',{'data-pagination-total': True}).text)
        
        pages = URL + '/page/' + pd.Series(np.arange(1,int(NrOfPages)+1)).astype(str)
        html_all = self.fetch_combined_html(pages,serverSideDelay=True)

        joblisting = html_all.find_all('h2', class_='card-title')
        vacancies = pd.Series(joblisting).apply(lambda x: x.text).str.strip().rename('Vacancy')  

        locationslisting = html_all.find_all('div', class_='location')
        locations = pd.Series(locationslisting).apply(lambda x: x.text).str.strip().rename('Location')

        cards = html_all.find_all('div',class_='card-body vacancy')
        imgName =  pd.Series(cards).apply(lambda x: x.find('img').get('src'))
        Company = imgName.apply(classify_logo).rename('Company')
    
        df_Ahold = pd.concat([vacancies,locations,Company],axis=1)
        return df_Ahold
    
    def getVacanciesFrieslandCampina(self):
        Company = 'FrieslandCampina'
        URL = self.URLs[Company]
        #URL = "https://careers.frieslandcampina.com/global/en/vacancy-search?search_api_fulltext="
        
        Html_initial = self.exploreURL(URL)
        FindMaxPage = Html_initial.find('li', class_ = 'pagination__label').text.strip()
        NrOfPages = int(FindMaxPage.split('/')[-1])

        pages = URL + '&page=' + pd.Series(np.arange(1,int(NrOfPages)+1)).astype(str)
        html_all = self.fetch_combined_html(pages)

        joblisting = html_all.find_all('h3',class_='vacancy-card__title u-h3--small')
        vacancies = pd.Series(joblisting).apply(lambda x: x.text).str.strip().rename('Vacancy')  

        locationsDirty = html_all.find_all('span',class_='meta-data-item__label')
        locations = pd.Series(locationsDirty).apply(lambda x: x.text if re.search(r'\(.*?\)', x.text) else None).dropna().rename('Locations')
        DutchVacancies = locations.apply(lambda x: 'NLD' in x)
        vacanciesDutch = vacancies[DutchVacancies]
        locationsDutch= locations[DutchVacancies]
        df_FC = pd.concat([vacanciesDutch,locationsDutch],axis=1)
        df_FC['Company'] = Company
        return df_FC
    
