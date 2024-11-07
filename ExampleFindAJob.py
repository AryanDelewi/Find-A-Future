# Create an instance of the FindAJob class with the keywords and URLs
from FindAJob import FindAJob
keywords = ['Data Scientist', 'Data', 'IT', 'AI', 'Machine learning']
URL_dict = {
    'Politie': "https://kombijde.politie.nl/vacatures",
    'AS Watson': "https://www.werkenbijaswatson.nl/vacatures",
    'VodafoneZiggo': "https://careers.vodafoneziggo.com/vacatures",
    'KPN':"https://jobs.kpn.com/vacatures",
    "Odido":"https://werkenbij.odido.nl/vacatures",
    "WerkenVoorNederland":"https://www.werkenvoornederland.nl/vacatures",
    "Ahold":"https://careers.aholddelhaize.com/vacancies/global-brands/ahold-delhaize-1/albert-heijn-1/etos-1/country/nld",
    "FrieslandCampina":"https://careers.frieslandcampina.com/global/en/vacancy-search?search_api_fulltext="
}

job_finder = FindAJob(keywords, URL_dict, time_sleep = 2, concurrentworkers=4)

# Test the methods to fetch vacancies for each company

job_finder.getVacanciesPolitie()

job_finder.getVacanciesAswatson()

job_finder.getVacanciesVodaphoneZiggo()

job_finder.getVacanciesKPN()

job_finder.getVacanciesOdido()

job_finder.getVacanciesWerkenVoorNederland()

job_finder.getVacanciesAhold()

job_finder.getVacanciesFrieslandCampina()

