import requests

url = 'https://data.electricitymaps.com/2025-04-03/{}_{}_hourly.csv'
countries = ['IT', 'SE']
years = [2021 + i for i in range(0, 4)]
for country in countries:
    for year in years:
        query = url.format(country, year)
        response = requests.get(query)
        with open(f'datasets/{country}_{year}.csv', 'w') as fp:
            if response.status_code != 200:
                print(f'error code {response.status_code} with request {query}')
                continue
            fp.write(response.text)