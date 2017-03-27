import requests
import collections
import pandas as pd

def countChecker(url_template):
    ## to-dos: use congress.gov's api to get congress stats
    website_stat = pd.read_csv('/Users/Andrew/Desktop/website.csv', sep=',', header=0)
    congressArray = []

    # parse the data source and grab the tag to the congressArray
    for page in range(1,517):
        req=requests.get(url_template.format(page))
        data=req.json()
        for item in data['results']:
            congress = item['TAG'].split('-')[-1]
            if congress[0:2] =='99':
                congress=congress[:2]
                congressArray.append(int(congress))
            else:
                congress=congress[:3]
                if congress.isdigit() == True:
                    congressArray.append(int(congress))

    # count frequency by congress number
    counter = collections.Counter(congressArray)
    df = pd.DataFrame.from_dict(counter, orient='index').reset_index()
    df = df.rename(columns={'index': 'congress', 0: 'codeCount'})

    # merge dataframes and write the result
    result = pd.merge(website_stat, df, on = 'congress', how='inner')
    result['diff'] = result['website'] - result['codeCount']
    result.to_csv("countChecker.csv", sep=',', index=False)
    print("countChecker file is saved")
    return(result)


# calling functions
url_template = "https://cc.lib.ou.edu/api-dsl/data_store/data/congressional/hearings/?format=json&page={0}"
result = countChecker(url_template)
