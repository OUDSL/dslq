from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from time import sleep
import requests
from elastic_search import es_get, es_search, es_helper_scan
from elasticsearch import Elasticsearch
import pandas as pd
import os
from pymongo import MongoClient
import json
import re
from urllib import urlopen
from urlparse import urlparse, parse_qs
from bs4 import BeautifulSoup
from xmltodict import parse

#Default base directory 
basedir="/data/web_data/static"
ES_HOST = [{'host':'esearch'}]

mainURL="https://www.gpo.gov"
filterLinks=[]

modsURL_template =  "https://www.gpo.gov/fdsys/pkg/{0}/mods.xml"

#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
    
@task()
def sub(x, y):
    """ Example task that subtracts two numbers
        args: x and y
        return subtraction
    """
    result = x - y
    return result

@task()
def pull_congressional_data(hearingsURL="https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG&browsePath=114&isCollapsed=true&leafLevelBrowse=false&ycord=0"):
    mainLinks(hearingsURL)
    return "Success!! :D :P"



@task()
def get_congressional_data(congress=None, mongo_database="congressional",mongo_collection="hearings",update=True):
    """Congressional Hearing Inventory task
        Agrs: 
        kwargs: congress=<None> # This will run all congresses. Valid values is 99-114 to run individual 
                mongo_database=<'congressional'>, 
                mongo_collection=<'hearings'>, 
                update=<True>
        If update = False will inventory entire congressional hearins. Must delete records in mongo. Task does not check of record exists.
    """
    # Session will work better to store connection state. Cookies!
    s = requests.Session()
    #load base page and setup session
    s.get("https://www.gpo.gov/fdsys/")
    url_template="http://www.gpo.gov/fdsys/search/search.action?sr={0}&originalSearch=collection%3aCHRG&st=collection%3aCHRG&ps=100&na=__congressnum&se=__{1}true&sb=dno&timeFrame=&dateBrowse=&govAuthBrowse=&collection=&historical=true"
    total_ids=[]
    db = MongoClient("dsl_search_mongo",27017)
    #db.congressional.hearings.save(x)
    if update:
        """ Just pull first page of congressional hearing search"""
        if congress:
            #for cong in range(99,115):
            total_ids =total_ids + get_chrg_ids(s,url_template,page=1,congress=congress)
        else:   
            for cong in range(99,115):
                total_ids =total_ids + get_chrg_ids(s,url_template,page=1,congress=cong)
        print "Update Total IDs returned %d" % (len(total_ids))
        for chrg in total_ids:
            if db[mongo_database][mongo_collection].find({'tag':chrg}).count() < 1:
                modsParser(s,chrg,modsURL_template.format(chrg))
    else:  
        """ Run entire inventory """
        if congress:
            total_ids =total_ids + get_ids(s,url_template,congress)
        else:
            for cong in range(99,115):
                total_ids =total_ids + get_ids(s,url_template,congress)
        print "All Total IDs returned %d" % (len(total_ids))
        for chrg in total_ids:
            if db[mongo_database][mongo_collection].find({'tag':chrg}).count() < 1:
                modsParser(s,chrg,modsURL_template.format(chrg))

	

@task()
def search_stats(index,doctype,query,context_pages=5):
    task_id = str(search_stats.request.id)
    #create Result Directory
    resultDir = os.path.join(basedir, 'dsl_tasks/', task_id)
    os.makedirs(resultDir)
    #Get ES dataset
    result = es_retun_all(Elasticsearch(ES_HOST),query,index,doctype,context_pages)
    df = pd.DataFrame(result)
    #Save results to csv
    df.to_csv("{0}/es_query_data.csv".format(resultDir),encoding='utf-8')
    #save dataframe pickle to file
    df.to_pickle("{0}/es_query_data.pkl".format(resultDir))
    return "http://dev.libraries.ou.edu/dsl_tasks/{0}".format(task_id)



def es_retun_all(es,query,index,doctype,context_pages):
    #meta = es_search(es, index, doctype, query=query, page=1, nPerPage=1)

    result = es_helper_scan(es,index,doctype,query,context_pages)

    return result


def mainLinks(hearingsURL):

    r = s.get(hearingsURL)

    soup = BeautifulSoup(r.text,'html.parser')

    test = soup.findAll('input',{"name":"urlhid"})

    for i in test:
        # print mainURL+i.get('value').replace("amp;","")
        level1(mainURL+i.get('value').replace("amp;",""))
    # level1("https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG&browsePath=112&isCollapsed=false&leafLevelBrowse=false")
#****************************************************************************************************************#
#Getting links for HOUSE || JOINT || SENATE
def level1(url):
#This is for getting links for HOUSE || JOINT || SENATE
    # test = "https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG&browsePath=113&isCollapsed=false&leafLevelBrowse=false"
    test = url
    r = s.get(test)

    soup = BeautifulSoup(r.text,"html.parser")

    # print soup.prettify()

    test = soup.findAll('div',class_="browse-level")


    for i in test:
        if "FHOUSE" in i.a["onclick"]:
            # print mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)',i.a["onclick"])[0]
            level2(mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)',i.a["onclick"])[0])
        elif "FJOINT" in i.a["onclick"]:
            # print mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)',i.a["onclick"])[0]
            level2(mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)',i.a["onclick"])[0])
        elif "FSENATE" in i.a["onclick"]:
            # print mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)',i.a["onclick"])[0]
            level2(mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)',i.a["onclick"])[0])


    # test = "goWithVars('/fdsys/browse/collection.action?collectionCode=CHRG&browsePath=114%2FHOUSE&isCollapsed=false&leafLevelBrowse=false',''); return false;"
    # test = "q2///ftp://www.somewhere.com/over/the/rainbow/image.jpg"
    # print re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)',test)[0]


#This gives House || Senate || Joint links for extending
def level2(url):
    # test = "https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG&browsePath=114%2FHOUSE&isCollapsed=false&leafLevelBrowse=false"
    test = url
    r = s.get(test)

    soup = BeautifulSoup(r.text,"html.parser")

    filteredDiv = []
    for i in soup('div'):
        for j in i('div'):
            for k in j('div'):
                filteredDiv.append(k)

    l2 = []
    for i in filteredDiv:
        if i('div') and "browse-level" in i('div')[0]['class']:
            for j in i.findAll('div',class_="level2 browse-level"):
                l2.append(j)

    for i in l2:
        for j in i('a'):
            # print mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)', j['onclick'])[0].replace("isCollapsed=true","isCollapsed=false")
            level3(mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\'\'\);)', j['onclick'])[0].replace("isCollapsed=true","isCollapsed=false"))

    test=""


#Get the links to get options for HTML | PDF | MORE
def level3(url):
    # test ="https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG&browsePath=114%2FHOUSE&isCollapsed=false&leafLevelBrowse=false"
    test = url
    r = s.get(test)

    soup = BeautifulSoup(r.text,"html.parser")

    for i in soup.findAll('div', class_="level3"):
        for j in i('a'):
            # print "Title --> "+j.getText().strip()+"  "+mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\')', j['onclick'])[0].replace("isCollapsed=true","isCollapsed=false")
            morePageLinks(mainURL+re.findall(r'(?<=goWithVars\(\').*?(?=\',\')', j['onclick'])[0].replace("isCollapsed=true","isCollapsed=false"))

#More page links
def morePageLinks(url):
    # test = "https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG&browsePath=114%2FHOUSE%2FCommission+on+Security+and+Cooperation+in+Europe&isCollapsed=false&leafLevelBrowse=false&isDocumentResults=true"
    test = url
    r = s.get(test)
    soup = BeautifulSoup(r.text,"html.parser")
    # print soup.prettify()
    for i in soup.findAll('a'):
        if i.get('href')!=None and "search/page" in i.get('href'):
            if mainURL+"/fdsys/"+i.get('href') not in filterLinks:
                filterLinks.append(mainURL+"/fdsys/"+i.get('href'))
                url =  mainURL+"/fdsys/"+i.get('href')
                parseURL = urlparse(url)
                id = parse_qs(parseURL.query)['packageId'][0]
                modsURL =  "https://www.gpo.gov/fdsys/pkg/"+id+"/mods.xml"
                modsParser(id,modsURL)


def get_chrg_ids(s,url_template,page=1,congress=99):
    try:
        r=s.get(url_template.format(page,congress))
        soup=BeautifulSoup(r.text,'html.parser')
    except:
        sleep(15)
        r=s.get(url_template.format(page,congress))
        soup=BeautifulSoup(r.text,'html.parser')
    links=[]
    for link in soup.findAll('a'):
        if link.get('href'):
            links.append(link.get('href'))
    print "Total Links: %d" % (len(links))
    valid_ids=[]
    for link in links:
        end_url=link.split('/')[-1]
        if re.match('^CHRG*',end_url):
            valid_ids.append(end_url.split('.p')[0])
    print "Total Valid Links: %d" % (len(links))
    return valid_ids

def get_ids(s,url_template,congress):
    cum_ids=[]
    for itm in range(1,1000):
        ids = get_chrg_ids(page=itm,congress=congress)
        if ids==[]:
            break
        cum_ids = cum_ids + ids
    return cum_ids


def modsParser(s,tag,url):
    xmlURL = url
    r = s.get(xmlURL)
    try:
        data = parse(r.text)
        data["tag"]=tag
        x = json.loads(json.dumps(data).replace("@",'').replace("#",''))
        db = MongoClient("dsl_search_mongo",27017)
        db.congressional.hearings.save(x)
    except:
        print "ERROR: %s %s" %  (tag,url) 



