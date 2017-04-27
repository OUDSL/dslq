from datetime import datetime

from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from time import sleep
import requests
from nltk import sent_tokenize
import collections
from .elastic_search import es_get, es_search, es_helper_scan,es_insert,es_delete_by_tag,es_delete_all,es_index_exist,es_add_chamber,es_helper_main_scan
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
from subprocess import call
import ConfigParser

#Default base directory 
basedir="/data/web_data/static"
ES_HOST = [{'host':'esearch'}]

mainURL="https://www.gpo.gov"
filterLinks=[]
s= requests.session()
modsURL_template =  "https://www.gpo.gov/fdsys/pkg/{0}/mods.xml"


def _get_config_parameter(group,param,config_file="cybercom.cfg"):
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    return config.get(group,param)
def _api_get(tag,collection='inventory'):
    base_url = _get_config_parameter('api','base_url')
    query= "{'filter':{'TAG':'%s'}}" % (tag)
    api_url = "{0}/api-dsl/data_store/data/congressional/{1}/.json?query={2}".format(base_url,collection,query)
    req =requests.get(api_url)
    return req.json()
def _api_save(data,collection='inventory'):
    token = _get_config_parameter('api','token')
    base_url = _get_config_parameter('api','base_url')
    headers ={"Content-Type":"application/json","Authorization":"Token {0}".format(token)}
    api_url = "{0}/api-dsl/data_store/data/congressional/{1}/.json".format(base_url,collection)
    req = requests.post(api_url,data=json.dumps(data),headers=headers)
    req.raise_for_status()
    return True
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
def countChecker():
    url_template = "https://cc.lib.ou.edu/api-dsl/data_store/data/congressional/hearings/?format=json&page={0}"

    task_id = str(countChecker.request.id)
    #create Result Directory
    resultDir = os.path.join(basedir, 'dsl_tasks/', task_id)
    os.makedirs(resultDir)

    ## to-dos: use congress.gov's api to get congress stats
    website_stat = pd.read_csv('website.csv', sep=',', header=0)
    cong = [99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115]
    stat = [1,1,0,1,55,289,870,1685,2158,2367,2890,3646,3261,3094,2708,1990,12]
    website_stat = pd.DataFrame({'congress': cong, 'website':stat})

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

    df = pd.DataFrame(result)
    #Save results to csv
    df.to_csv("{0}/es_query_data.csv".format(resultDir),encoding='utf-8')
    #save dataframe feather to file
    #:feather.write_dataframe(df, "{0}/es_query_data.pkl".format(resultDir))

    return "https://cc.lib.ou.edu/dsl_tasks/{0}/".format(task_id)

@task()
def get_cong_data_python3(congress=None):
    """Python 3 script to pull congressional hearings from gpo website.
       args: 
       kwargs: 
            congress -  comma separated string of congress numbers ("99,100,101,102,103,....115")
                        Default will run all 99 through 115
    """
    task_id = str(get_cong_data_python3.request.id)
    #create Result Directory
    resultDir = os.path.join(basedir, 'dsl_tasks/', task_id)
    os.makedirs(resultDir)
    if congress:
        cong =congress.split(',')
    else: 
        cong = ['99','100','101','102','103','104','105','106','107','108','109','110','111','112','113','114','115']
    for c in cong:
        call(["/anaconda3/gpo/mods.py",c,str(int(c)+1),"{0}/log.txt".format(resultDir),_get_config_parameter('api','token')])
    return "{0}/dsl_tasks/{1}".format(_get_config_parameter('api','base_url'),task_id)
    
@task()
def search_stats(index,doctype,query,context_pages=5):
    """
    Search results returned in csv and pickled dataframe!
    args:
        index - Elasticsearch Index
        doctype - Elasticsearch Doctype
        query - Elasticsearch query
        context_pages - Default 5 (Number of lines above and below
    """
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
    return "{0}/dsl_tasks/{1}".format(_get_config_parameter('api','base_url'),task_id)

@task()
def search_main_stats(index,doctype,query,context_pages=5):
    """
    Search results returned in csv and pickled dataframe!
    args:
        index - Elasticsearch Index
        doctype - Elasticsearch Doctype
        query - Elasticsearch query
        context_pages - Default 5 (Number of lines above and below
    """
    task_id = str(search_main_stats.request.id)
    #create Result Directory
    resultDir = os.path.join(basedir, 'dsl_main_tasks/', task_id)
    os.makedirs(resultDir)
    #Get ES dataset
    result = es_helper_main_scan(Elasticsearch(ES_HOST),index,doctype,query,context_pages)
    df = pd.DataFrame(result)
    #Save results to csv
    df.to_csv("{0}/es_query_data.csv".format(resultDir),encoding='utf-8')
    #save dataframe pickle to file
    df.to_pickle("{0}/es_query_data.pkl".format(resultDir))
    return "{0}/dsl_tasks/{1}".format(_get_config_parameter('api','base_url'),task_id)

@task()
def index_data(): # "inventory_option":"YES"
    """
    Elasticsearch Index data: Runs through all recorded hearings within congressional hearings collection.
    """
    api_host=_get_config_parameter('api','base_url')
    URL="{0}/api-dsl/data_store/data/congressional/hearings/?format=json".format(api_host)
    r = s.get(URL)
    rjson = r.json()
    pagecount = rjson['meta']['pages']
    url_template="{0}/api-dsl/data_store/data/congressional/hearings/.json?page={1}"
    hearings_count=0
    hearings_new =0
    hearings_retry=0
    for i in range(1,pagecount+1):
        rd=requests.get(url_template.format(api_host,i))
        rdjson=rd.json()
        for item in rdjson['results']:
            #metadata = htmlparser(item)
            hearings_count+=1
            data=_api_get(item["TAG"])
            if data['count']>0:
                if data['results'][0]['STATUS'] != 'SUCCESS':
                    hearings_retry+=1
                    metadata = htmlparser(item)
                    metadata['_id']=data['results'][0]['_id']
                    _api_save(metadata)
            else:
                hearings_new+=1
                _api_save(htmlparser(item))
    return "{0} Congressional Hearings Checked. New: {1}, Retry: {2}".format(hearings_count,hearings_new,hearings_retry)
            
@task()
def add_chamber():
    es_add_chamber("congressional","hearings",Elasticsearch(ES_HOST))

def es_retun_all(es,query,index,doctype,context_pages):
    """
    Returns all results from search to return results 
    """
    result = es_helper_scan(es,index,doctype,query,context_pages)
    return result

def htmlparser(x):
    """
    Function to parse metadata and return text from gpo site and index into Elasticsearch.
    """
    line_count=0
    rurls=requests.session()
    url=""
    tag=""
    try:
        #set metadata
        if type(x['HELD_DATE'])== list:
            helddate=x['HELD_DATE'][-1]
        else:
            helddate=x['HELD_DATE']

        tag = x['TAG']
        url = x['URL']
        title = x['TITLE_INFO'][0]['title']
        title = title.replace("&amp;","&").replace("&quot;",'"')\
                    .replace("&apos;","'").replace("&gt;",">").replace("&lt;","<")
        
        hd = datetime(int(helddate.split('-')[0]),int(helddate.split('-')[1]),int(helddate.split('-')[2]))
        # print hd
        h_date = '{dt:%B} {dt.day}, {dt.year}'.format(dt=hd)
        # check if url of text is available
        if url=="":
            #db.congressional.inventory.save({'TAG':tag,'LINE_COUNT': 'N/A','TYPE': 'PDF','STATUS':'FAIL'})
            return {'TAG':tag,'LINE_COUNT': 'N/A','TYPE': 'PDF','STATUS':'FAIL'}
        # Get text from GPO url
        try:
            soup = BeautifulSoup(rurls.get(url).text,'html.parser')
        except:
            sleep(60)
            soup = BeautifulSoup(rurls.get(url).text,'html.parser')

        startPoint = soup.text.find(h_date.upper())
        requiredData = soup.text[startPoint::].replace('\n'," ").replace("&amp;","&").replace("&quot;",'"')\
                                              .replace("&apos;","'").replace("&gt;",">").replace("&lt;","<")
        requiredData = re.sub(' +',' ',requiredData)
        requiredData = re.sub('-+',' - ',requiredData)
        requiredData = requiredData.replace("[GRAPHIC]","-").replace("[TIFF OMITTED]","-")
        requiredData = re.sub('-+',' - ',requiredData)
        # requiredData=re.sub('-+','[GRAPHIC][TIFF OMITTED]',requiredData)
        requiredDataList = sent_tokenize(requiredData)
        # print "NUMBER OF SENTENCES ---> ",len(requiredDataList),"\n"
        line_count=len(requiredDataList)

        if line_count < 10:
            return {'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'PDF','STATUS':'FAIL'}
        else:
            es=Elasticsearch(ES_HOST)
            try:
                req =es.search(index='congressional',doc_type='hearings',body={"query":{"match_all":{}},"sort":{"SENTENCE_ID": "desc"},"size":1})
                #add one to the largest id 
                id =  req['hits']['hits'][0]['_source']['SENTENCE_ID'] + 1
            except:
                id=1

            if es_index_exist("congressional",Elasticsearch(ES_HOST)):
                req =es.search(index='congressional',doc_type='hearings',body={'query':{'match_phrase':{'TAG':{'query':tag,'type':'phrase'}}}},size=0)
                if req['hits']['total'] != line_count:
                    #Delete tag from index
                    es_delete_by_tag("congressional","hearings",tag,Elasticsearch(ES_HOST))
                    #Insert complete sentences into index
                    for each_sentence in requiredDataList:
                        data={'TAG': tag,'DATA': each_sentence, 'TITLE': title,'DATE':helddate}
                        es_insert("congressional","hearings",data,es,id)
                        id+=1
            else:
                #Insert complete sentences into index
                for each_sentence in requiredDataList:
                    data={'TAG': tag,'DATA': each_sentence, 'TITLE': title,'DATE':helddate}
                    es_insert("congressional","hearings",data,es,id)
                    id+=1
            return {'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'TEXT','STATUS':'SUCCESS'}

    except Exception as e:
        return {'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'ERROR','STATUS':'FAIL','ERROR':str(e),'URL':url}
    rurls.close()
