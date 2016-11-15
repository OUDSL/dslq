from datetime import datetime

from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from time import sleep
import requests
from nltk import sent_tokenize

from elastic_search import es_get, es_search, es_helper_scan,es_insert,es_delete_by_tag,es_delete_all,es_index_exist,es_add_chamber,es_helper_main_scan
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


#Default base directory 
basedir="/data/web_data/static"
ES_HOST = [{'host':'esearch'}]

mainURL="https://www.gpo.gov"
filterLinks=[]
s= requests.session()
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
def get_cong_data_python3(start="99",end="115"):
    """Python 3 script to pull congressional hearings from gpo website.
       args: 
       kwargs: start="99" - starting congressional session (string)
               end="115" - end congressional session default one more then wanted max 114 so default is 115 (string)
    """
    task_id = str(get_cong_data_python3.request.id)
    #create Result Directory
    resultDir = os.path.join(basedir, 'dsl_tasks/', task_id)
    os.makedirs(resultDir)
    call(["/anaconda3/gpo/mods.py",start,end,"{0}/log.txt".format(resultDir)])
    return "http://dev.libraries.ou.edu/dsl_tasks/{0}".format(task_id)
    
@task()
def get_congressional_data(congress=None, mongo_database="congressional",mongo_collection="hearings",update=None):
    """Congressional Hearing Inventory task
        Agrs: 
        kwargs: congress=<None> # This will run all congresses. Valid values is 99-114 to run individual 
                mongo_database=<'congressional'>, 
                mongo_collection=<'hearings'>, 
                update=<None> # this will run through all hearings
        If update = False will inventory entire congressional hearins. Must delete records in mongo. Task does not check of record exists.
    """
    # Session will work better to store connection state. Cookies!
    s = requests.Session()
    #load base page and setup session
    s.get("https://www.gpo.gov/fdsys/")
    url_template="http://www.gpo.gov/fdsys/search/search.action?sr={0}&originalSearch=collection:CHRG&st=collection:CHRG&ps=100&na=__congressnum&se=__{1}true&sb=dno&timeFrame=&dateBrowse=&govAuthBrowse=&collection=&historical=true"
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

@task()
def search_main_stats(index,doctype,query,context_pages=5):
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
    return "http://dev.libraries.ou.edu/dsl_main_tasks/{0}".format(task_id)

@task()
def index_data(inventory_option=None): # "inventory_option":"YES"
    db = MongoClient("dsl_search_mongo",27017)
    testURL="https://dev.libraries.ou.edu/api-dsl/data_store/data/congressional/hearings/?format=json"
    r = s.get(testURL)
    rjson = r.json()
    pagecount = rjson['meta']['pages']
    url_template="https://dev.libraries.ou.edu/api-dsl/data_store/data/congressional/hearings/.json?page={0}"

    if inventory_option:
        es_delete_all("congressional",Elasticsearch(ES_HOST))
        db.congressional.inventory.remove({})

    for i in range(1,pagecount+1):
        rd=requests.get(url_template.format(i))
        rdjson=rd.json()
        for item in rdjson['results']:
            if not inventory_option:
                if db.congressional.inventory.find({'TAG':item['TAG'],'STATUS':"FAIL"}).count()>0:
                    htmlparser(item)

                elif db.congressional.inventory.find({'TAG':item['TAG']}).count() == 0:
                    htmlparser(item)
            else:
                htmlparser(item)

@task()
def add_chamber():
    es_add_chamber("congressional","hearings",Elasticsearch(ES_HOST))

def es_retun_all(es,query,index,doctype,context_pages):
    #meta = es_search(es, index, doctype, query=query, page=1, nPerPage=1)

    result = es_helper_scan(es,index,doctype,query,context_pages)

    return result


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
    #print links
    valid_ids=[]
    for link in links:
        end_url=link.split('/')[-1]
        if re.match('^CHRG*',end_url):
            valid_ids.append(end_url.split('.p')[0])
    print "Total Valid Links: %d" % (len(valid_ids))
    return valid_ids

def get_ids(s,url_template,congress):
    cum_ids=[]
    for itm in range(1,1000):
        ids = get_chrg_ids(s,url_template,page=itm,congress=congress)
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
        db.congressional.srihearings.save(x)
    except:
        print "ERROR: %s %s" %  (tag,url) 


def htmlparser(x):
    db = MongoClient("dsl_search_mongo",27017)
    line_count=0
    rurls=requests.session()
    url=""
    tag=""
    try:
        if type(x['HELD_DATE'])== list:
            # print x['HELD_DATE'][1]
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


        # print "TITLE : ",title," DATE : ",h_date.upper()," URL : ",url,"TAG : ",tag
        if url=="":
            # print json.dumps({'TAG':tag,'LINE_COUNT': 'N/A','TYPE': 'PDF','STATUS':'FAIL'})
            db.congressional.inventory.save({'TAG':tag,'LINE_COUNT': 'N/A','TYPE': 'PDF','STATUS':'FAIL'})
            return


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
            # print json.dumps({'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'PDF','STATUS':'FAIL'})
            metadata=db.congressional.inventory.find_one({'TAG':tag})
            if metadata:
                metadata['LINE_COUNT'] = line_count
                metadata['TYPE']="PDF"
                metadata['STATUS']="FAIL"
                db.congressional.inventory.save(metadata)
            else:
                db.congressional.inventory.save({'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'PDF','STATUS':'FAIL'})

        else:
            es=Elasticsearch(ES_HOST)
            try:
                temp=es.count(index="congressional", doc_type="hearings")
                id=temp['count'] + 1
            except:
                id=1

            if es_index_exist("congressional",Elasticsearch(ES_HOST)):
                es_delete_by_tag("congressional","hearings",tag,Elasticsearch(ES_HOST))

            for each_sentence in requiredDataList:
                data={'TAG': tag,'DATA': each_sentence, 'TITLE': title,'DATE':helddate}
                es_insert("congressional","hearings",data,es,id)
                id+=1
                # print json.dumps({'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'TEXT','STATUS':'SUCCESS'})

            metadata=db.congressional.inventory.find_one({'TAG':tag})
            if metadata:
                metadata['LINE_COUNT'] = line_count
                metadata['TYPE']="TEXT"
                metadata['STATUS']="SUCCESS"
                db.congressional.inventory.save(metadata)
            else:
                db.congressional.inventory.save({'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'TEXT','STATUS':'SUCCESS'})

    except Exception as e:
        metadata=db.congressional.inventory.find_one({'TAG':tag})
        if metadata:
            metadata['LINE_COUNT'] = line_count
            metadata['TYPE']="ERROR"
            metadata['STATUS']="FAIL"
            metadata['ERROR']=str(e)
            metadata['URL']=url
            db.congressional.inventory.save(metadata)
        else:
            db.congressional.inventory.save({'TAG':tag,'LINE_COUNT': line_count,'TYPE': 'ERROR','STATUS':'FAIL','ERROR':str(e),'URL':url})
    rurls.close()
