from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
from elastic_search import es_get, es_search, es_helper_scan
from elasticsearch import Elasticsearch
import pandas as pd
import os
#Default base directory 
basedir="/data/web_data/static"
ES_HOST = [{'host':'esearch'}]

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
def search_stats(index,doctype,query,context_pages=5):
    task_id = str(search_stats.request.id)
    #create Result Directory
    resultDir = os.path.join(basedir, 'dsl_tasks/', task_id)
    os.makedirs(resultDir)
    #Get ES dataset
    result = es_retun_all(Elasticsearch(ES_HOST),query,index,doctype,context_pages)
    df = pd.DataFrame(result)
    #Save results to csv
    df.to_csv("{0}/es_query_data.csv".format(resultDir),index=False)
    #save dataframe pickle to file
    df.to_pickle("{0}/es_query_data.pkl".format(resultDir))
    return "http://dev.libraries.ou.edu/dsl_tasks/{0}".format(task_id)



def es_retun_all(es,query,index,doctype,context_pages):
    #meta = es_search(es, index, doctype, query=query, page=1, nPerPage=1)

    result = es_helper_scan(es,index,doctype,query,context_pages)

    return result
    
