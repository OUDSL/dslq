from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
from elastic_search import es_get, es_search
from elasticsearch import Elasticsearch
import pandas as pd

#Default base directory 
basedir="/data/web_data/static"
ES_HOST = [{'host':'localhost'}]

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
def search_stats(index,doctype,query):
    task_id = str(search_stats.request.id)
    #create Result Directory
    resultDir = os.path.join(basedir, 'dsl_tasks/', task_id)
    os.makedirs(resultDir)
    #Get ES dataset
    data = es_retun_all(Elasticsearch(ES_HOST),query,index,doctype)
    df = pd.DataFrame(data['hits']['hits'])
    #Save results to csv
    df.to_csv("{0}/es_query_data.csv".format(resultDir)
    return "http://dev.libraries.ou.edu/dsl_tasks/{0}".format(task_id)



def es_retun_all(es,query,index,doctype):
    data = es_search(es, index, doctype, query=query, page=1, nPerPage=1)
    total_count = data['hits']['hits']['total']
    data = es_search(es, index, doctype, query=query, page=1, nPerPage=total_count)
    return data
    
