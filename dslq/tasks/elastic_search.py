__author__ = 'mstacy'
import ast
import math
from elasticsearch import helpers
#import collections
#from rest_framework.templatetags.rest_framework import replace_query_param

def es_get(es_client, index, doc_type, ids=[]):
    es = es_client
    body = {"ids":ids}
    return es.mget(body,index,doc_type)

def es_search(es_client, index, doc_type, query=None, page=1, nPerPage=10): #, uri=''):
    es = es_client
    #setup es query params
    query = ast.literal_eval(str(query))
    count = es.search(index=index,doc_type=doc_type,size=0,body=query)['hits']['total']
    page,offset = find_offset(count,page,nPerPage)
    data = es.search(index=index,doc_type=doc_type,from_=offset,size=nPerPage, body=query)
    return data

def es_helper_scan(es_client,index,doc_type,query,context_pages):
    es = es_client
    #setup es query params
    query = ast.literal_eval(str(query))
    data = helpers.scan(es,index=index,doc_type=doc_type,query=query,preserve_order=True)
    result=[]
    for itm in data:
        if context_pages >0:
            ids = list(range(int(itm['_id'])-context_pages,int(itm['_id'])+context_pages+1))
            str_ids = [str(x) for x in ids]
            temp=''
            for item in es_get(es, index, doc_type, ids=str_ids)['docs']:
                if item['found']==True:
                    if item['_source']['TAG']== itm['_source']['TAG']:
                        temp=temp + item['_source']['DATA']
            result.append({'TAG':itm['_source']['TAG'],'DATA':temp})        
        else:
            result.append(itm)
    return result

def find_offset(count,page,nPerPage):
    max_page = math.ceil(float(count) / nPerPage)
    # Page min is 1
    if page < 1:
        page = 1
    #Change page to last page with data
    if page * nPerPage > count:
        page = int(max_page)
    #Cover count =0
    if page < 1:
        page = 1
    offset = (page - 1) * nPerPage
    return page,offset
