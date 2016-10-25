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
            mquery = {'query':{'match':{'TAG':{'query':itm['_source']['TAG'],'operator':'and'}}}}
            mdata=es_search(es,index,'metadata',query=mquery)
            metadata = ''
            if mdata['hits']['total']>0:
                metadata = mdata['hits']['hits'][0]['_source']
                title=metadata.get('title',None)
                congress=metadata.get('congress',None)
                chamber=metadata.get('chamber',None)
                committee = metadata.get('committee',None)
                member=[]
                for name in metadata.get('members',[]):
                    member.append(name.get('name',None))
                held_date= metadata.get('held_date',None)
                session=metadata.get('session',None)
                score =itm.get('_score',None)
                index = itm.get('_index',None)
                types= itm.get('_type',None)
            result.append({'TAG':itm['_source']['TAG'],'DATA':temp,'TITLE':title,'CONGRESS':congress,'CHAMBER':chamber,
                            'COMMITTEE':committee,'MEMBERS':member,'HELD_DATE':held_date, 'SESSION':session})        
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

def es_index_exist(esindex,es_client):
    es = es_client
    return es.indices.exists(index=esindex)

def es_delete_by_tag(esindex,estype,tag,es_client):
    es = es_client
    data=es_helper_scan(es,esindex,estype,{'query':{'match_phrase':{'TAG':{'query':tag,'type':'phrase'}}}},0)
    for item in data:
        es.delete(index=esindex,doc_type=estype,id=item['id'])

def es_delete_all(esindex,es_client):
    es = es_client
    es.indices.delete(index=esindex, ignore=[400, 404])

def es_insert(esindex,estype,data,es_client,id):
    es = es_client
    # try:
    #     temp=es.search(index=esindex, doc_type=estype, size=0)
    #     id_start=temp['hits']['total'] + 1
    # except:
    #     id_start=1
    data['SENTENCE_ID']=id
    es.index(index=esindex, doc_type=estype, id=id, body=data)


def es_add_chamber(esindex,estype,es_client):
    es = es_client
    data = helpers.scan(es_client,index=esindex,doc_type=estype,query={'query': {'match_all': {}}},preserve_order=True)
    for doc in data:
        if "hhrg" in doc['_source']['TAG']:
            es.update(index=esindex,doc_type=estype,id=doc['_id'],body={"doc":{"CHAMBER":"HOUSE"}})
        elif "shrg" in doc['_source']['TAG']:
            es.update(index=esindex,doc_type=estype,id=doc['_id'],body={"doc":{"CHAMBER":"SENATE"}})
        elif "jhrg" in doc['_source']['TAG']:
            es.update(index=esindex,doc_type=estype,id=doc['_id'],body={"doc":{"CHAMBER":"JOINT"}})