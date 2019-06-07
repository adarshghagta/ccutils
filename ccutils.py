#!/usr/bin/env python
# coding: utf-8

# In[327]:


import gzip
import json
import requests
from io import BytesIO,StringIO
import random
import re
import multiprocessing as mp
import http.client


# In[250]:


def is_mime_html(page):
    link = page['url']
    return all([
        page['mime'] == 'text/html'
    ])


# In[253]:


def get_pages(domain_key,cc_index,url_filter=is_mime_html,params={'output':'json'}):
    params["url"] = domain_key
    resp = requests.get('http://index.commoncrawl.org/CC-MAIN-%s-index'%cc_index,params=params)
    if resp.status_code != 200:
        raise Exception("Searching for this key failed with error code %s"%resp.status_code)
    else:
        pages = [json.loads(x) for x in resp.text.strip().split('\n')]
        pages = [x for x in pages if is_mime_html(x)]
    return pages


# In[196]:


def get_page_from_cc(filename,offset,offset_end):
    prefix = 'https://commoncrawl.s3.amazonaws.com/'
    try:
        resp = requests.get(prefix + filename, headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
        resp.raise_for_status()
    except:
        return None
    else:
        return resp


# In[308]:


def unzip_page(response):
    raw_data = BytesIO(response.content)
    f = gzip.GzipFile(fileobj=raw_data)
    warc, response = data.strip().decode().split('\r\n\r\n', 1)
    return {'warc':warc,'response':response}


# In[231]:


def process_page_links(page):
    offset, length = int(page['offset']), int(page['length'])
    offset_end = offset + length - 1
    response = get_page_from_cc(page['filename'],offset,offset_end)
    return {
        "url":page['url'],
        "response":response
    }


# In[341]:


def get_documents(pages,num_jobs):
    with mp.Pool(num_jobs) as pool:
        responses = pool.map(process_page_links,pages)
    return responses


# In[267]:


def unzip_pages(responses):
    output = {}
    for each in responses:
        output[each['url']] = unzip_page(each['response'])
    return output


# In[346]:


def download_pages(domain_key,cc_index,unzip,num_jobs=2,url_filter=is_mime_html,params={'output':'json'}):
    pages = get_pages(domain_key,cc_index,url_filter=is_mime_html,params={'output':'json'})
    unzipped_responses = get_documents(pages,num_jobs)
    if not unzip:
        return unzipped_responses
    return unzip_pages(unzipped_responses)


# In[ ]:




