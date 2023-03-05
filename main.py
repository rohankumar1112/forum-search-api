try:
    from flask import Flask, jsonify, request
    import time
    import bson
    from bson import ObjectId
    from sentence_transformers import SentenceTransformer
    import json
    import os
    import uuid
    from pymongo import MongoClient
    from bson.objectid import ObjectId
    import time
    import pandas as pd
    import numpy as np
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
    from sentence_transformers import SentenceTransformer, util
except Exception as e:
    print("Some Modules are Missing :{}".format(e))

app = Flask(__name__)
# cors = CORS(app)
def get_database():
   CONNECTION_STRING = "mongodb+srv://emseccomandcenter:TUXnEN09VNM1drh3@cluster0.psiqanw.mongodb.net/?retryWrites=true&w=majority"
   client = MongoClient(CONNECTION_STRING)
   return client['Main_ScrappingTeamData']
db = get_database()
collection = db["ForumData"]

class Tokenizer(object):
    def __init__(self):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def get_token(self, documents):
        sentences  = [documents]
        sentence_embeddings = self.model.encode(sentences)
        _ = list(sentence_embeddings.flatten())
        encod_np_array = np.array(_)
        encod_list = encod_np_array.tolist()
        return encod_list
    
def forum_search(data):
    try:
      forums=[]
      for  i in data:
          document = collection.find_one(ObjectId(i))
          forums.append({'post':document['posts']})
      return forums
    except:
        return "Data Not Found"
def multi_col_search( search_text, start_index):
    es = Elasticsearch("http://68.183.89.124:9200",basic_auth=("elastic", "vvooquXVmXPMRLiXIMss"),timeout=600)
    index_name='main_scrapping_team_data'
    if len(search_text.split( ))<=2:

        col_list = ["cleaned_posts","entities","assets"]
        script_query = {
            "query":{
                "query_string": {
                    "query": search_text,
                    "fields": col_list
                }
            },
            "from": start_index
        }
        try:
          res = es.search(index=index_name, body=script_query)
          y = [x['_source'] for x in res['hits']['hits']]
          indexes = []
          print("result from ES" + str(len(y)))
          for result in y:
      #             indexes.append(result['Id'])
                  print(result['Id'])
                  first_part, second_part = result['Id'].split("-")
                  document = collection.find_one({"_id": ObjectId(first_part)})
                  
                  indexes.append({'id':first_part,"post_index":int(second_part),'thread_title':document['title'],'thread_url':document['url'],'author_name':result['author_name'],'assets': result['assets'] ,'post':document['posts'][int(second_part)]['post'],"post_count":len(document['posts'])})
                  
  
          return indexes
        except:
            return []
    else:
        helper_token = Tokenizer()
        token_vector = helper_token.get_token(search_text)

        query ={
          "query": {
            "script_score": {
              "query": {
                "match_all": {}
              },
              "script": {
                "source": "cosineSimilarity(params.query_vector, 'posts_vector') + 1.0",
                "params": {
                  "query_vector": token_vector
                }
              }
            }
          },
          "from": start_index
        }
        try:
          res = es.search(index=index_name, body=query)
          y = [x['_source'] for x in res['hits']['hits']]
          indexes = []
          for result in y:
  #             indexes.append(result['Id'])
              first_part, second_part = result['Id'].split("-")
              document = collection.find_one({"_id": ObjectId(first_part)})
                  
              indexes.append({'id':first_part,"post_index":int(second_part),'thread_title':document['title'],'thread_url':document['url'],'author_name':result['author_name'],'assets': result['assets'] ,'post':document['posts'][int(second_part)]['post'],"post_count":len(document['posts'])})
                  
  
          return indexes
        except:
            return []
@app.route('/search', methods=['POST'])
def search():
    search_text = request.json['search_text']
    page = int(request.json['page'])
    #page = request.json.get('page', default = 1, type = int)
    results = multi_col_search(search_text, (page-1) * 10)
    return jsonify({'results': results})

@app.route('/forumdata', methods=['POST'])
def search2():
    data = request.json.get('data', [])
    results =forum_search(data)
    return jsonify({'results': results})



if __name__=='__main__':
    app.run(port=5500,debug=True)  