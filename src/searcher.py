from elasticsearch import Elasticsearch


es = Elasticsearch()
INDEX = "wordtovec"
TYPE = "google_news"

def search_similar_word(word):

    query = {
      "query": {
        "match": {
          "word": word
        }
      }
    }

    res = es.search(index=INDEX, doc_type=TYPE, body=query)

    # print(res)

    try:
        item = res["hits"]["hits"][0]['_source']
        vector = item["vector"]
        print("FOUND %s"%item["word"])
    except:
        return None

    cosine_query = {
          "query": {
            "function_score": {
              "query": {
                "match_all": {}
              },
              "functions": [{
                "script_score": {
                  "script": {
                    "params": {
                      "vector" : vector,
                      "size" : 300
                    }
                    , "source": """
                        double s = 0;                                          
                        for(int i=0;i<params.size;i++){                                           
                            s += params.vector[i] * (doc['ordered_vector'][i]-i);  
                        }                   
                        return s;
                    """
                  }
                }
              }]
            }
          }
            ,"_source": ["id", "word"]
        }

    res = es.search(index=INDEX, doc_type=TYPE, body=cosine_query)
    return res

import sys
if __name__=="__main__":
    try:
        word = sys.argv[1]
    except:
        word = ""

    res = search_similar_word(word)
    try:
        items = res["hits"]["hits"]
        for item in items:
            score = item["_score"]
            word = item["_source"]["word"]
            id = item["_source"]["id"]
            print("%s\t%s\t%s"%(word, id, score))
    except:
        print("No similar words found")

