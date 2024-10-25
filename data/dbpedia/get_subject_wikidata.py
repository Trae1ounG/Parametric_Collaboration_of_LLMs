import json
from time import sleep

from tqdm import tqdm

from wikidataintegrator import wdi_core

import pandas as pd
def execute_sparql_query_with_limit(item, limit=10):
    all_results = None
    max_retries = 3
    _limit = 5000
    offset = 0
    while True:
        query_with_limit_offset = f"""SELECT DISTINCT ?subject  ?subjectLabel 
WITH {{
SELECT DISTINCT ?subject 
    WHERE {{
    # ?subject wdt:P118 wd:Q155223.
    # ?subject wdt:P54 wd:Q121783.
    ?subject wdt:P106 wd:Q82955.
    #  FILTER (?subject = wd:Q22686).
    }}
LIMIT {_limit}
OFFSET {offset}
      }} AS %SUB
WHERE{{
  INCLUDE %SUB
  ?subject rdfs:label ?subjectLabel.    FILTER(LANG(?subjectLabel) = "en").
}}
GROUP BY ?subject  ?subjectLabel 
"""
        query_with_limit_offset1 = f"""SELECT DISTINCT ?subject ?object ?subjectLabel ?objectLabel 
WITH {{
SELECT DISTINCT ?subject ?object
    WHERE {{
    #    ?subject wdt:{item} ?object.
    #    ?subject a wd:Q14211.
    #    ?subject rdfs:label ?object.
    ?subject wdt:P39 wd:Q11696.
    #  FILTER (?subject = wd:Q22686).
    }}
LIMIT {_limit}
OFFSET {offset}
      }} AS %SUB
WHERE{{
  INCLUDE %SUB
  ?subject rdfs:label ?subjectLabel.    FILTER(LANG(?subjectLabel) = "en").
  OPTIONAL{{?object rdfs:label ?objectLabel.}}  FILTER(LANG(?objectLabel) = "en").
}}
GROUP BY ?subject ?object ?subjectLabel ?objectLabel
"""
        query_with_limit_offset1=f"""
    SELECT DISTINCT ?subject ?object ?subjectLabel ?objectLabel (COUNT(DISTINCT ?r) AS ?relationCount) WITH{{
        SELECT DISTINCT ?subject ?object ?subjectLabel ?objectLabel 
        WHERE {{
        ?subject  wdt:P54 ?object. 
        OPTIONAL {{ ?subject rdfs:label ?subjectLabel . }}
        FILTER (LANG(?subjectLabel) = "en") .
        FILTER (?subject = wd:Q58590) #指定特定实体
        OPTIONAL {{  ?object rdfs:label ?objectLabel .}}
        FILTER (LANG(?objectLabel) = "en") .
        }}
        LIMIT 1000
    #     OFFSET 100
    }} AS %SUB
    WHERE {{
    INCLUDE %SUB
    ?subject ?r [].
    }}
    GROUP BY ?subject ?object ?subjectLabel ?objectLabel
        """
        result = wdi_core.WDItemEngine.execute_sparql_query(query_with_limit_offset, max_retries=max_retries)
        print(result["results"]["bindings"])
        print(len(result["results"]["bindings"]), end='\t')
        if all_results is None:
            all_results = result
        else:
            all_results['results']['bindings'].extend(result['results']['bindings'])
        print(len(all_results["results"]["bindings"]))

        if len(result["results"]["bindings"]) == 0 or len(all_results["results"]["bindings"]) > 300 or offset > 2000:
            print("break", len(all_results["results"]["bindings"]), len(all_results), offset)
            break
        offset += _limit
        break
    return all_results


res = execute_sparql_query_with_limit("P54")

df = pd.DataFrame(res['results']['bindings'])
ret = []
keys = df['subject'].keys()
for key in keys:
    subject, subject_lable = df['subject'][key], df['subjectLabel'][key]
    ret.append({
        "subject": subject['value'],
        "subjectLabel":subject_lable['value']
    })
with open('./politician_subject.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(ret, indent=4, ensure_ascii=False))
