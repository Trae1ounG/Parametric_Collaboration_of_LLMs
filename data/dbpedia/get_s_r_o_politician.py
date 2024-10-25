import json
from time import sleep

from tqdm import tqdm

from wikidataintegrator import wdi_core

import pandas as pd
def execute_sparql_query_with_limit(subject, relations, limit=10):
    all_results = None
    max_retries = 3
    _limit = 500
    offset = 0
    relations = " ".join(relations)
    while True:
        query_with_limit_offset = f"""SELECT DISTINCT ?subject ?object ?predicate ?subjectLabel ?objectLabel 
WITH {{
SELECT DISTINCT ?subject ?predicate ?object
    WHERE {{
       ?subject ?predicate ?object.
    #    FILTER(?subject = wd:{subject}).
    VALUES ?subject {{{subject}}}.
    VALUES ?predicate {{ {relations} }}.
    }}
LIMIT {_limit}
OFFSET {offset}
      }} AS %SUB
WHERE{{
  INCLUDE %SUB
  ?subject rdfs:label ?subjectLabel.    FILTER(LANG(?subjectLabel) = "en").
  OPTIONAL{{?object rdfs:label ?objectLabel.}}  FILTER(LANG(?objectLabel) = "en").
}}
GROUP BY ?subject ?object ?predicate ?subjectLabel ?objectLabel
"""
        print(query_with_limit_offset)
        try:
            result = wdi_core.WDItemEngine.execute_sparql_query(query_with_limit_offset, max_retries=max_retries, retry_after=2)
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
        # break
        except Exception as e:
            print(e)
            continue
        break
    return all_results["results"]["bindings"]


# res = execute_sparql_query_with_limit("P54")
# print(res)
def get_relation_by_id(r_id):
    r = relation[r_id]
    return r['wikidata_describe_en']['relation_label'], r['qa_query'], r['fill_query'], r['completion_query']
with open("./relation.json", "r", encoding="utf-8") as f:
    relation = json.load(f)
theme = "politician"
with open(f"./{theme}_subject.json", "r", encoding="utf-8") as f:
    subject = json.load(f)
relations = ["wdt:"+k for k in relation.keys()]
ret = []
# subs = subject[:5]
# subs = " ".join(["wd:"+ s['subject'].split("/")[-1] for s in subs])
# print(execute_sparql_query_with_limit(subs, relations[:10],))


# import sys; sys.exit(0)
for idx in range(3127, len(subject), 20):
    # if idx < 1268:
        # continue
    subs = subject[idx:idx+20]
    s_names = []
    subs_dict = {}
    for s in subs:
        s_name, s_label = s['subject'].split("/")[-1], s['subjectLabel']
        s_names.append(s_name)
        s_dict = {"subject" : s_label, "subjectProp" : s['subject']}
        s_dict["objectList"] = []
        s_dict['qa_prompts'] = []
        s_dict['fill_prompts'] = []
        s_dict['completion_prompts'] = []
        subs_dict[s_name] = s_dict
    for i in range(0, len(relations), 10):
        res = execute_sparql_query_with_limit(" ".join(["wd:"+s for s in s_names]), relations[i: min(len(relations), i + 10)])
        print(f"第{i}次查询结束！")
        for item in res:
            s_, o_, r_, s_label, o_label = item['subject'], item['object'], item['predicate'], item['subjectLabel'], item['objectLabel']
            s_value, o_value = s_label['value'], o_label['value']
            r_uri = r_['value']
            s_id, r_id = s_['value'].split('/')[-1], r_uri.split('/')[-1]
            r_item = get_relation_by_id(r_id)
            d = {
                "object": o_value,
                "objectProp": o_['value'],
                "relationDesc": r_item[0]
            }
            subs_dict[s_id]["objectList"].append(d)
            qa_querys, fill_querys, completion_querys = r_item[1:]
            qa_prompts = [q.replace("{}", s_value) for q in qa_querys]
            fill_prompts = [q.replace("{subject}", s_value).replace("{object}","{}") for q in fill_querys]
            completion_prompts = [q.replace("{}", s_value) for q in completion_querys]
            subs_dict[s_id]["qa_prompts"].extend([{"prompt":p,"ground_truth":o_value} for p in qa_prompts])
            subs_dict[s_id]["fill_prompts"].extend([{"prompt":p,"ground_truth":o_value} for p in fill_prompts])
            subs_dict[s_id]["completion_prompts"].extend([{"prompt":p,"ground_truth":o_value} for p in completion_prompts])
    for s_name in s_names:
        ret.append(subs_dict[s_name]) 
    with open(f"./{theme}_s_r_o_1.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(ret, indent=4, ensure_ascii=False))
    print(f"写入第{idx}个subject")