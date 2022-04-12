import requests
from pyspark.sql import SparkSession
from termcolor import cprint

from question import Question
from EndPoint import EndPoint

import json
import logging
import operator
import requests
from itertools import chain, product, zip_longest
from collections import defaultdict

from pyspark.sql import SparkSession
from termcolor import cprint

from vertex import Vertex
from EndPoint import EndPoint

#helpers
def start_vertex_linking(question):
    entity_to_uris_names = []
    for entity in question.query_graph:
        if is_variable(entity):
            continue
        entity_query = make_keyword_unordered_search_query_with_type(entity, limit=400)
        # cprint(f"== SPARQL Q Find V: {entity_query}")

        try:
            uris, names = sparql_end_point.get_names_and_uris(entity_query)
            entity_to_uris_names.append((entity, uris, names))
        except:
            # cprint(f"Error at 'extract_possible_V_and_E' method with v_query value of {entity_query} ")
            continue
    return entity_to_uris_names

def start_predicate_linking(question):
    relation_to_uris_names = []
    data = []
    for (source, destination, key, relation) in question.query_graph.edges(data='relation', keys=True):
        if not relation:
            continue
        source_URIs = question.query_graph.nodes[source]['uris']
        destination_URIs = question.query_graph.nodes[destination]['uris']
        combinations = get_combination_of_two_lists(source_URIs, destination_URIs, with_reversed=False)

        uris, names = list(), list()
        for comb in combinations:
            if is_variable(source) or is_variable(destination):
                if is_variable(source):
                    uris, names = question.query_graph.nodes[destination]['vertex'].get_predicates()
                else:
                    uris, names = question.query_graph.nodes[source]['vertex'].get_predicates()
            else:
                URIs_false, names_false, URIs_true, names_true = [], [], [], []
                if len(source_URIs) > 0 and len(destination_URIs) > 0:
                    v_uri_1, v_uri_2 = comb
                    URIs_false, names_false = sparql_end_point.get_predicates_and_their_names(v_uri_1, v_uri_2,
                                                                                                   nlimit=limit_EQuery)
                    URIs_true, names_true = sparql_end_point.get_predicates_and_their_names(v_uri_2, v_uri_1,
                                                                                                 nlimit=limit_EQuery)
                if len(URIs_false) > 0 and len(URIs_true) > 0:
                    URIs_false = list(zip_longest(URIs_false, [False], fillvalue=False))
                    URIs_true = list(zip_longest(URIs_true, [True], fillvalue=True))
                    uris.extend(URIs_false + URIs_true)
                    names.extend(names_false + names_true)
                elif (len(URIs_false) > 0):
                    URIs_false = list(zip_longest(URIs_false, [False], fillvalue=False))
                    uris.extend(URIs_false)
                    names.extend(names_false)
                elif (len(URIs_true) > 0):
                    URIs_true = list(zip_longest(URIs_true, [True], fillvalue=True))
                    uris.extend(URIs_true)
                    names.extend(names_true)
        else:
            relation_to_uris_names.append((relation, uris, names))
            data.append(source, destination, key)
            return (relation_to_uris_names, data)


def is_variable(label):
    return 'var' in label

def make_keyword_unordered_search_query_with_type( keywords_string: str, limit=500):
    # for cases such as "Angela Merkel ’s"
    escape = ['’s']
    kwlist = []
    for w in keywords_string.strip().split():
        if w not in escape:
            if w.isnumeric():
                w = '\\\'' + w + '\\\''
                kwlist.append(w)
            else:
                kwlist.append(w)
    kws = ' AND '.join(kwlist)
    return f"prefix rdf: <http://www.w3.org/2000/01/rdf-schema#> " \
           f"select distinct ?uri  ?label " \
           f"where {{ ?uri rdf:label ?label. ?label  <bif:contains> '{kws}' . }}  LIMIT {limit}"

def parallel_computing_similarity(word, word_list):
    words_rdd = spark.sparkContext.parallelize(word_list)
    scores_rdd = words_rdd.map(lambda x: n_similarity(word.lower().split(), x.lower().split()))
    return scores_rdd.collect()

def n_similarity( mwe1, mwe2):
    mwe1 = ' '.join(mwe1)
    mwe2 = ' '.join(mwe2)

    data = {"word1": mwe1, "word2": mwe2}
    r = requests.post('http://206.12.95.86:5050/', json=data)
    return r.json()['similarity']

def get_combination_of_two_lists(list1, list2, directed=False, with_reversed=False):
    lists = [l for l in (list1, list2) if l]

    if len(lists) < 2:
        return set(chain(list1, list2))
    else:
        pass

    combinations = product(*lists, repeat=1)
    combinations_selected = list()
    combinations_memory = list()

    for comb in combinations:
        pair = set(comb)

        if len(lists) == 2 and len(pair) == 1:
            continue

        if not directed and pair in combinations_memory:
            continue
        combinations_memory.append(pair)
        combinations_selected.append(comb)
    else:
        if with_reversed:
            combinations_reversed = [(comb[1], comb[0]) for comb in combinations_selected if len(lists) == 2]
            combinations_selected.extend(combinations_reversed)

        return set(combinations_selected)

def remove_duplicates(sequence):
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]

def __get_chosen_URIs_for_relation(relation: str, uris: list, names: list):
    if not uris:
        return uris
    scores = parallel_computing_similarity(relation, names)
    l1, l2 = list(zip(*uris))
    URIs_with_scores = list(zip(l1, l2, scores))
    URIs_with_scores.sort(key=operator.itemgetter(2), reverse=True)
    return remove_duplicates(URIs_with_scores)[:max_Es]

max_Vs = 1
max_Es = 21
max_answers = 41
limit_VQuery = 600
limit_EQuery = 300

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
knowledge_graph = 'dbpedia'
sparql_end_point = EndPoint('dbpedia', 'http://206.12.95.86:8890/sparql')


file_name = r"qald-9.json"
with open(file_name) as f:
    qald9_testset = json.load(f)

question_filtered = []
for i, question in enumerate(qald9_testset['questions']):
    for language_variant_question in question['question']:
        if language_variant_question['language'] == 'en':
            question_text = language_variant_question['string'].strip()
            break
    question_filtered.append((question_text, question['id']))

question_text_with_index = spark.sparkContext.parallelize(question_filtered)
question_rdd = question_text_with_index.map(lambda x: Question(question_text=x[0], question_id=x[1]))
vertex_before = question_rdd.map(lambda x: (x, start_vertex_linking(x)))
# Calculate scores
vertex_before = vertex_before.collect()
for question in vertex_before:
    question_obj = question[0]
    for entity in question[1]:
        scores = parallel_computing_similarity(entity[0], entity[2])
        URIs_with_scores = list(zip(entity[1], scores))
        URIs_with_scores.sort(key=operator.itemgetter(1), reverse=True)
        URIs_sorted = []
        if len(list(zip(*URIs_with_scores))) > 0:
            URIs_sorted = list(zip(*URIs_with_scores))[0]

        updated_vertex = Vertex(max_Vs, URIs_sorted, sparql_end_point, limit_EQuery)
        URIs_chosen = updated_vertex.get_vertex_uris()
        question_obj.query_graph.nodes[entity[0]]['uris'].extend(URIs_chosen)
        question_obj.query_graph.nodes[entity[0]]['vertex'] = updated_vertex
print("Vertex Linking ended")
predicate_before = question_rdd.map(lambda x: (x, start_predicate_linking(x)))
predicate_before = predicate_before.collect()
for question in predicate_before:
    question_obj = question[0]
    for relation in question[1]:
        linking_result = relation[0]
        question_data = relation[1]
        URIs_chosen = __get_chosen_URIs_for_relation(linking_result[0], linking_result[1], linking_result[2])
        question.query_graph[question_data[0]][question_data[1]][question_data[2]]['uris'].extend(URIs_chosen)
print("Predicate Linking ended")





