import operator
from collections import defaultdict
from itertools import chain, product, zip_longest
from termcolor import cprint

import requests
from pyspark.sql import SparkSession

from EndPoint import EndPoint
from vertex import Vertex


class ParallelLinking:
    def __init__(self,  sparql_end_point: EndPoint, n_limit_EQuery: int, n_max_Es: int = 10, n_max_Vs: int = 1):
        self.n_max_Vs = n_max_Vs
        self.n_max_Es = n_max_Es
        self.vertices = list()
        self.predicates_uris = list()
        self.predicates_names = list()
        self.sparql_end_point = sparql_end_point
        self.n_limit_EQuery = n_limit_EQuery
        self.v_uri_scores = defaultdict(float)
        self.question = ''
        self.spark = self.init_spark()

    def make_keyword_unordered_search_query_with_type(self, keywords_string: str, limit=500):
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

    def get_combination_of_two_lists(self, list1, list2, directed=False, with_reversed=False):
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

    # This will parallelize similarity calculation
    @staticmethod
    def __compute_semantic_similarity_between_single_word_and_word_list(word, word_list):
        scores = list()
        score = 0.0
        for w in word_list:
            try:
                score = ParallelLinking.n_similarity(word.lower().split(), w.lower().split())
            except KeyError:
                score = 0.0
            finally:
                scores.append(score)
        else:
            return scores

    @staticmethod
    def parallel_computing_similarity(word, word_list, spark):
        words_rdd = spark.sparkContext.parallelize(word_list)
        scores_rdd = words_rdd.map(lambda x: ParallelLinking.n_similarity(word.lower().split(), x.lower().split()))
        return scores_rdd.collect()

    @staticmethod
    def n_similarity( mwe1, mwe2):
        mwe1 = ' '.join(mwe1)
        mwe2 = ' '.join(mwe2)

        data = {"word1": mwe1, "word2": mwe2}
        r = requests.post('http://206.12.95.86:5050/', json=data)
        return r.json()['similarity']

    def remove_duplicates(self, sequence):
        seen = set()
        return [x for x in sequence if not (x in seen or seen.add(x))]

    def __get_chosen_URIs_for_relation(self, relation: str, uris: list, names: list):
        if not uris:
            return uris
        # scores = self.__class__.parallel_computing_similarity(relation, names, self.spark)
        scores = self.__class__.__compute_semantic_similarity_between_single_word_and_word_list(relation, names)
        l1, l2 = list(zip(*uris))
        URIs_with_scores = list(zip(l1, l2, scores))
        URIs_with_scores.sort(key=operator.itemgetter(2), reverse=True)
        return self.remove_duplicates(URIs_with_scores)[:self.n_max_Es]


    def init_spark(self):
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        return spark

    def is_variable(self, label):
        return 'var' in label

    # TODO: what if we have multiple vertices
    def vertex_linking(self, entity):
        if self.is_variable(entity):
            return
        entity_query = self.make_keyword_unordered_search_query_with_type(entity, limit=400)
        # cprint(f"== SPARQL Q Find V: {entity_query}")
        try:
            uris, names = self.sparql_end_point.get_names_and_uris(entity_query)
        except:
            # logger.error(f"Error at 'extract_possible_V_and_E' method with v_query value of {entity_query} ")
            return

        # TODO parallelize similarity calculation
        # scores = self.parallel_computing_similarity(entity, names, self.spark)
        scores = self.__compute_semantic_similarity_between_single_word_and_word_list(entity, names)

        URIs_with_scores = list(zip(uris, scores))
        URIs_with_scores.sort(key=operator.itemgetter(1), reverse=True)
        self.v_uri_scores.update(URIs_with_scores)
        URIs_sorted = []
        if len(list(zip(*URIs_with_scores))) > 0:
            URIs_sorted = list(zip(*URIs_with_scores))[0]

        # The 2 predicate queries are executed here
        updated_vertex = Vertex(self.n_max_Vs, URIs_sorted, self.sparql_end_point, self.n_limit_EQuery)
        URIs_chosen = updated_vertex.get_vertex_uris()
        return URIs_chosen, updated_vertex
        # self.question.query_graph.nodes[entity]['uris'].extend(URIs_chosen)
        # self.question.query_graph.nodes[entity]['vertex'] = updated_vertex

    # def parallel_similarity(self, comb, source, destination, source_URIs, destination_URIs):
    #     if self.is_variable(source) or self.is_variable(destination):
    #         if self.is_variable(source):
    #             uris, names = self.question.query_graph.nodes[destination]['vertex'].get_predicates()
    #         else:
    #             uris, names = self.question.query_graph.nodes[source]['vertex'].get_predicates()
    #     else:
    #         URIs_false, names_false, URIs_true, names_true = [], [], [], []
    #         if len(source_URIs) > 0 and len(destination_URIs) > 0:
    #             v_uri_1, v_uri_2 = comb
    #             URIs_false, names_false = self.sparql_end_point.get_predicates_and_their_names(v_uri_1, v_uri_2,
    #                                                                                            nlimit=self.n_limit_EQuery)
    #             URIs_true, names_true = self.sparql_end_point.get_predicates_and_their_names(v_uri_2, v_uri_1,
    #                                                                                          nlimit=self.n_limit_EQuery)
    #         if len(URIs_false) > 0 and len(URIs_true) > 0:
    #             URIs_false = list(zip_longest(URIs_false, [False], fillvalue=False))
    #             URIs_true = list(zip_longest(URIs_true, [True], fillvalue=True))
    #             uris.extend(URIs_false + URIs_true)
    #             names.extend(names_false + names_true)
    #         elif (len(URIs_false) > 0):
    #             URIs_false = list(zip_longest(URIs_false, [False], fillvalue=False))
    #             uris.extend(URIs_false)
    #             names.extend(names_false)
    #         elif (len(URIs_true) > 0):
    #             URIs_true = list(zip_longest(URIs_true, [True], fillvalue=True))
    #             uris.extend(URIs_true)
    #             names.extend(names_true)

    def predicate_linking(self, input):
        (source, destination, key, relation) = input
        if not relation:
            return
        source_URIs = self.question.query_graph.nodes[source]['uris']
        destination_URIs = self.question.query_graph.nodes[destination]['uris']
        combinations = self.get_combination_of_two_lists(source_URIs, destination_URIs, with_reversed=False)

        uris, names = list(), list()
        for comb in combinations:
            if self.is_variable(source) or self.is_variable(destination):
                if self.is_variable(source):
                    uris, names = self.question.query_graph.nodes[destination]['vertex'].get_predicates()
                else:
                    uris, names = self.question.query_graph.nodes[source]['vertex'].get_predicates()
            else:
                URIs_false, names_false, URIs_true, names_true = [], [], [], []
                if len(source_URIs) > 0 and len(destination_URIs) > 0:
                    v_uri_1, v_uri_2 = comb
                    URIs_false, names_false = self.sparql_end_point.get_predicates_and_their_names(v_uri_1, v_uri_2,
                                                                                                   nlimit=self.n_limit_EQuery)
                    URIs_true, names_true = self.sparql_end_point.get_predicates_and_their_names(v_uri_2, v_uri_1,
                                                                                                 nlimit=self.n_limit_EQuery)
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
            URIs_chosen = self.__get_chosen_URIs_for_relation(relation, uris, names)
            return URIs_chosen
            # self.question.query_graph[source][destination][key]['uris'].extend(URIs_chosen)

    def extract_possible_V_and_E(self, question):
        self.question = question
        # spark = self.init_spark()
        nodes = self.question.query_graph
        entities_rdd = self.spark.sparkContext.parallelize(nodes)
        vertices = entities_rdd.map(lambda x: (x, self.vertex_linking(x)))
        result = vertices.collect()
        for entry in result:
            if entry[1] is not None:
                entity = entry[0]
                self.question.query_graph.nodes[entity]['uris'].extend(entry[1][0])
                self.question.query_graph.nodes[entity]['vertex'] = entry[1][1]
        edges = self.question.query_graph.edges(data='relation', keys=True)
        edges_rdd = self.spark.sparkContext.parallelize(edges)
        predicates = edges_rdd.map(lambda x: (x, self.predicate_linking(x)))
        result = predicates.collect()
        for entry in result:
            self.question.query_graph[entry[0][0]][entry[0][1]][entry[0][2]]['uris'].extend(entry[1])
        cprint(f"[GRAPH NODES WITH URIs:] {self.question.query_graph.nodes(data=True)}")
        cprint(f"[GRAPH EDGES WITH URIs:] {self.question.query_graph.edges(data=True)}")
