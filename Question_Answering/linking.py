import logging
import operator
import requests
from itertools import chain, product, zip_longest
from collections import defaultdict
from termcolor import cprint

from vertex import Vertex
from EndPoint import EndPoint

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')

# logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('linking.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)
# logger.addHandler(file_handler)
# logger.setLevel(logging.INFO)
# logger.propagate = False


class Linking:
    def __init__(self,  sparql_end_point: EndPoint, n_limit_EQuery: int, n_max_Es: int = 10, n_max_Vs: int = 1):
        self.n_max_Vs = n_max_Vs
        self.n_max_Es = n_max_Es
        self.vertices = list()
        self.predicates_uris = list()
        self.predicates_names = list()
        self.sparql_end_point = sparql_end_point
        self.n_limit_EQuery = n_limit_EQuery
        self.v_uri_scores = defaultdict(float)

    def make_keyword_unordered_search_query_with_type(self, keywords_string: str, limit=500):
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

    @staticmethod
    def __compute_semantic_similarity_between_single_word_and_word_list(word, word_list):
        scores = list()
        score = 0.0
        for w in word_list:
            try:
                score = Linking.n_similarity(word.lower().split(), w.lower().split())
            except KeyError:
                score = 0.0
            finally:
                scores.append(score)
        else:
            return scores

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
        scores = self.__class__.__compute_semantic_similarity_between_single_word_and_word_list(relation, names)
        l1, l2 = list(zip(*uris))
        URIs_with_scores = list(zip(l1, l2, scores))
        URIs_with_scores.sort(key=operator.itemgetter(2), reverse=True)
        return self.remove_duplicates(URIs_with_scores)[:self.n_max_Es]

    def is_variable(self, label):
        return 'var' in label

    def get_vertex_uris(self):
        return self.vertices

    def extract_possible_V_and_E(self, question):
        for entity in question.query_graph:
            if self.is_variable(entity):
                continue
            entity_query = self.make_keyword_unordered_search_query_with_type(entity, limit=400)
            # cprint(f"== SPARQL Q Find V: {entity_query}")

            try:
                uris, names = self.sparql_end_point.get_names_and_uris(entity_query)
            except:
                # logger.error(f"Error at 'extract_possible_V_and_E' method with v_query value of {entity_query} ")
                continue

            scores = self.__compute_semantic_similarity_between_single_word_and_word_list(entity, names)

            URIs_with_scores = list(zip(uris, scores))
            URIs_with_scores.sort(key=operator.itemgetter(1), reverse=True)
            self.v_uri_scores.update(URIs_with_scores)
            URIs_sorted = []
            if len(list(zip(*URIs_with_scores))) > 0:
                URIs_sorted = list(zip(*URIs_with_scores))[0]


            updated_vertex = Vertex(self.n_max_Vs, URIs_sorted, self.sparql_end_point, self.n_limit_EQuery)
            URIs_chosen = updated_vertex.get_vertex_uris()
            question.query_graph.nodes[entity]['uris'].extend(URIs_chosen)
            question.query_graph.nodes[entity]['vertex'] = updated_vertex

            # Find E for all relations
        for (source, destination, key, relation) in question.query_graph.edges(data='relation', keys=True):
            if not relation:
                continue
            source_URIs = question.query_graph.nodes[source]['uris']
            destination_URIs = question.query_graph.nodes[destination]['uris']
            combinations = self.get_combination_of_two_lists(source_URIs, destination_URIs, with_reversed=False)

            uris, names = list(), list()
            for comb in combinations:
                if self.is_variable(source) or self.is_variable(destination):
                    if self.is_variable(source):
                        uris, names = question.query_graph.nodes[destination]['vertex'].get_predicates()
                    else:
                        uris, names = question.query_graph.nodes[source]['vertex'].get_predicates()
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
                question.query_graph[source][destination][key]['uris'].extend(URIs_chosen)
        # else:
        #     logger.info(f"[GRAPH NODES WITH URIs:] {question.query_graph.nodes(data=True)}")
        #     logger.info(f"[GRAPH EDGES WITH URIs:] {question.query_graph.edges(data=True)}")
