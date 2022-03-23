def sparql_query_to_get_predicates_when_subj_and_obj_are_known(subj_uri, obj_uri, limit=1000):
    return f"select distinct ?p where {{ <{subj_uri}> ?p <{obj_uri}> . }}  LIMIT {limit}"

def make_top_predicates_sbj_query(uri, limit=1000):
    return f"select distinct ?p where {{ <{uri}> ?p ?o . }}  LIMIT {limit}"

def make_top_predicates_obj_query(uri, limit=1000):
    return f"select distinct ?p where {{ ?s ?p <{uri}> . }} LIMIT {limit}"