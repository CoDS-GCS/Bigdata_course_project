from pyspark.sql import SparkSession

from question import Question
from linking import Linking
from EndPoint import EndPoint
from linking_parallel import ParallelLinking
import json


max_Vs = 1
max_Es = 21
max_answers = 41
limit_VQuery = 600
limit_EQuery = 300

questions = [
    "What is the revenue of IBM?",
    "Who founded Intel?",
    "What is Angela Merkel’s birth name?",
    "Where did Abraham Lincoln die?",
    "What is the profession of Frank Herbert?",
    "When did princess Diana die?",
    "Which movies did Kurosawa direct?",
    "Who was the wife of President Lincoln?",
    "Who does the voice of Bart Simpson?",
    "How many moons does Mars have?"
]
knowledge_graph = 'dbpedia'
sparql_end_point = EndPoint(knowledge_graph, 'http://206.12.95.86:8890/sparql')
linking = Linking(sparql_end_point, n_limit_EQuery=limit_EQuery, n_max_Es=max_Es, n_max_Vs=max_Vs)


def perform(question, index):
    parsed_question = Question(question_text=question, question_id=index)
    linking.extract_possible_V_and_E(parsed_question)
#
#
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# questions_rdd = spark.sparkContext.parallelize(questions)
# questions_with_index = questions_rdd.zipWithIndex()
# result = questions_with_index.map(lambda x: perform(x[0], x[1]))
# result.collect()

# for i, q in enumerate(questions):
#     parsed_question = Question(question_text=q, question_id=i)
#     linking.extract_possible_V_and_E(parsed_question)

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

question_with_index = spark.sparkContext.parallelize(question_filtered)
result = question_with_index.map(lambda x: perform(x[0], x[1]))
result.collect()
    # parsed_question = Question(question_text=question_text, question_id=question['id'])
    # linking.extract_possible_V_and_E(parsed_question)