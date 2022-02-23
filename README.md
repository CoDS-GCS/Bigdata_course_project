# Bigdata_course_project (Scalable Graph Analytics)
### Team 26: Ahmed Aly, Reham Omar, Ishika Dhall, Maria Manzoor


## Introduction:

The goal of this project is to search and implement the techniques needed to optimize the performance of workloads on graphs. Our project will be focussing on two kinds of workloads: 
1) Huge amount of lightweight workloads, where a huge number of requests are to be sent to a graph and these requests are lightweight processes, 
2) A few amounts of heavy workloads, where a few numbers of requests are to be sent to a graph but these requests will need heavy computations. 

These two types of workloads were initiated from the challenges we faced in two different applications. The first application relates to question answering on top of knowledge graphs where a question is given as an input to the system and the outputs are the answers extracted from the knowledge graph. A QA system generally consists of 3 modules that work together to form a pipeline where the first module parses the question to understand the key information, the second module maps the extracted information to vertices and edges in a knowledge graph and the last module creates a query, executes it and returns the answer to the user. In this pipeline, the second module is related to the first workload where mapping the key information from a question to the knowledge graph needs to perform multiple lightweight tasks. Currently, a performance issue needs to be solved wherein the average time taken to run the QA system on a benchmark is 4.5 hours (16 seconds per question). The second application relates to detecting hidden attacks in provenance graphs of kernel logs by getting similarities between 2 graphs: provenance and the attack query graph. Provenance graphs are huge and continuously growing graphs, so it requires time and memory efficiency. During our experimentations, we found that the average time taken for pre-processing a provenance graph of one day is 4 hours, and a provenance graph of 4 days with 1.25M nodes and 3.5M edges consumed around 43 GB RAM. The system crashes even before the completion of pre-processing. 

We aim to solve this by finding the answers to the following questions: 
1) What are the best scalability techniques to process graph datasets efficiently from time and memory perspectives?  
2) Can they be applied on graphs of different nature, or are they domain-specific?


## Datasets:

We will deal with two datasets:

1) Question Answering:
The dataset for this application is the DBpedia knowledge graph, It contains information extracted from Wikipedia pages and represented in a graph form. This graph describes 6.0 million entities, out of which 5.2 million are classified in a consistent ontology, including 1.5M persons, 810k places, 135k music albums, 106k films, etc. In order to evaluate the performance of the system on this graph, there are 2 benchmark datasets that contain the ground truth for answering questions on Dbpedia (QALD-9 and LCQUAD-1.0)

2) Provenance graphs:
The dataset for this application is DARPA TC 3. A provenance graph is a representation of kernel audit logs that capture all system events in the operating system. DARPA TC 3 consists of two weeks of kernel logs for five different operating systems (Linux 12, Linux 14, Windows, FreeBSD, Android). Logs contain simulated attacks hidden within many benign backgrounds. A provenance graph of one day may consist of 100K nodes on average.


## Model Design:

Both applications can be classified as supervised learning problems. The question answering benchmarks contain the questions with their answers and in the provenance graph application, the model is trained on labeled data with GED (Graph Edit Distance) as a target. The graph similarity takes pairs of graphs and predicts similarity scores between them ranging from 0 to 1, so it’s a regression model from that perspective.


## Algorithms:

1) For question answering application:

  a. Currently, the algorithms existing for handling the second module (that we are concerned with) tend to get the knowledge graph, create dictionaries from it. These dictionaries carry the information about vertices and predicates in the graph and then use these dictionaries to perform the mapping from the question to the KG.

  b. On the other hand, we are trying to bypass this indexing step. That's why we replaced the steps of building these dictionaries with trying to fetch only the information that we need from the knowledge graph. We do that by creating several simple queries to fetch the required vertices and then fetch the relations connected to them.

2) For Provenance graph application:

  a. We use the GNN (Graph Neural Network) algorithm to predict similarity, with the GCN (Graph Convolutional Networks) algorithm for node embedding. Besides, we aim to use the RGCN (RelationalGraph Convolutional Networks) algorithm and compare results between both embedding algorithms. 

  b. We will develop parallelized implementation to process graph pairs simultaneously. Then compare performance between the sequential and the parallelized models.  

## Evaluation metrics: 

For both applications, our most important metric will be the performance with respect to time and memory, but each application has its own evaluation metric to evaluate the correctness of the results
1) Question Answering: Precision, Recall, and F1 score
2) Provenance Graph:   Mean Square Error, Precision at k where k = (1,5,10,20)

