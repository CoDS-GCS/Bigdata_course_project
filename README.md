# Project Summary 
### Scalable Graph Analytics
### Team 26: Ahmed Aly, Reham Omar, Ishika Dhall, Maria Manzoor

## Introduction

The goal of this project is to search and implement the techniques required to optimize the performance of workloads on graphs. Our project will be focussing on two kinds of workloads: 
1) Huge amount of lightweight workloads, where a huge number of requests are to be sent to a graph and these requests are lightweight processes.
2) Few amounts of heavy workloads, where a few numbers of requests are to be sent to a graph but these requests will need heavy computations. 

These two types of workloads can be investigated with two different applications. The first application is question answering over knowledge graphs. QA system pipeline consists of 3 modules where the first module parses the question for question understanding, the second module maps the extracted information to vertices and edges in a knowledge graph and the last module creates a query and executes it. The second module relates to the first workload where mapping the information from a question to the knowledge graph requires multiple lightweight processes. Currently, the system is not efficient as the average time taken to run the system on a benchmark is 4.5 hours (16 seconds per question). The second application relates to detecting hidden attacks in provenance graphs of kernel logs by getting similarities between 2 graphs: provenance and attack query graph. Provenance graphs are huge and fast-growing, so they require time and memory efficiency. The average time taken for pre-processing a provenance graph of one day is around 4 hours, and a provenance graph of 4 days with 1.25M nodes and 3.5M edges consumed around 43 GB RAM. The system crashes even before the completion of pre-processing. 

## Datasets:

We will deal with two datasets:

1) Question Answering:
The dataset is the DBpedia knowledge graph which contains information extracted from Wikipedia pages represented in graph form. This graph describes 6 million entities, where 5.2 million are classified in a consistent ontology, including 1.5M persons, 810k places, 135k music albums, etc. To evaluate the system's performance on this graph, 2 benchmark datasets containing the ground truth for questions on Dbpedia (QALD-9 and LCQUAD-1.0) will be used.

2) Provenance graphs:
The dataset is DARPA TC 3. A provenance graph is a representation of kernel audit logs that capture all system events in the operating system. DARPA TC 3 consists of two weeks of kernel logs for five different operating systems (Linux 12, Linux 14, Windows, FreeBSD, Android). Logs contain simulated attacks hidden within many benign backgrounds. A provenance graph of one day may consist of 100K nodes on average. The provenacne graph is NetworkX multiDiGraph property graph which consist of three types of nodes (Process, File, IP Address) and four edge types (Access, Launch, Write, Connect To) with various types of attributes such as {command line, file name, full path, hashes, extension}.

## Model Design:

Both applications can be classified as supervised learning problems. The question answering benchmarks contain the questions with their answers. In the provenance graph application, the model is trained on labeled data with GED (Graph Edit Distance) as a target. The graph similarity takes pairs of graphs and predicts similarity scores between them ranging from 0 to 1, so it’s a regression model from that perspective.

## Algorithms:

1) For question answering:

a) The first module uses a fine-tuned BART model (Bidirectional and Auto-Regressive Transformer) for question understanding. The output of this module is then given to the second module for linking.

b) For the second module, the existing algorithms tend to create dictionaries about vertices and predicates which is used to map the question to the knowledge graph. However, we try to skip this step by fetching the required vertices and the connected relationships from the knowledge graph using several simple queries which will be parallelized.

2) For Provenance graph:

a) We use the GNN (Graph Neural Network) algorithm to predict similarity, with the GCN (Graph Convolutional Networks) algorithm for node embedding. Besides, we aim to use the RGCN (Relational Graph Convolutional Networks) algorithm and compare results between both embedding algorithms. 

b) We will develop parallelized implementation to process graph pairs simultaneously. Then compare performance between the sequential and the parallelized models.  

## Evaluation metrics: 

For both applications, our most important metric would be the system's performance with respect to time and memory. Also, each application has its own evaluation metric to evaluate the correctness of the results:
1) Question Answering: Precision, Recall, and F1 score
2) Provenance Graph:   Mean Square Error, Precision at k where k = (1,5,10,20)

## Research Questions
The questions we aim to answer:
1) What are the best scalability techniques to process graph datasets efficiently from time and memory perspectives?  
2) Can they be applied on graphs of different nature, or are they domain-specific?
