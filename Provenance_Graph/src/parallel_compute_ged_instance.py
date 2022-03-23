from networkx.readwrite import json_graph
import time
import pickle
import torch
import pickle
import argparse
from ged import graph_edit_distance
import os
import dask.bag as db


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        
def combination_m(n_dataset,n_training):
    combined_list=[]
    for i in range(n_training):
        for j in range(i,n_training):
            combined_list.append((i,j))       
    for i in range(n_training,n_dataset):    
        for j in range(n_training):
            combined_list.append((i,j))
    return combined_list


def main():
    start_running_time = time.time()
    training_file = r"/shared_mnt/GnnDeepHunter/dataset/atlas/simgnn/hot_encoding/rgcn_exp/dgl/training_dataset.pt"  
    with open(training_file, 'rb') as f:
        training_dataset = pickle.load(f)
    testing_file = r"/shared_mnt/GnnDeepHunter/dataset/atlas/simgnn/hot_encoding/rgcn_exp/dgl/testing_dataset.pt"  
    with open(testing_file, 'rb') as f:
        testing_dataset = pickle.load(f)
    print("Training Samples", len(training_dataset))
    print("Testing Samples", len(testing_dataset))
    graph_data = training_dataset + testing_dataset
    
   
    combined_list = None
    n_training = len(training_dataset)
    n_dataset = len(graph_data)
    combined_list = combination_m(n_dataset,n_training)
    ged_matrix = torch.full((len(graph_data), len(graph_data)), float('inf'))

    def ged_distance_dask(i,j):
        start_time = time.time()
        distance_beam,_,_ = graph_edit_distance(graph_data[i], graph_data[j], algorithm='beam', max_beam_size=2)
        distance_bipartite, _, _ = graph_edit_distance(graph_data[i], graph_data[j], algorithm='bipartite')
        distance_hausdorff, _, _ = graph_edit_distance(graph_data[i], graph_data[j], algorithm='hausdorff')
        distance = min(distance_beam,distance_bipartite,distance_hausdorff)
        print("Done",i,j," in : %s seconds ---" % (time.time() - start_time))
        return i,j,distance

    graph_dask = db.from_sequence(combined_list, npartitions=10)
    graph_GEDs = graph_dask.map(lambda x : ged_distance_dask(x[0],x[1])).compute()
    for i,j,d in graph_GEDs:
        ged_matrix[i,j] = d
        if i < n_training:
            ged_matrix[j,i] = d

    ged_file = "/shared_mnt/GnnDeepHunter/dataset/atlas/simgnn/hot_encoding/rgcn_exp/ged_matrix.pt"
    ensure_dir(ged_file)
    with open(ged_file, 'wb') as f:
        pickle.dump(ged_matrix, f)

    print("---Total Running Time : %s seconds ---" % (time.time() - start_running_time))

if __name__ == "__main__":
    main()    
