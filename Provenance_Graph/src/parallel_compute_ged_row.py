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


    n_training = len(training_dataset)
    n_dataset = len(graph_data)
    ged_matrix = torch.full((len(graph_data), len(graph_data)), float('inf'))

    def ged_distance_dask(i):
        start_time = time.time()
        g1 = graph_data[i]
        geds_sample = []
        ged_matrix_temp = torch.full((len(graph_data), len(graph_data)), float('inf'))
        if i < n_training:
            for j in range(i, n_training):
                g2 = graph_data[j]
                distance_beam, _, _ = graph_edit_distance(g1, g2, algorithm='beam', max_beam_size=2)
                distance_bipartite, _, _ = graph_edit_distance(g1, g2, algorithm='bipartite')
                distance_hausdorff, _, _ = graph_edit_distance(g1, g2, algorithm='hausdorff')
                distance = min(distance_beam, distance_bipartite, distance_hausdorff)
#                print("Distance between ", i, j," is:", distance)
                geds_sample.append((i, j, distance))
                ged_matrix_temp[i, j] = distance
                ged_matrix_temp[j, i] = distance
#                if j > 3:
#                    break
        else:
            for j in range(n_training):
                g2 = graph_data[j]
                distance_beam, _, _ = graph_edit_distance(g1, g2, algorithm='beam', max_beam_size=2)
                distance_bipartite, _, _ = graph_edit_distance(g1, g2, algorithm='bipartite')
                distance_hausdorff, _, _ = graph_edit_distance(g1, g2, algorithm='hausdorff')
                distance = min(distance_beam, distance_bipartite, distance_hausdorff)
#                print("Distance between ", i, j," is: ",distance)
                geds_sample.append((i, j, distance))
                ged_matrix_temp[i, j] = distance
#                if j > 3:
#                    break
        print("Done: ", i,"in %s seconds" % (time.time() - start_time))
        ged_file = "/shared_mnt/GnnDeepHunter/dataset/atlas/simgnn/hot_encoding/rgcn_exp/ged_"+str(i)+".pt"
        ensure_dir(ged_file)
        with open(ged_file, 'wb') as f:
            pickle.dump(ged_matrix_temp, f)
        ged_matrix_temp,g1,g2 = None,None,None
        return geds_sample

    list_indices = list(range(len(graph_data)))
#    list_indices = list(range(9))
#    list_indices.append(800)
    graph_dask = db.from_sequence(list_indices, npartitions=10)
    graph_GEDs = graph_dask.map(lambda x: ged_distance_dask(x)).compute()
    graph_GEDs = [sample for geds_sample in graph_GEDs for sample in geds_sample]
    for i, j, d in graph_GEDs:
        ged_matrix[i, j] = d
        if i < n_training:
            ged_matrix[j, i] = d

    ged_file = "/shared_mnt/GnnDeepHunter/dataset/atlas/simgnn/hot_encoding/rgcn_exp/ged_matrix.pt"
    ensure_dir(ged_file)
    with open(ged_file, 'wb') as f:
        pickle.dump(ged_matrix, f)
    print(ged_matrix)

    print("---Total Running Time : %s seconds ---" % (time.time() - start_running_time))


if __name__ == "__main__":
    main()
