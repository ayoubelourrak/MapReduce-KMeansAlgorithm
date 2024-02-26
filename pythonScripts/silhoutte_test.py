from sklearn.metrics import silhouette_score
import numpy as np
import csv

D = [5, 7]
K = [7]
N = [100000]

for n in N:
    for d in D:
        for k in K:
            centroid_path = "finalLog/KMeans_"+str(n)+"_"+str(d)+"_"+str(k)+"_1_combiner.csv"
            data_path = "finalDataset/n_"+str(n)+"_d_"+str(d)+"_k_"+str(k)+"_cstd_0.75_iteration0_dataset_test.csv"
            
            # Leggi i dati dal file del dataset
            with open(data_path, 'r') as f:
                reader = csv.reader(f)
                dataset = np.array([row[0].split(';') for row in reader], dtype=float)

            # Leggi i centroidi dal file dei centroidi
            with open(centroid_path, 'r') as f:
                reader = csv.reader(f)
                centroids = np.array([row[0].split(';') for row in reader], dtype=float)

            # Assegna ogni punto al suo centroide più vicino
            distances = np.sqrt(((dataset[:, np.newaxis, :] - centroids) ** 2).sum(axis=2))
            cluster_labels = np.argmin(distances, axis=1)

            # Calcola il Silhouette Score
            score = silhouette_score(dataset, cluster_labels)
            print(f'Silhouette Score: {score}')

            with open("scores.txt", 'a') as f:
                f.write('\n')
                f.write(str(n)+"_"+str(d)+"_"+str(k)+" Silhouette Score: "+str(score)+"\n")
