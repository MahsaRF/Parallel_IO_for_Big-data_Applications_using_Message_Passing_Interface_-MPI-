import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
#---------------------------------
import csv
import math
import time
import numpy as np
import collections
from mpi4py import MPI

import read_csv
import read_parquet
def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

comm = MPI.COMM_WORLD
nproc = comm.Get_size()
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()
df = []
df_valid = []
df_invalid = []
res = []
k = 0
m = 0
lines=146862720  
timestart=MPI.Wtime()

# CSV read_all method

df = MPI_2.read_all('CSV_filename.csv', lines, comm, 10*rank)

# Parquet read_all method
df = read_parquet.read_all('part-00000-b80b8cf4-1e4a-4a01-8c5a-4e42ff787c79-c000.snappy.parquet', comm, 10*rank)

"""
filter the data to extract valid and invalid sets
"""

for row in df:
    #print("row is ", row)
    if row[7] == 'o3' and str(row[11]) == 'nan' and float(row[10]) > 0:
        df_valid.append(row)
    elif (row[7] == 'o3' and str(row[11]) == 'nan' and float(row[10]) < 0) or (row[7] == 'o3' and str(row[11]) != 'nan'):
        df_invalid.append(row)      

"""
group by
"""
histogram_valid = collections.defaultdict(list)
histogram_invalid = collections.defaultdict(int)

for row in df_valid:
    key, sensor_value, count = str(row[8]) + ',' + str(row[1]) + ',' + str(row[2]) +',' + str(row[3]), float(row[10]), 1
    if key not in histogram_valid:
        histogram_valid[key].append(sensor_value)
        histogram_valid[key].append(count)
    else:
        histogram_valid[key][0] += sensor_value
        histogram_valid[key][1] += count 

for row in df_invalid:
    key, count = str(row[8]) + ',' + str(row[1]) + ',' + str(row[2]) +',' + str(row[3]), 1
    histogram_invalid[key] += count        

if rank != 0: 
    try: 
        comm.send(histogram_valid,dest = 0,tag=11)
    except:
        pass
        
    try: 
        comm.send(histogram_invalid,dest = 0,tag=12)
    except:
        pass
     
else:
    """
    aggregate valid sets and update final result
    """
    for i in range(1, size):
        rec_entry = collections.defaultdict(list)
        rec_entry = comm.recv(source=i,tag=11)        
        for key, val in rec_entry.items():
           if key in histogram_valid:
               histogram_valid[key][0] += val[0]
               histogram_valid[key][1] += val[1]
           else:
               histogram_valid[key] = val
    #print("histogram_valid",histogram_valid)
    for key, val in histogram_valid.items():
        temp = []
        temp.append(key)
        temp.append((val[0]/val[1]))
        res.append(temp) 
    
    """
    aggregate invalid sets and update final result
    """
    for i in range(1, size):
        rec_entry = collections.defaultdict(int)
        rec_entry = comm.recv(source=i,tag=12)        
        for key, count in rec_entry.items():
           histogram_invalid[key] += count
    #print("histogram_invalid",histogram_invalid)
    for key, count in histogram_invalid.items():
       if count >= 12:
          temp = []
          temp.append(key)
          temp.append(-1)
          res.append(temp)

timeend=MPI.Wtime()

if rank ==0:
    final_res = []
    for v in res:
       temp = [int(e) for e in v[0].split(',')]
       temp.append(v[1])
       final_res.append(temp)
       
    #print("****************************")
    #print("final hourly average of O3 pollutant in year 2013:  ")
    #for r in final_res:
    #   print(r)
    print(len(final_res))
    #print("****************************")
   
if rank==0:
    print("execution time %f" %(timeend-timestart))
