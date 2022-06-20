import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import csv
import math
import time
import numpy as np
import collections
from mpi4py import MPI
import write_lib


def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

comm = MPI.COMM_WORLD
nproc = comm.Get_size()
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()
df = []
k = 0
m = 0

# ---------------------  parallel read from file by all ranks 
#CSV
lines=146862720   # for full dataset
k, m = divmod(lines, size)
#k,m = comm.bcast((k,m), root = 0)
panda_df = pd.read_csv('/home2/input/2013_data_full.csv',sep=',', header = None, skiprows=(rank) * k + min(rank, m), nrows=((rank + 1) * k + min(rank + 1, m))- (rank * k + min(rank, m)),low_memory=False)

# converting the panda dataframe to a list
df = panda_df.values.tolist()

timestart=MPI.Wtime()
out_filename = "temp_full_array.csv"
error = write_lib.write_all(out_filename, comm, panda_df)

timeend=MPI.Wtime()
if rank==0:
    print("execution time %f" %(timeend-timestart))
