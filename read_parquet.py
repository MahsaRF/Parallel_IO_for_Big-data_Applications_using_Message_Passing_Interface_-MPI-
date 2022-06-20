# base code for read_all
# This is the script to read in parallel (I/O)

# !/usr/bin/python
#import argparse
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from fastparquet import ParquetFile
import fastparquet
import snappy
from mpi4py import MPI

def get_reader_comm(collective_rank_list, comm):
    rank = comm.Get_rank()
    size = comm.Get_size()
    for i, r in enumerate(collective_rank_list):
        if rank < r:
            receivers_list = [e for e in range(collective_rank_list[i-1], collective_rank_list[i])]
            print(" I am rank ", rank, " receivers_list is: ",receivers_list)
            receive_size = len(receivers_list)
            return receivers_list,receive_size
        if rank == r:
            if i == (len(collective_rank_list)-1): # if rank is the (last chunk)'s reader
                receivers_list = [e for e in range(collective_rank_list[len(collective_rank_list)-1], size)]
            else:
                receivers_list = [e for e in range(collective_rank_list[i], collective_rank_list[i+1])]
            receive_size = len(receivers_list)
            print(" I am rank ", rank, " receivers_list is: ",receivers_list)
            return receivers_list,receive_size
    receivers_list = [e for e in range(collective_rank_list[len(collective_rank_list)-1], size)]
    receive_size = len(receivers_list)
    print(" I am rank ", rank, " receivers_list is: ",receivers_list)
    return receivers_list,receive_size
    
def get_reader_index(collective_rank_list,comm): 
    rank = comm.Get_rank()
    size = comm.Get_size()
    for i, r in enumerate(collective_rank_list):
        if rank < r:
           return collective_rank_list[i-1]
        elif rank == r:
           return r
    return collective_rank_list[len(collective_rank_list)-1] 
    
def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def get_location(readers,comm):
    rank = comm.Get_rank()
    size = comm.Get_size()
    for i, e in enumerate(readers):
       for j, x in enumerate(e):
          if rank == x:
             return (i, j)
    return (-1, -1)
    
def read_all(file_name, comm, num_rows_perRank):
   
    size = comm.Get_size()
    rank = comm.Get_rank()
    num_rows_perRank_list=[]
    num_rows_perRank_list = comm.allgather(num_rows_perRank)
    df=[]
    #------------------------------------------------------------------
    pf = ParquetFile(file_name)
    
    parquet_file = pq.ParquetFile(file_name)
    num_columns = parquet_file.metadata.num_columns
    num_rows = parquet_file.metadata.num_rows
    num_rowgroups = parquet_file.metadata.num_row_groups
    num_column_chunks = num_columns * num_rowgroups
    
    if size >= num_column_chunks:
        num_rowgroup_master_readers = num_rowgroups  
        num_column_readers_perRowgroupReader = num_columns
    else:
        num_rowgroup_master_readers = num_rowgroups
    #------------------------------------------------------------------
    
    k, m = divmod(size, num_rowgroup_master_readers)
    row_group_readers_rank_list = [i * k + min(i, m)  for i in range(num_rowgroup_master_readers)]
    
    #  initialize a two-dimensional list
    column_chunck_readers_rank_list = [[0] * num_columns for i in range(num_rowgroups)]
    if size >= num_column_chunks:
    
        for i, master in enumerate(row_group_readers_rank_list):
            for j in range(num_columns):
                column_chunck_readers_rank_list[i][j] = master + j #[slave for slave in range(master, master + num_columns-1)]
    
    i, j = get_location(column_chunck_readers_rank_list,comm)            
    if i != -1 and j != -1:
        
        #-------------------------------------------------------------------------
        for iter,df in enumerate(pf.iter_row_groups(columns=[pf.columns[j]])):
            if iter == i:
                panda_df_rowgroup = df
        #-------------------------------------------------------------------------    
        if rank in set(row_group_readers_rank_list):                 # if the rank is the main row_group_reader and has to collect the partial columns from the slave column readers
            idx = row_group_readers_rank_list.index(rank)
          
            for k in column_chunck_readers_rank_list[idx]:
                if k != rank:
                    
                    panda_df_rec_column = comm.recv(source=k,tag=12)   
                    panda_df_rowgroup = panda_df_rowgroup.join(panda_df_rec_column)
                   
        else:
            comm.send(panda_df_rowgroup, dest = column_chunck_readers_rank_list[i][0],tag=12)
    
    if rank in set(row_group_readers_rank_list):
        
        row_groups_list = panda_df_rowgroup.values.tolist()
        receivers_list,num_recievers = get_reader_comm(row_group_readers_rank_list, comm)
        scattered_df = split(row_groups_list, num_recievers)
        for i in receivers_list:
            if i!= rank:
                idx = receivers_list.index(i)
                comm.send(scattered_df[idx], dest = i,tag=11)
                #print("from ", rank, " sending scattered_df to", i," and len(scattered_df) is ", len(scattered_df[idx]),"\n")
        idx = receivers_list.index(rank)
        scattered_df = scattered_df[idx]
            
    else:
        reader_rank = get_reader_index(row_group_readers_rank_list,comm)
        scattered_df = comm.recv(source=reader_rank,tag=11) 
        #print("I am rank ",rank, " and receiving scattered_df from ", reader_rank," and len(scattered_df) is ", len(scattered_df),"\n")
    
    return scattered_df

if __name__ == '__main__':
    
    read_all(file_name, comm, num_rows_perRank)
