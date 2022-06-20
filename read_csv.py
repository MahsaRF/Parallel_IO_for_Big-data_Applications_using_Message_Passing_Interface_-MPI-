# base code for read_all
# This is the script to read in parallel (I/O)

# !/usr/bin/python
#import argparse
import pandas as pd
import numpy as np
from mpi4py import MPI
def opt_max_sum_eachpartition(s_list, n_num_elements, k_num_partitons):
    
    k, m = divmod(len(s_list), k_num_partitons)   
    return max(sum(s_list[(i * k + min(i, m)):((i + 1) * k + min(i + 1, m))]) for i in range(k_num_partitons))
    
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
    for i, r in enumerate(collective_rank_list):
        if rank < r:
           return collective_rank_list[i-1]
        elif rank == r:
           return r
    return collective_rank_list[len(collective_rank_list)-1]   
    
def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def read_all(file_name, num_rows_csv, comm, num_rows):

    size = comm.Get_size()
    rank = comm.Get_rank()
    num_rows_perRank_list = comm.allgather(num_rows)
    df=[]
    df_perReceiver=[] # list to be returned by Read_all function
    collective_readers_percent = 0.5 #TODO: optimization work to find the best number of read processes
    num_collective_readers = int(size * collective_readers_percent)
    k_num_partitons = num_collective_readers
    n_num_elements = len(num_rows_perRank_list)
    max_sum = opt_max_sum_eachpartition(num_rows_perRank_list, n_num_elements, k_num_partitons)
    
    #-------------------------------------------------------------------------
    # can be done in rank zero and just bcast the "collective_readers_rank_list"
    #receivers_rank_list = []
    #receivers_rank_list = []
    temp_sum = 0
    collective_readers_rank_list = []
    for i, n_rows in enumerate(num_rows_perRank_list):
        if i==0:
            collective_readers_rank_list.append(i)
        temp_sum += n_rows    
        #if temp_sum <= max_sum:    
        #    receivers_rank_list.append(i) # keeping track of each partition's receivers rank number
        #    receivers_nrows_list.append(r) # keeping track of each partition's receivers requested number of rows
        if temp_sum > max_sum:
            #if i!= (size-1):
            collective_readers_rank_list.append(i)
            temp_sum = n_rows
            #collective_readers_receivers_list.append(receivers_rank_list)   # a list of list and len(collective_readers_receivers_list) = num_collective_readers 
            #collective_readers_receivers_list.append(receivers_rank_list)
            #temp_sum = 0
            #receivers_rank_list = []
    print("collective_readers_rank_list is ",collective_readers_rank_list)        
    if (len(collective_readers_rank_list) == num_collective_readers):
        print("len(collective_readers_rank_list) == num_collective_readers")
    else:
        print("len(collective_readers_rank_list) != num_collective_readers")
    #-------------------------------------------------------------------------    
    if rank in set(collective_readers_rank_list):
        idx = collective_readers_rank_list.index(rank)
        receivers_list,num_recievers = get_reader_comm(collective_readers_rank_list, comm)
        try:
            #-------------------------------------------------------------------------    
            skiprows_per_mainReader = 0
            for i in range(0, rank):
                skiprows_per_mainReader += num_rows_perRank_list[i]         # sum of the number of rows requested by all the ranks before the "rank" main reader
            #-------------------------------------------------------------------------    
            nrows_per_mainReader = 0
            if idx+1< len(collective_readers_rank_list):                    # if rank is not the last main reader!
                for i in range(rank, collective_readers_rank_list[idx+1]):  # counting the number of rows requested by each rank between the current main reader and next one, if there is any
                    nrows_per_mainReader += num_rows_perRank_list[i]
            else:                                                           # if rank is the last main reader!  
                for i in range(rank, size):                                 # counting the number of rows requested by last main reader rank until the last rank 
                    nrows_per_mainReader += num_rows_perRank_list[i]    
            #-------------------------------------------------------------------------    
            panda_df = pd.read_csv(file_name, sep=',', header=None,
                                   skiprows = skiprows_per_mainReader,
                                   nrows = nrows_per_mainReader, low_memory=False)
            df = panda_df.values.tolist()                                   # list of rows has been read by main reader "rank"
            print("I am rank", rank, "and num_recievers is ", num_recievers, " and len(df) is ", len(df))
            #-------------------------------------------------------------------------    
            #scattered_df = split(df, num_recievers)
            start_row = 0
            stop_row = 0
            for i,dest_rank in enumerate(receivers_list):
                num_rows_toBeRead_from_df = num_rows_perRank_list[dest_rank]
                stop_row = start_row +  num_rows_toBeRead_from_df    
                df_perReceiver = df[start_row:stop_row]
                if dest_rank!= rank:
                    comm.send(df_perReceiver, dest = dest_rank,tag=12)
                    print("from ", rank, " sending ", len(df_perReceiver),"rows to the rank ",dest_rank ,"\n")
                else:
                    df_forReader = df_perReceiver
                start_row = stop_row # check if it is correct
            
            df_perReceiver = df_forReader
            #print("Rank", rank," and I am main reader and and reading ",len(df_perReceiver), " rows from CSV \n")
        except:
            df_perReceiver = None
            print("Unable read from the file!")
    else:
        reader_rank = get_reader_index(collective_readers_rank_list,comm)
        df_perReceiver = comm.recv(source=reader_rank,tag=12) 
        #print("Rank ",rank, " and receiving df_perReceiver from ", reader_rank," and len(df_perReceiver) is ", len(df_perReceiver),"\n")
    return df_perReceiver

if __name__ == '__main__':
    args = parser.parse_args()
    read_all(args.file_name, args.num_rows_csv, comm)
