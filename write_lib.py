# base code for write_all funtion 
# write_lib.py

# !/usr/bin/python
#import argparse
import pandas as pd
import numpy as np
from mpi4py import MPI

import io
import csv
import os
import array

def get_writer_comm(collective_rank_list, comm):
    rank = comm.Get_rank()
    size = comm.Get_size()
    for i, r in enumerate(collective_rank_list):
        if rank < r:
            receivers_list = [e for e in range(collective_rank_list[i-1], collective_rank_list[i])]
            #print(" I am rank ", rank, " receivers_list is: ",receivers_list)
            receive_size = len(receivers_list)
            return receivers_list,receive_size
        if rank == r:
            if i == (len(collective_rank_list)-1): # if rank is the (last chunk)'s reader
                receivers_list = [e for e in range(collective_rank_list[len(collective_rank_list)-1], size)]
            else:
                receivers_list = [e for e in range(collective_rank_list[i], collective_rank_list[i+1])]
            receive_size = len(receivers_list)
            #print(" I am rank ", rank, " receivers_list is: ",receivers_list)
            return receivers_list,receive_size
    receivers_list = [e for e in range(collective_rank_list[len(collective_rank_list)-1], size)]
    receive_size = len(receivers_list)
    #print(" I am rank ", rank, " receivers_list is: ",receivers_list)
    return receivers_list,receive_size
    
def get_writer_index(collective_rank_list,comm): 
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


def write_all(file_name, comm, panda_df):
    
    size = comm.Get_size()
    rank = comm.Get_rank()
    df_list = panda_df.values.tolist()
    df_arr = panda_df.to_numpy(dtype=np.str)
    #print('\nNumpy Array Datatype :', df_arr.dtype)
    
    df_arr = np.asarray(df_list, dtype='U20') # converting the list of lists on each process to the numpy array of type string 
    
    row, col = df_arr.shape
    sent_array_size = df_arr.size   # finding size of the numpy array
    #print("rank ",rank, " and df_arr is ", df_arr," \n and The number of dimensions of it is ", df_arr.ndim ,"\n and The size (= total number of elements) of the array is ", df_arr.size, "\n")
    
    # finding the maximum size of the numpy array to be received by each writer
    max_size_of_received_array_by_all_writers = comm.allreduce(row, op=MPI.MAX) 
    #print("I am rank ",rank, " and max_size_of_received_array_by_all_writers is ", max_size_of_received_array_by_all_writers)
    
    
    collective_writers_percent = 1                 # optimization work to find the best number of write processes
    num_collective_writers = int(size * collective_writers_percent)
    k, m = divmod(size, num_collective_writers)
    collective_writers_rank_list = [i * k + min(i, m)  for i in range(num_collective_writers)]
    
    sender_list,num_senders = get_writer_comm(collective_writers_rank_list, comm)
    writer_rank = get_writer_index(collective_writers_rank_list,comm)

    if rank not in set(collective_writers_rank_list):   # if the process is not a main writer, sends its list to its main writer
        
        # ------------------------------------------------
        comm.Send([df_arr, MPI.BYTE], dest = writer_rank, tag=12)
        # ------------------------------------------------
        #comm.send(df_list, dest = writer_rank, tag=12) 
        file_in_memory = io.StringIO()
        file_in_memory_size = 0                         # to skip the NON-writer processes in calculating the offset by "exscan" in the final output file 
        
    if rank in set(collective_writers_rank_list):       #if the process is a main writer
        
        # ------------------------------------------------
        MAX_LEN = max_size_of_received_array_by_all_writers
        received_df_arr = np.empty((MAX_LEN,col) ,dtype='U20')
        # ------------------------------------------------
        for i in sender_list:
            if i!= rank:                                # if i is NOT the writer, we should receive it's partial dataframe at the writer process 
                
                comm.Recv([received_df_arr, MPI.BYTE], source = i,tag=12)       # Receiving the numpy array
                df_list_partial = received_df_arr.tolist()                      # converting the numpy array to list 
                df_list_row_merged_for_writer.extend(df_list_partial)           # Accumulate data in a list, not a DataFrame, Lists also takes up less memory and are a much lighter data structure to work with,
                
            if i == rank:                               # if i is the writer itself, should add it's df to the final dataframe to be written by the writer process                
                df_list_row_merged_for_writer = df_list
        
        # In-memory text streams are available as StringIO objects:
        file_in_memory = io.StringIO()                 
        
        # we can pass StringIO() to the csv writer directly:
        csv.writer(file_in_memory).writerows(df_list_row_merged_for_writer)        
        
        # -------   Retrieving file size in Bytes  -------  

        pos = file_in_memory.tell()
        file_in_memory.seek(0, os.SEEK_END)
        file_in_memory_size = file_in_memory.tell()
        #print(" rank ", rank, " and Number of Bytes is ", file_in_memory_size) 
        file_in_memory.seek(pos)
        # -----------------------------------------------
    
    # Calculating the offset    
    offset = comm.exscan(file_in_memory_size)           # all processes will call exscan
    if rank ==0:
        offset = 0                                      # because the exscan return the recv buf for process 0 as "none" instead of zero
    else:
        offset = offset + 1
        
    # MPI-IO
    filename = file_name
    fh = MPI.File.Open(comm, filename, amode= MPI.MODE_CREATE | MPI.MODE_RDWR) # open the file for read and write, create it if it does not exist
    
    # Retrieve file contents - convert string to byte array 
    contents_buffer = bytearray()
    contents_buffer.extend(map(ord, file_in_memory.getvalue()))
    
    fh.Write_at_all(offset, contents_buffer)
    fh.Close()
    
    # Close object and discard memory buffer
    file_in_memory.close()
    return 1

if __name__ == '__main__':
    args = parser.parse_args()
    write_all(args.file_name, comm, args.panda_df)
