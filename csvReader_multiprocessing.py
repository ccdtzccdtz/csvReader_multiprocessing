

import os
import multiprocessing


def linesproc(*arg):                                                           # change this to process each line
    # modify this fucntion
    lines,subarg=arg
    returnval=[]
    for line in lines:
        cells=line.split(",")
        returnval.append(cells[0])
    #print(returnval)
    return returnval

def subprocessor(*arg):
    cursor,end,inputfile,lineproc_args=arg
    with open(inputfile,"rb") as f:
        f.seek(cursor)
        lines = f.readlines(end - cursor)
        lines_text=[line.decode('utf-8').strip("\n") for line in lines ]        # decode binary to text
        returnval=linesproc(lines_text,lineproc_args)
    return returnval

def csvReader_multiprocessing(subprocessor,inputfile,cpu=4,split_size=1024*1024*100,subp_args=[]):
    filesize = os.path.getsize(inputfile)                                       # get the size of file in bytes
    results=[]
    cursor=0
    pool = multiprocessing.Pool(cpu)
    with open(inputfile,"rb") as f:
        for chunk in range(filesize//split_size+1):
            if cursor+split_size>filesize:
                end=filesize
            else:
                end=cursor+split_size
            end1=end
            f.seek(end1)
            f.readline()                                                        # move to the end of the a line
            end1=f.tell()
            f.readline()                                                        # move to the end of next line to avoid overlap
            end2=f.tell()
            proc=pool.apply_async(subprocessor,[cursor,end1,inputfile,subp_args])
            cursor=end2
            results.append(proc)
    pool.close()
    pool.join()
    return results

if __name__ == '__main__':
    linkdict=csvReader_multiprocessing(subprocessor,"DP2_path_links.CSV",cpu=4,split_size=1024*1024*10)

     # this returns a list of result from all processes. use item.get() to retrieve
    for item in linkdict:
        print(item.get())













    