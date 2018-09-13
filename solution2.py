import multiprocessing as mp,os
import threading
from classes.cache import LRUCache
import pdb

numProcess = 2

#Read part of file, save the results in a dictionary
def parseFile(filePath, chunkStart, chunkSize, index, result):
    with open(filePath) as f:
        f.seek(chunkStart)
        lines = f.read(chunkSize).splitlines()
        result[index] =  lines


#Break the file to equal parts each part contain x lines of the file
def chunkify(filePath,size=1024*2):
    fileEnd = os.path.getsize(filePath)
    with open(filePath,'rb') as f:
        chunkEnd = f.tell()
        index = 1
        while True:
            chunkStart = chunkEnd
            f.seek(size,1)
            f.readline()
            chunkEnd = f.tell()
            yield chunkStart, chunkEnd - chunkStart, index 
            if chunkEnd > fileEnd:
                break
            index=index+1

#Manage the procedure of breaking and reading the file + sort the results
def readFile(filePath):
    #init objects
    pool = mp.Pool(numProcess)
    jobs = []
    manager   = mp.Manager()
    fileContent = manager.dict()
    #create jobs
    for chunkStart,chunkSize,index in chunkify(filePath):
        jobs.append( pool.apply_async(parseFile,(filePath, chunkStart, chunkSize, index,fileContent)) )
    #wait for all jobs to finish
    for job in jobs:
        job.get()
    #clean up
    pool.close()  
    #sort the file by line number
    result = []
    lineNum= 0
    for i in range(len(fileContent)):
        for j in range(len(fileContent[i+1])):
            result.append(fileContent[i+1][j])
            lineNum=lineNum+1
    
    return result

#Get set of rows form parsed file
def getRows(parseFile, numberOfLines, offset, reverse):
    result = []
    if reverse:
        fromLine=offset-numberOfLines
        toLine  =offset
    else:
        fromLine=offset-1
        toLine  =offset+numberOfLines-1
    for i in range(fromLine, toLine):
        result.append(parseFile[i])

    return list(reversed(result)) if reverse else result



def fetchLogs(logFilePath, numberOfLines, offset, reverse=False):
    if numberOfLines > 100: 
        print("You can read until 100 rows.")
        return
    fileContent = chache.get(logFilePath)
    if fileContent is None:
        parsedFile  = readFile(logFilePath)
        chache.set(logFilePath, parsedFile)      

    return getRows(parsedFile, numberOfLines, offset, reverse)


if __name__ == '__main__':
    chache = LRUCache(10)
    print(fetchLogs('data/test.txt', 10, 30, True))

 

