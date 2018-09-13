import itertools
import pdb
import multiprocessing as mp,os
from time import sleep
from classes.cache   import LRUCache

numProcess = 2

#Add line to dictionary
def parseFile(numAndLine, outList):
    lineNo, line = numAndLine
    if line == None:
        return   
    outList[lineNo] = line 


#Manage the procedure reading the file and collecting the results
def readFile(filePath):
    #init objects
    pool = mp.Pool(numProcess)
    jobs = []
    manager   = mp.Manager()
    fileContent = manager.dict()
    #create jobs
    with open(filePath) as f:
        iters = itertools.chain(f)
        for numAndLine in enumerate(iters):
        	jobs.append( pool.apply_async(parseFile,(numAndLine,fileContent)) )
    #wait for all jobs to finish
    for job in jobs:
        job.get()
    #clean up
    pool.close()  
    #sort the file by line number
    #I decided to not use the reverse in order to use the cache
    keys = sorted(fileContent.keys(), reverse=False)
    result = []
    for key in keys:
	    result.append(fileContent[key])
    
    return result

#Get set of rows form parsed file
def getRows(parsedFile, numberOfLines, offset, reverse):
	result = []
	print()
	if reverse:
		fromLine=offset-numberOfLines
		toLine  =offset
	else:
		fromLine=offset-1
		toLine  =offset+numberOfLines-1
	for i in range(fromLine, toLine):
		result.append(parsedFile[i])

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




