
def printTimeElapsed(counter,df,tic):   #add task
    import time
    toc = time.time()
    print(f"Finished calculating entry {counter} of {len(df)}. Processing is {round((100*counter)/len(df),2)}% complete.")
    print(f"Time spent processing: {int(toc-tic)} sec. Time remaining (est.): {int((len(df)-(counter+1))*((toc-tic)/(counter+1)))} sec.")

def printTimeElapsedSuccess(counter,df,tic,numSuccess,numFail):
    import time
    numSuccess = numSuccess + 1
    numFail = (counter + 1) - numSuccess
    toc = time.time()
    print(f"Finished entry {counter} of {len(df)}. {numSuccess} linked successfully, {numFail} failures. Success rate {round((100*numSuccess)/(numSuccess+numFail),2)}%.")
    print(f"Time spent processing: {int(toc-tic)} sec. Time remaining (est.): {int((len(df)-(counter+1))*((toc-tic)/(counter+1)))} sec. {round((100*counter)/len(df),2)}% complete.")
    return numSuccess

def printTimeElapsedFail(counter,df,tic,numSuccess,numFail):
    import time
    numFail = numFail + 1
    numSuccess = (counter + 1) - numFail
    toc = time.time()
    print(f"Failed entry {counter} of {len(df)}. {numSuccess} linked successfully, {numFail} failures. Success rate {round((100*numSuccess)/(numSuccess+numFail),2)}%.")
    print(f"Time spent processing: {int(toc-tic)} sec. Time remaining (est.): {int((len(df)-(counter+1))*((toc-tic)/(counter+1)))} sec. {round((100*counter)/len(df),2)}% complete.")
    return numFail


def printTaskStart(task):
    import time
    tic = time.time()
    print(f"Starting {task}.")

def printTaskFinish(task,tic):
    import time
    toc = time.time()
    print(f"Finished {task}. {int(toc-tic)} sec. elapsed.")

def keepOnly_ListToRemove(fundq,listToKeep):
    listOfCols = fundq.columns.values.tolist()
    for i in range(0,len(listToKeep)):
        listOfCols.remove(listToKeep[i])
    return(listOfCols)















