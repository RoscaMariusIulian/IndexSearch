import os
import sys
import math
import pymongo
import nltk
ps=nltk.stem.PorterStemmer()
client = pymongo.MongoClient("mongodb+srv://test:test@storage-qwagc.mongodb.net/test?retryWrites=true&w=majority")
db = client.get_database("mydatabase")

def createEx():    
    with open("exceptions.txt", "r", encoding='utf-8-sig') as e:
        exceptions=list()
        for i in e:
            exceptions.append(i.strip().lower()) 
        return exceptions

def createStop():    
    with open("stopwords.txt", "r", encoding='utf-8-sig') as s:
        stops=list()
        for i in s:
            stops.append(i.strip().lower()) 
        return stops
                
def checkEx(word, exceptions):
    if word.strip().lower() in exceptions:
        return True
    else:
        return False
        
def checkStop(word, stops):
    if word.strip().lower() in stops:
        return True
    else:
        return False     

def getBaseForm(word):
    return ps.stem(word.strip().lower())

def iterativePath(dirName):
    fileList= list()
    for subdir, dirs, files in os.walk(dirName):
        for file in files:
            filepath = subdir + os.sep + file
            fileList.append(filepath)
    return fileList    
              
def directIndex():
    print("Direct Index is starting..")
    dirName = os.getcwd() + "/Folder"
    print("Checking contents in /Folder..")
    listOfFiles = iterativePath(dirName)
    exceptions = createEx()
    stops = createStop()
    dictList = list()
    for file in listOfFiles:
        with open(file, "r", encoding='utf-8-sig') as f:
            word=""
            dictionary=dict()
            while True:
                f_contents= f.read(1)
                if(f_contents.isalnum()):
                    word=word+f_contents   
                else:
                    if word !="":
                        word = word.strip().lower()
                        if checkEx(word, exceptions)==True:
                           if word in dictionary:
                                dictionary[word]=dictionary[word]+1
                                word=""
                           else:
                                dictionary[word]=1;
                                word=""
                        if checkEx(word,exceptions) == False and checkStop(word,stops)==False:
                            neword = getBaseForm(word)
                            if neword in dictionary:
                                dictionary[neword]=dictionary[neword]+1
                                word=""
                            else:
                                dictionary[neword]=1;
                                word=""
                        if(checkStop(word,stops)==True):
                            word=""             
                if not f_contents:
                    break          
            dictionary=calcTF(file,dictionary)
            dictList.append(dictionary)
    printDirectMongo(dictList)
    print("Direct Index done!\n")
            
def calcTF(file,dictionary):
    words=dict()
    aux= 0
    for key, value in dictionary.items():
        aux = aux + value
    for key, value in dictionary.items():
        tf = value/aux
        temp={"t":key,"c":value, "TF":tf}
        arr= words.setdefault("terms", [])
        arr.append(temp)
        words["terms"]=arr
    name=os.path.basename(file)   
    tmp = {"doc":name, "terms":{}}
    tmp["terms"]=words["terms"]
    return tmp   

def printDirectMongo(dictionary):
    records = db["direct"]
    records.delete_many({})
    records.insert_many(dictionary)
   
def readMongoDirect():   
    records = db["direct"]
    cursor = records.find()
    return cursor
    
def reverseIndex():
    print("Indirect Index is starting..")
    cursor = readMongoDirect()
    words=dict()
    noFiles = 0
    for data in cursor:
        noFiles = noFiles + 1
        for items in data["terms"]:
            temp={"d":data["doc"],"c":items["c"],"TF":items["TF"]}
            arr= words.setdefault(items["t"], [])
            arr.append(temp)
            words[items["t"]]=arr       
    words = calcIDF(words, noFiles)
    printIndirectMongo(words)
    print("Indirect Index done!\n")
    
def calcIDF(dictionary, docs):
    var=list()
    aux= 0
    for key, values in dictionary.items():
        temp = list()
        for i in values:
            aux=aux+1  
            temp.append(i)
        fract = docs / (aux)
        idf = math.log(fract, 10) 
        x={"term":key,"IDF":idf, "docs": {}}
        x["docs"]=temp
        var.append(x)  
        aux=0   
    return var

def printIndirectMongo(dictionary):
    records = db["indirect"]
    records.delete_many({})
    records.insert_many(dictionary)  

def booleanSearchMongo():
    if len(sys.argv) > 3:
        terms=list()
        op= list()
        files= list()
        print("Words searched: " ,end="")
        for i in range(1,len(sys.argv),2):
            terms.append(getBaseForm(sys.argv[i].strip().lower()))
            print(sys.argv[i], end=" ")
        for i in range(2,len(sys.argv),2):
            op.append(sys.argv[i])
        records = db["indirect"]
        cursor = records.aggregate([{"$match": {"term":{"$in":terms}}},
        {"$group": {"_id":"BooleanSearch","files": { "$push": "$docs.d" }}}])   
        for i in cursor:
            files = i['files']
        print("\n\nBoolean Search")        
        print("Initial step:")       
        print(files)
        print(op)    
        while(len(files)>1):
            x1=files.pop(0)
            x2=files.pop(0)
            if op[0] == " ":
                r=set(x1).intersection(set(x2))    
            if op[0] == "+":
                r=set(x1).union(set(x2))   
            if op[0] == "-":
                r=set(x1).difference(set(x2))
            files.insert(0,sorted(r))    
            op.pop(0)
            print("Intermediary step:")
            print(files)
            print(op)
        if len(files) > 0:   
            files = files.pop(0)    
        print("Boolean Search result is: " + str(files)+ "\n")
        return files  
    else:
        print("Index successfully created!")
        return 0        
                                       
def readMongoIndirect(terms):
    records = db["indirect"]
    cursor = records.aggregate([{"$match": {"term":{"$in":terms}}}])
    return cursor
    
def vectorSearch(imput):
    print("Vector Search")        
    query=list()
    file=dict()
    
    for i in range(1,len(sys.argv),2):
        query.append(getBaseForm(sys.argv[i].strip().lower()))
    cursor = readMongoIndirect(query)
    
    for item in cursor:
        for docs in item["docs"]:
            for doc in imput:
                if doc == docs["d"]:
                    temp={"term":item["term"], "TF":docs["TF"],"IDF":item["IDF"]}
                    arr= file.setdefault(doc, [])
                    arr.append(temp)
                    file[doc]=arr
                    
    cosinus= dict()
    numarator = 0
    numitor = 1
    for key, values in file.items():
        if key in imput:
            for val in values:
                numarator = numarator + val["TF"]*val["IDF"]*val["IDF"]
                numitor = numitor * math.sqrt(pow(val["TF"],2)+pow(val["IDF"],2))*math.sqrt(1+pow(val["IDF"],2))
            cosinus[key]=numarator/(numitor+1)        
    output = dict()
    for k in sorted(cosinus, key=cosinus.get, reverse=True):
        output[k]="{:.3e}".format(cosinus[k])
    print ("Vectorial Search result is: "+str(output))
    return output
                   
def openFilesForView(imput):
    dirName = os.getcwd() + "/Folder"
    listOfFiles = iterativePath(dirName)
    for i in reversed(imput):
        for file in listOfFiles:
            name=os.path.basename(file)
            if name == i:
                os.startfile(file)
                
def main():   
    directIndex()
    reverseIndex()
    output=booleanSearchMongo() 
    if output != 0: 
        output=vectorSearch(output)
        openFilesForView(output)    
             
if __name__=="__main__":
        main()
        