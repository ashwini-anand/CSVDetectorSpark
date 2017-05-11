from pyspark import SparkContext
import csv
import subprocess
import sys
import os
import StringIO

def processFiles(fileNameContentsPair):
  fileName= fileNameContentsPair[0]
  result = "\n\n"+fileName
  resultEr = "\n\n"+fileName
  input = StringIO.StringIO(fileNameContentsPair[1].encode('utf-8'))
  reader = csv.reader(input,strict=True)

  try:
       i=0
       for row in reader:
         if i==100:
           break
         i=i+1
  except csv.Error as e:
    resultEr = resultEr +" is not a CSV file\n\n"
    return resultEr

  try:
    input.seek(0)
    samplestr = input.readline()
    samplestr = samplestr + input.readline()
    samplestr = samplestr + input.readline()
    samplestr = samplestr + input.readline()
    samplestr = samplestr + input.readline()
    dialect = csv.Sniffer().sniff(samplestr)
    result = result+"\n Delimiter: "+str(dialect.delimiter)
    result = result+"\n Doublequote: "+str(dialect.doublequote)
    result = result+"\n Escapechar: "+str(dialect.escapechar)
    result = result+"\n Quotechar: "+str(dialect.quotechar)
    result = result+"\n Skipinitialspace: "+str(dialect.skipinitialspace)
    hasHeader = csv.Sniffer().has_header(samplestr)
    result = result+"\n Has Header: "+str(hasHeader)
    input.seek(0)
    if hasHeader == True:
      reader2 = csv.reader(input)
      result = result +"\n Header: "+ str(reader2.next())
    result = result + "\n Metadata for this file ends here"
  except:
    resultEr = resultEr +" is not a CSV file\n\n"
    return resultEr

  return result
  
         

if __name__ == "__main__":
  inputFile = sys.argv[1]
  outputFile = sys.argv[2]
  if len(sys.argv) >= 3:
    tempFile = sys.argv[3]
  else:
    tempFile = "/user/detectCsvApp/tempFile"
  #hardcoding tmpFile, need to change this
  tempFile = "hdfs:/user/root/test"
  sc = SparkContext(appName = "DetectCSV")
  proc = subprocess.Popen("hdfs dfs -stat '%n' "+inputFile+"/*",shell=True,stdout=subprocess.PIPE)
  #below commented code is asking password, why ? , need to fix this
  #subprocess.call("su - hdfs -c 'hdfs dfs -chown yarn hdfs:/user/root/test'",shell=True)
  for fname in proc.stdout:
    fname = fname.strip()
    f = inputFile+"/"+fname
    subprocess.call("hdfs dfs -touchz "+tempFile+"/"+fname,shell=True)
    cmdd = "hdfs dfs -cat "+f+" | head -150 | hdfs dfs -appendToFile - "+tempFile+"/"+fname
    subprocess.call(cmdd,shell=True)
  #minPartitions is hard coded, change this later
  resultRDD = sc.wholeTextFiles(tempFile, 40).map(processFiles)
  resultRDD.saveAsTextFile(outputFile)
  
      
      
      
  