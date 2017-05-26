from pyspark import SparkContext
import csv
import subprocess
import sys
import os
import StringIO
import time

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
  #calculate timestamp to append with output filename
  otimestamp = time.strftime("%Y%m%d-%H%M%S")
  outputFile = outputFile+""+otimestamp
  if len(sys.argv) >= 4 and str.isdigit(sys.argv[3]):
    minPartitions = int(sys.argv[3])
  else:
    minPartitions = 2
  #hardcoding tmpFile, shall I change it ?
  tempFile = "hdfs:/user/root/detectCSVTemp"
  sc = SparkContext(appName = "DetectCSV")
  #Now working only on directory (see inputFile+"/*"). Make it to work with a single file and recursive directory also.
  proc = subprocess.Popen("hdfs dfs -stat '%n' "+inputFile+"/*",shell=True,stdout=subprocess.PIPE)
  for fname in proc.stdout:
    fname = fname.strip()
    f = inputFile+"/"+fname
    subprocess.call("hdfs dfs -touchz "+tempFile+"/"+fname,shell=True)
    cmdd = "hdfs dfs -cat "+f+" | head -150 | hdfs dfs -appendToFile - "+tempFile+"/"+fname
    subprocess.call(cmdd,shell=True)
  resultRDD = sc.wholeTextFiles(tempFile, minPartitions).map(processFiles)
  resultRDD.saveAsTextFile(outputFile)
  
  