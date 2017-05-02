# CSVDetectorSpark
Run it using following command:

spark-submit --master yarn --deploy-mode cluster --driver-memory 6g --executor-memory 14g --num-executors 5 --executor-cores 10 --queue default csvdetect/python/CSVDetectorV1.py /user/csvdetection /user/root/outputcsvdetection

Adjust driver-memory , executor-memory , num-executors and executor-cores as per your  need. First parameter to CSVDetectorV1.py is input file and 2nd parameter is output file.
