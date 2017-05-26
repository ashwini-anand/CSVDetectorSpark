#give input directory as command line argument to this shell script. e.g. bash csvdetect/python/run-detectCSV.sh /user/csvdetection
#where "/user/csvdetection" is input directory

declare hdfs_input_dir="$1"

su - hdfs -c "hdfs dfs -mkdir -p hdfs:/user/root/detectCSVTemp"
su - hdfs -c "hdfs dfs -chown yarn hdfs:/user/root/detectCSVTemp"

time spark-submit --master yarn --deploy-mode cluster --driver-memory 6g --executor-memory 15g --num-executors 5 --executor-cores 10 --queue default hdfs:/user/root/CSVDetectorV5.py ${hdfs_input_dir} /user/root/outputcsvdetection 20

su - hdfs -c "hdfs dfs -rm -r hdfs:/user/root/detectCSVTemp"