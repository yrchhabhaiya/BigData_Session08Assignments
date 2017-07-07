student_details = LOAD '/home/acadgild/hadoop/hdfs/Students_Distinct.txt' USING PigStorage(',') AS (id:int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);

distinct_data = DISTINCT student_details;

Dump distinct_data;
