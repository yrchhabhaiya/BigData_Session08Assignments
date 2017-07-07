student_details = LOAD '/home/acadgild/hadoop/hdfs/Students_Limit.txt' USING PigStorage(',') AS (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray, city:chararray, gpa:int);

student_limit = LIMIT student_details 4;

Dump student_limit;
