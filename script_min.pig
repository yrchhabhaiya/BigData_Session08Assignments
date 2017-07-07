student_details = LOAD '/home/acadgild/hadoop/hdfs/Students_Min.txt' USING PigStorage(',') AS (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray, city:chararray, gpa:int);

student_grouped = GROUP student_details all;

student_min = FOREACH student_grouped GENERATE(student_details.firstname, student_details.gpa), MIN(student_details.gpa);

Dump student_min;
