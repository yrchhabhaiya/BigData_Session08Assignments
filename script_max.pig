student_details = LOAD '/home/acadgild/hadoop/hdfs/Students_Max.txt' USING PigStorage(',') AS (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray, city:chararray, gpa:int);

student_grouped = GROUP student_details all;

student_max = FOREACH student_grouped GENERATE(student_details.firstname, student_details.gpa), MAX(student_details.gpa);

Dump student_max;
