student_details = LOAD '/home/acadgild/hadoop/hdfs/Students_Concat.txt' USING PigStorage(',') AS (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray, city:chararray, gpa:int);

student_name_concat = foreach student_details Generate CONCAT (firstname, '_', lastname);

Dump student_name_concat;
