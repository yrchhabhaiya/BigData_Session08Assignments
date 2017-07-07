student_details = LOAD '/home/acadgild/hadoop/hdfs/Students_Tokenize.txt' USING PigStorage(',') as (id:int, name:chararray, age:int,  city:chararray);

student_name_tokenize = FOREACH student_details  GENERATE TOKENIZE(name);

DUMP student_name_tokenize;
