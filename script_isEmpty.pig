emp_sales = LOAD '/home/acadgild/hadoop/hdfs/Employee_Sales_IsEmpty.txt' USING PigStorage(',') as (sno:int, name:chararray, age:int, salary:int, dept:chararray);
	
emp_bonus = LOAD '/home/acadgild/hadoop/hdfs/Employee_Bonus_IsEmpty.txt' USING PigStorage(',') as (sno:int, name:chararray, age:int, salary:int, dept:chararray);

cogroup_data = COGROUP emp_sales by age, emp_bonus by age;

Dump cogroup_data;

isempty_data = filter cogroup_data by IsEmpty(emp_sales);

Dump isempty_data;
