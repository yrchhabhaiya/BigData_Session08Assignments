REGISTER '/home/acadgild/pig/airline/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/pig/airline/DelayedFlights.csv' USING CSVExcelStorage (',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
 
B = FOREACH A GENERATE (int)$1 AS year, (int)$10 AS flight_num, (chararray)$17 AS origin,(chararray) $18 AS dest;
 
C = FILTER B BY dest is not null;
 
D = GROUP C BY dest;
 
E = FOREACH D GENERATE group, COUNT(C.dest);
 
F = ORDER E BY $1 DESC;
 
Result = LIMIT F 5;
 
A1 = load '/user/acadgild/pig/airline/airports.csv' USING CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

A2 = foreach A1 generate (chararray)$0 as dest, (chararray)$2 as city, (chararray)$4 as country;
 
joined_table = join Result by $0, A2 by dest;
 
dump joined_table;
