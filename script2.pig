REGISTER '/home/acadgild/pig/airline/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/pig/airline/DelayedFlights.csv' USING CSVExcelStorage (',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

B = FOREACH A GENERATE (int)$1 AS year, (int)$2 AS month,(int)$10 AS flight_num,(int)$22 AS cancelled,(chararray)$23 AS cancel_code;
 
C = FILTER B BY cancelled == 1 AND cancel_code =='B';
 
D = GROUP C BY (year,month);

E = FOREACH D GENERATE group, COUNT(C.cancelled);
 
F = ORDER E BY $1 DESC;
 
Result = LIMIT F 1;
 
DUMP Result;
