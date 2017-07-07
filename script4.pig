REGISTER '/home/acadgild/pig/airline/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/pig/airline/DelayedFlights.csv' USING CSVExcelStorage (',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

B = FOREACH A GENERATE (chararray)$17 AS origin, (chararray)$18 AS dest, (int)$24 AS diverted;

C = FILTER B by (origin is not null) AND (dest is not null) AND (diverted == 1);

D = GROUP C BY (origin,dest);

E = FOREACH D GENERATE group, COUNT(C.diverted);

F = ORDER E BY $1 DESC;

G = LIMIT F 5;

DUMP G;

