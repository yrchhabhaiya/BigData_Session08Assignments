REGISTER '/home/acadgild/pig/airline/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/pig/airline/DelayedFlights.csv' USING CSVExcelStorage (',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

B = FOREACH A GENERATE (int)$16 AS departure_delay, (chararray)$17 AS origin;

C = FILTER B BY (departure_delay is not null) AND (origin is not null);

D = GROUP C BY origin;

E = FOREACH D GENERATE group, AVG(C.departure_delay);

F = ORDER E by $1 DESC;

G = LIMIT F 10;

DUMP G;

