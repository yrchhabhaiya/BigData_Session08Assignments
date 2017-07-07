players = load '/home/acadgild/hadoop/hdfs/baseball' as (name:chararray, team:chararray, position:bag{t:(p:chararray)}, bat:map[]);

pos = foreach players generate name, flatten(position) as position;

pos10 = limit pos 10;

dump pos10;
