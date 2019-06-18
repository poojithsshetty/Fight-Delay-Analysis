flights = LOAD 'apr17.csv' using PigStorage(',');
grouped = GROUP flights BY $6;
summed = FOREACH grouped GENERATE group, COUNT(flights) AS cntd;
sorted = ORDER summed BY cntd DESC;
top25 = LIMIT sorted 10;
Dump top25;
STORE top25 into 'top10Carriers';
