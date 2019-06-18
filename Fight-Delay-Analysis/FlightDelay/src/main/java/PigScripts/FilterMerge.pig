flights = LOAD 'Merge.csv' using PigStorage(',');
filter1 = FILTER flights BY $0 > 2017;
filter2 = FOREACH filter1 GENERATE $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,'LatestData'; 
STORE filter2 into 'LatestData';
