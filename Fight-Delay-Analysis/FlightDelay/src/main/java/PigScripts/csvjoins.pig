uniqueid = LOAD 'distinct' using PigStorage(',') as (uid);
carriers = LOAD 'carriers.csv' using PigStorage(',') as (id,name);
joined = JOIN uniqueid BY uid, carriers BY id;
summed = FOREACH joined GENERATE $0,$2;
STORE summed into 'joinoutput';
