--PIG SCRIPT TO FIND TOP 10 MOST VIEWED MOVIES

moviesName = LOAD 'mydir/moveis.csv' using PigStorage(',') as (id:int, title:chararray, geners:chararray);
moviesCount = LOAD 'myoutput/OUTPUT000/part-r-00000' using PigStorage('\t') as (id:int, viewed:int);
moviesOrdered = ORDER moviesCount BY viewed desc;
moviesLimit = LIMIT moviesOrdered 10;
dump moviesLimit;
