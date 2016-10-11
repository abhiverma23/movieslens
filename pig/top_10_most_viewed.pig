--PIG SCRIPT TO FIND TOP 10 MOST VIEWED MOVIES

moviesName = LOAD 'mydir/movies.csv' using PigStorage(',') as (id:int, title:chararray, geners:chararray);
moviesCount = LOAD 'myoutput/OUTPUT000/part-r-00000' using PigStorage('\t') as (id:int, viewed:int);
moviesOrdered = ORDER moviesCount BY viewed desc;
moviesLimit = LIMIT moviesOrdered 10;
moviesWithName = JOIN moviesLimit by id LEFT, moviesName by id;
Top10Movies = FOREACH moviesWithName GENERATE title, viewed;
store Top10Movies INTO 'myoutput/PIGOUT$DEST' using PigStorage('\t');
