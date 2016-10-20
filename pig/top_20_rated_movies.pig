--PIG SCRIPT TO FIND TOP 20 MOVIES RATED BY VALID USERS3
--USER40PLUS001/part-r-00000 format userId,ratedMovies
--ratings.csv format userId,movieId,rating,timestamp

validUser = LOAD 'myoutput/USERS40PLUS001/part-r-00000' USING PigStorage('\t') as (uId:int, uRated:int);
moviesRatings = LOAD 'mydir/ratings.csv' USING PigStorage('\t') as (uId:int,mId:int,ratings:float,timestamp:long);
validUserRating = JOIN validUser BY uId LEFT OUTER, moviesRatings BY uId;
--DESCRIBE validUserRating
--validUserRating format validUser.uId, validUserRating.mId, validUser.uRated, validUserRating.uId,
-- validUserRating.ratings, validUserRating.timestamp
groupData = GROUP validUserRating BY moviesRatings.mId;
STORE validUserRating INTO 'myoutput/PIGOUTTEMP001';
DESCRIBE validUserRating;
--Illustrate groupData;