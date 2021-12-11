-------------------------------------------------------------------TABLE name.basics.tsv.gz --------------------------------------------------------------------
####################BASH#############################
cd data 
wget https://datasets.imdbws.com/name.basics.tsv.gz
gzip -d name.basics.tsv.gz
tail -n +2 name.basics.tsv >> names.tsv
rm name.basics.tsv
docker exec -it hive-server bash
##################HDFS###############################

hdfs dfs -mkdir /imdb
hdfs dfs -put data/names.tsv /imdb/names.tsv
hdfs dfs -ls /imdb

###################HIVE###################################
hive

CREATE DATABASE IMDB_db;

USE IMDB_db;

CREATE TABLE names( nconst STRING, primaryname STRING, birthYear SMALLINT, deathYear SMALLINT, primaryProfession ARRAY<STRING>, knownForTitles ARRAY<STRING> )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


LOAD DATA INPATH '/imdb/names.tsv'
INTO TABLE names;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE names_partitions( nconst STRING,  primaryname STRING, birthYear SMALLINT, deathYear SMALLINT, knownForTitles ARRAY<STRING>,secondProfession  STRING , thirdProfession STRING)
PARTITIONED BY (firstProfession STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


INSERT INTO TABLE names_partitions
PARTITION (firstProfession)
SELECT nconst, primaryname, birthYear, deathYear, knownForTitles,primaryProfession[1] as s, primaryProfession[2] as t, primaryProfession[0] as firstProfession
FROM names;

hdfs dfs -cat /user/hive/warehouse/imdb_db.db/names_partitions/firstprofession=actor/000000_0 | head -n 10

-------------------------------------------------------------------TABLE title.ratings.tsv.gz -------------------------------------------------------------------------------------
####################BASH#############################
cd data
wget https://datasets.imdbws.com/title.ratings.tsv.gz
gzip -d title.ratings.tsv.gz
tail -n +2 title.ratings.tsv >> ratings.tsv
rm title.ratings.tsv 
docker exec -it hive-server bash
##################HDFS####################################
hdfs dfs -put data/ratings.tsv /imdb/ratings.tsv

###################HIVE###################################
hive

CREATE TABLE ratings( tconst STRING, averageRating FLOAT, numVotes BIGINT )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


LOAD DATA INPATH '/imdb/ratings/ratings.tsv'
INTO TABLE ratings;


select * from ratings limit 10;

CREATE TABLE ratings_partitions(tconst STRING, averageRating FLOAT, numVotes BIGINT)
PARTITIONED BY (averageRating_floored  FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE ratings_partitions
PARTITION (averageRating_floored)
SELECT tconst, nvl(averageRating,0) as averageRating ,nvl(numVotes,0) as numVotes, floor(nvl(averageRating,0)) as averageRating_floored
FROM ratings;

hdfs dfs -cat /user/hive/warehouse/imdb_db.db/ratings_partitions/averagerating_floored=7/000000_0 | head -n 10

------------------------------------------------------------------TABLE title.principals.tsv.gz ----------------------------------------------------------------------------------------------------------------
####################BASH#############################
wget https://datasets.imdbws.com/title.principals.tsv.gz
gzip -d title.principals.tsv.gz
sed -i 's/\[//g' title.principals.tsv
sed -i 's/\]//g' title.principals.tsv
sed -i 's/\"//g' title.principals.tsv
tail -n +2 title.principals.tsv >> principals.tsv
rm title.principals.tsv

##################HDFS###############################
hdfs dfs -put data/principals.tsv /imdb/principals.tsv

###################HIVE###################################
hive

CREATE TABLE principals( tconst STRING,  ordering SMALLINT, nconst  STRING, category  STRING, job STRING,characters ARRAY<STRING> )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA INPATH '/imdb/principals.tsv'
INTO TABLE principals;

CREATE TABLE principals_partitions(tconst STRING,  ordering SMALLINT, nconst  STRING, job STRING,characters ARRAY<STRING> )
PARTITIONED BY (category STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


INSERT INTO TABLE principals_partitions
PARTITION (category)
SELECT tconst,ordering ,nconst, job , characters,category
FROM principals;


----------------------------------------------------------------------------TABLE title.akas.tsv.gz ----------------------------------------------------------------------------------------------------------------
####################BASH#############################
cd data
wget https://datasets.imdbws.com/title.akas.tsv.gz 
gzip -d title.akas.tsv.gz
tail -n +2 title.akas.tsv >> akas.tsv
sed -e 's|["'\'']||g' akas.tsv >> akas_n.tsv                        
'
sed -i 'y/è/e/' akas_n.tsv    
''
rm akas.tsv
mv akas_n.tsv akas.tsv

##################HDFS###############################
hdfs dfs -put data/akas.tsv /imdb/akas.tsv

###################HIVE###################################
hive
SET hive.lazysimple.extended_boolean_literal=true;

CREATE TABLE akas( titleId STRING,  ordering SMALLINT, title STRING, region STRING, lang STRING,types  ARRAY<STRING>, attributes ARRAY<STRING>, isOriginalTitle  BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


LOAD DATA INPATH '/imdb/akas.tsv'
INTO TABLE akas;


CREATE TABLE akas_partitions( titleId STRING,  ordering SMALLINT, title STRING, region STRING, lang STRING,types  ARRAY<STRING>, attributes ARRAY<STRING>, isOriginalTitle  BOOLEAN)
PARTITIONED BY (p_type STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

select distinct split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",")[0] from akas
union 
select distinct split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",")[1] from akas;

alter table akas_partitions add partition (p_type = "NULL");
alter table akas_partitions add partition (p_type = "alternative");
alter table akas_partitions add partition (p_type = "dvd");
alter table akas_partitions add partition (p_type = "festival");
alter table akas_partitions add partition (p_type = "tv");
alter table akas_partitions add partition (p_type = "video");
alter table akas_partitions add partition (p_type = "working");
alter table akas_partitions add partition (p_type = "original");
alter table akas_partitions add partition (p_type = "imdbDisplay");



INSERT INTO TABLE akas_partitions
PARTITION (p_type = "alternative")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"alternative")= TRUE;

INSERT INTO TABLE akas_partitions
PARTITION (p_type = "dvd")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"dvd")= TRUE;


INSERT INTO TABLE akas_partitions
PARTITION (p_type = "festival")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"festival")= TRUE;

INSERT INTO TABLE akas_partitions
PARTITION (p_type = "tv")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"tv")= TRUE;


INSERT INTO TABLE akas_partitions
PARTITION (p_type = "video")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"video")= TRUE;
	  
INSERT INTO TABLE akas_partitions
PARTITION (p_type = "working")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"working")= TRUE;

INSERT INTO TABLE akas_partitions
PARTITION (p_type = "original")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"original")= TRUE;

INSERT INTO TABLE akas_partitions
PARTITION (p_type = "imdbDisplay")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE array_contains(split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),","),"imdbDisplay")= TRUE;

INSERT INTO TABLE akas_partitions
PARTITION (p_type = "NULL")
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle
      from akas as a WHERE types[0] is NULL ;




################################################autre piste est le partitionnment selon le premiere element de l array types  ############################################################

INSERT INTO TABLE akas_partitions
PARTITION (p_type)
select a.titleId, a.ordering, a.title,a.region,a.lang,
       split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",") as types_clean, split(REGEXP_REPLACE(attributes[0], '[^0-9A-Za-z- ]+', ','),",") as attributes_clean, a.isOriginalTitle, split(REGEXP_REPLACE(types[0], '[^0-9A-Za-z ]+', ','),",")[0] as p_type
      from akas a;

--------------------------------------------------------------------------TABLE title.basics.tsv.gz----------------------------------------------------------------------------------------------------
####################BASH#############################

wget https://datasets.imdbws.com/title.basics.tsv.gz
gzip -d title.basics.tsv.gz
tail -n +2 title.basics.tsv >> basics.tsv

##################HDFS###############################
hdfs dfs -put data/basics.tsv /imdb/basics.tsv

###################HIVE###################################
hive

SET hive.lazysimple.extended_boolean_literal=true;

CREATE TABLE basics( tconst STRING,  titleType STRING, primaryTitle STRING, originalTitle STRING, isAdult BOOLEAN,startYear SMALLINT, endYear  SMALLINT, runtimeMinutes INT, genres ARRAY<STRING> )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;



LOAD DATA INPATH '/imdb/basics.tsv'
INTO TABLE basics;


CREATE TABLE basics_partitions( tconst STRING, primaryTitle STRING, originalTitle STRING, isAdult BOOLEAN,startYear SMALLINT, endYear  SMALLINT, runtimeMinutes INT, genres ARRAY<STRING> )
PARTITIONED BY (titleType STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE basics_partitions
PARTITION (titleType)
SELECT tconst,primaryTitle ,originalTitle,isAdult, startYear,endYear,runtimeMinutes,genres, titleType
FROM basics;

--------------------------------------------------------------------TABLE title.crew.tsv.gz----------------------------------------------------------------------------------------------------------------------
####################BASH#############################
cd data
wget https://datasets.imdbws.com/title.crew.tsv.gz
gzip -d title.crew.tsv.gz
tail -n +2 title.crew.tsv >> crew.tsv
##################HDFS###############################
hdfs dfs -put data/crew.tsv /imdb/crew.tsv

###################HIVE###################################

hive

CREATE TABLE crew( tconst  STRING,  directors  ARRAY<STRING>, writers  ARRAY<STRING>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


LOAD DATA INPATH '/imdb/crew.tsv'
INTO TABLE crew;

CREATE TABLE crew_partitions(tconst STRING, writers ARRAY<STRING>,director_i INT, nm_director STRING )
CLUSTERED BY (director_i) INTO 40 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

CREATE TABLE crew_exploded as 
SELECT tconst,director,writers
FROM crew
LATERAL VIEW explode(directors) itemTable AS director;

INSERT INTO TABLE crew_partitions
SELECT tconst, writers ,cast(substring(director,3,length(director)) as int) as director_i, director as nm_director
FROM crew_exploded;

drop table crew;

drop table crew_exploded;
######################################################################autre piste est le partitionnment selon le premiere element de l array directors###################

CREATE TABLE crew_partitions_1(tconst STRING, directors ARRAY<STRING>, writers ARRAY<STRING>,director INT  )
CLUSTERED BY (director) INTO 5 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE crew_partitions_1
SELECT tconst,directors, writers ,cast(substring(directors[0],3,length(directors[0])) as INT) as director
FROM crew;


-----------------------------------------------------------TABLE title.episodes.tsv.gz------------------------------------------------------------------------------------------------------------------------
####################BASH#############################
cd data
wget https://datasets.imdbws.com/title.episode.tsv.gz
gzip -d title.episode.tsv.gz
tail -n +2 title.episode.tsv >> episode.tsv
wc -l episode.tsv #nb de ligne
##################HDFS###############################

hdfs dfs -put data/episode.tsv /imdb/episode.tsv

###################HIVE###################################

hive

CREATE TABLE episode( tconst STRING, parentTconst STRING, seasonNumber SMALLINT, episodeNumber INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


LOAD DATA INPATH '/imdb/episode.tsv'
INTO TABLE episode;

CREATE TABLE episode_partitions(tconst STRING, parentTconst_i INT, seasonNumber SMALLINT, episodeNumber INT, parentTconst STRING )
CLUSTERED BY (parentTconst_i) INTO 20 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;



INSERT INTO TABLE episode_partitions
SELECT tconst, cast(substring(parentTconst,3,length(parentTconst)) as int) as parentTconst_i ,seasonNumber,episodeNumber, parentTconst
FROM episode;


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

hadoop fs -cat /user/hive/warehouse/imdb_db.db/crew_partitions/000001_0 | head
hadoop fs -cat /user/hive/warehouse/imdb_db.db/crew_partitions/000001_0 | grep -i nm0712761



#################################################################Queries############################################################################################################

# nb occurence par category
SELECT count(*),category
FROM principals_partitions
group by category;

#nb occurence par titleType
SELECT count(*),titleType 
FROM basics_partitions
GROUP BY titleType;

# titres sur lesquels Quentin Tarantino comme director a travaillé avec rating  
SELECT b.originalTitle , r.averageRating 
FROM  basics_partitions b 
INNER JOIN ratings_partitions r ON r.tconst = b.tconst 
INNER JOIN crew_partitions c ON b.tconst = c.tconst
INNER JOIN names_partitions n ON n.nconst = c.nm_director
INNER JOIN principals_partitions p on p.tconst=b.tconst and p.nconst=c.nm_director
WHERE n.primaryName LIKE 'Quentin Tarantino' and p.category = 'director' ;

# nombre d'espisode par saison de la serie Breaking Bad '

SELECT  max(episodeNumber), seasonNumber
FROM basics_partitions b
INNER JOIN episode_partitions e ON b.tconst = e.parentTconst
WHERE b.primaryTitle LIKE 'Breaking Bad' AND b.titleType  LIKE 'tvSeries'
GROUP BY seasonNumber;

# note moyenne des titres par catégorie, par ordre décroissant des notes moyennes 
SELECT AVG(r.averageRating) as avg , b.genres
FROM  basics_partitions b
INNER JOIN ratings_partitions r ON r.tconst = b.tconst  
GROUP BY b.genres  
ORDER BY avg DESC 
LIMIT 10;

# les titre des films avec rating de l acteur Robert De Niro
SELECT b.originalTitle , r.averageRating 
FROM  basics_partitions b
INNER JOIN ratings_partitions r ON r.tconst = b.tconst  
INNER JOIN principals_partitions p on p.tconst=b.tconst
INNER JOIN names_partitions n on n.nconst= p.nconst 
WHERE n.primaryName LIKE 'Robert De Niro' and p.category = 'actor' and b.titleType = 'movie' ;

# Les notes des series les plus votés
SELECT AVG(r.averageRating) as avg , b.primaryTitle, numVotes
FROM  basics_partitions b
INNER JOIN ratings_partitions r ON r.tconst = b.tconst 
WHERE b.titleType  LIKE 'tvSeries' 
GROUP BY b.primaryTitle,numVotes
ORDER BY numVotes DESC 
LIMIT 10;



