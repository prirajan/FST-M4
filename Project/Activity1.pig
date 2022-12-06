-- Load data from HDFS
inputDialouges = LOAD 'hdfs:///user/priya/project/episodeIV_dialouges.txt' USING PigStorage('\t') AS (Name:chararray,Line:chararray);
-- Filter out the first 2 lines
ranked = Rank inputDialouges;
Dialogues= FILTER ranked By (rank_inputDialouges > 2);
--Group by name
GroupByName = GROUP Dialogues BY Name;
-- Count the number of lines by each character
Names = FOREACH GroupByName GENERATE $0 as Name, COUNT($1) as No_of_Lines;
SortNames= ORDER Names By No_of_Lines DESC;
-- Store result in HDFS
STORE SortNames INTO 'hdfs:///user/priya/project/outputs/episodeIVOutput' USING PigStorage('\t');
