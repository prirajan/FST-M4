-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/priya/txtInput.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grouped = GROUP words BY word;
-- Count the occurence of each word (Reduce)
counted = FOREACH grouped GENERATE group, COUNT(words);
-- Store the result in HDFS
STORE counted INTO 'hdfs:///user/priya/PigOutput';
