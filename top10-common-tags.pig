/*PROBLEM1 –TOP 10 MOST COMMONLY USED TAGS IN THE DATASET.*/

/*Step -1 Loaded Social Media comma seperated file*/
StackEx_file = LOAD '/samples/Social_Media.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (uid:int, qid:int, i:int, qs:int, qt:chararray, tags:chararray, qvc:int, qac:int, aid:int, j:int, as:int, at:chararray);

/*Step -2 For each record in file selected only "tag" column*/
qus_tags = FOREACH StackEx_file GENERATE tags;

/*Step 3 Since each qid has multiple tags associated with it, For each record in step 2 I have splitted/tokenized the tags*/
split_tags = FOREACH qus_tags GENERATE FLATTEN(TOKENIZE(tags)) as splittag;

/*Step 4 Grouped  the data in step 3 using each tag as key*/
grouped_tags = GROUP split_tags BY splittag;

/*step 5: For each key in step 4 counted number of tags associated with it*/
tag_count = FOREACH grouped_tags GENERATE group,COUNT(split_tags) as tot_tag;

/*Step 6 Sorted data of step 5 in desc order */
sort_tag = ORDER tag_count BY tot_tag DESC;

/*Step 7 Selected only top 10 records from Step 6*/
top_10_tag = limit sort_tag 10;

/*Stored the output in file*/
store top_10_tag into '/samples/output/solution1.txt';
