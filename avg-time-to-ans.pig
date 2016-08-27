/*PROBLEM 2: Average time to answer questions*/

/*Step 1 - Loaded the file as a comma seperated values*/
StackEx_file = LOAD '/samples/Social_Media.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (uid:int, qid:int, i:int, qs:int, qt:long, tags:chararray, qvc:int, qac:int, aid:int, j:int, as:int, at:long);

/*Step 2 - for each record in step 1 calculated the difference between the time at which question was posted and time at which it was answered for the first time */
ans_time = FOREACH StackEx_file GENERATE at-qt as timediff;

/*Grouped the data in step 2 under 1 key and calculated  the average using avg function in pig*/
avg_time = FOREACH (GROUP ans_time ALL) GENERATE AVG(ans_time.timediff); 

/*Stored the output in a file. Since epoch time is in seconds the final answer is in seconds*/  
store avg_time into '/samples/output/solution2';store avg_time into '/samples/output/solution2';
