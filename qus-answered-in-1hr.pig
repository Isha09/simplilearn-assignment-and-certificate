/*PROBLEM 3: NUMBER OF QUESTIONS WHICH GOT ANSWERED WITHIN ONE HOUR*/

/*Step 1: Loaded the file as csv*/
StackEx_file = LOAD '/samples/Social_Media.csv' USINGorg.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (uid:int, qid:int, i:int, qs:int, qt:long, tags:chararray, qvc:int, qac:int, aid:int, j:int, as:int, at:long);

/*Step 2:Filtered records from the file where the time difference between question posted and answer time was less than or equal to 1 hr. Since epoch time is in seconds i have compared with 3600 sec which is equivalent to 1 hr.*/
flt_rec = FILTER StackEx_file BY (at-qt) <= 3600;

/* step 3: Grouped the records in step 2 by question id*/
grp_rec = GROUP flt_rec BY qid;

/*Step 4: In step 3 we might get duplicates as there can be multiple answers given for a single question within an hour. Hence selecting only 1 record  for each qid.*/
rem_dup = FOREACH grp_rec { top_rec = LIMIT flt_rec 1;
    GENERATE flatten(top_rec); };
    
/*Step 5: Counted number of records in step 4*/
count_rec = FOREACH (GROUP rem_dup ALL) GENERATE COUNT(rem_dup.qid) ;

/*Step 6: stored the output in a file*/
store count_rec into '/samples/output/solution3';
