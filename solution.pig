/*        This script contains the solution for the following query,
        write properly commented Pig Latin scripts to perform sentimental
        analysis on the news headline, classifying them as positive, negative or neutral.
        For the Sentimental analysis you will compare words in the headline for presence in
        Sentiment Lexicon i.e files with positive and negative words (positive.txt, negative.txt and positivenegative.txt)
        The output should be written to a file in the following format: Headline | PositiveCount | NegativeCount | Classification
*/

-- First start with loading the data into srcRecords, assuming that you have the data files in current the working in the directory

srcNews = load 'abcnews.csv' using PigStorage (',') as
            (date:chararray , headline:chararray);
            
            
srcWords = load 'positivesnegatives.csv' using PigStorage(',') as
            (word:chararray , sentiment:chararray);

positiveWords = load 'positives.txt' as ( positiveWord:chararray);

negativeWords = load 'negatives.txt' as ( negativeWord:chararray);

posWords = filter srcWords by $1 == 'positive';

negWords = filter srcWords by $1 == 'negative';

posWords = foreach posWords generate word;

negWords = foreach negWords generate word;


positiveWords = UNION positiveWords , posWords;

negativeWords = UNION negativeWords , negWords;

positiveWords = DISTINCT positiveWords;

negativeWords = DISTINCT negativeWords;

-- Now assign rankings to each positive and negative word and Merge them

posRankings = FOREACH positiveWords GENERATE positiveWord , 1 as ( points );

negRankings = FOREACH negativeWords GENERATE negativeWord , 1 as ( points );

--totalRankings = UNION  posRankings , negRankings;

-- Now split (tokenize) the headline into words

words = FOREACH srcNews GENERATE date , headline , FLATTEN (TOKENIZE(headline) ) as (word);

-- Now join on the basis of the words in the headlines and the files

posJoined = join words by word left outer , posRankings by positiveWord;
negJoined = join words by word left outer , negRankings by negativeWord;

-- Words::word, posRanking::positveWord, posRanking::points


-- Now group by the date and the headline

posGrpd = group posJoined by (date , headline);
negGrpd = group negJoined by (date , headline);

-- Now extract required features only

posGrpd = FOREACH posGrpd GENERATE group , posJoined.points;
negGrpd = FOREACH negGrpd GENERATE group , negJoined.points;

-- Now get the average rating

positiveCount = foreach posGrpd generate group , SUM($1.posRankings::points) as posCount;
negativeCount = foreach negGrpd generate group , SUM($1.negRankings::points) as negCount;

-- Now join using the date and the headline

allCounts = join positiveCount by group , negativeCount by group;
allCounts =  FOREACH allCounts GENERATE positiveCount::group , (positiveCount::posCount is null?0:positiveCount::posCount) , (negativeCount::negCount is null?0:negativeCount::negCount);
-- Now filter the records


positiveHeadlines = filter allCounts by ($1 is not null and $2 is null ) or $1 > $2;
positiveHeadlines = FOREACH positiveHeadlines GENERATE  positiveCount::group , $1 , $2, 'positive';

negativeHeadlines = filter allCounts by ($1 is null and $2 is not null ) or $1 < $2;
negativeHeadlines = FOREACH negativeHeadlines GENERATE positiveCount::group , $1 , $2 , 'negative';

neutralHeadlines = filter allCounts by ($1 is null and $2 is null ) or $1 == $2;
neutralHeadlines = FOREACH neutralHeadlines GENERATE positiveCount::group , $1 , $2 , 'neutral';

allHeadlines = UNION positiveHeadlines ,  negativeHeadlines;

allHeadlines = UNION allHeadlines , neutralHeadlines;

store allHeadlines into 'results' using PigStorage (',');
