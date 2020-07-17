# French Tweets
In 2017, Emmanuel Macron and Marine Le Pen were the final two candidates in the French Presidential Election. The two candidates had drastically different approaches to governing, and as such, the election was a major topic of discussion on Twitter.

## Objective
The objective for this case study was to determine which of the candidates was mentioned the most on Twitter in the 3 days spanning from 4/26 - 4/29 leading up to the election. In these three days there were a total of 216,912 tweets each have over 20 attributes, so we started off by cleaning the data. 

## Data Cleaning
**For Mentions**
Tweets from three days of April 2017 were provided in a json file. In order to access the information the json file was read into a spark resilient distributed dataset (rdd). The format of this rdd was difficult to read.

![img](img/inital_data.png)

The rdd was converted into a json dictionary and the 'text' key was used to include only the text of each tweet. This rdd was further cleaned to include only words starting with '@' which corresponds to the mentions of other twitter handles. The sortByKey was used to create a count of the mentions of each twitter user. 

![img](img/cleaned_data.png)

showing only first 10 values

**For Macron vs Le Pen Mention Comparison**
The same tweets from three days in April 2017 were read into a spark dataframe. This dataframe was reduce, using SQL, to include only the text and the timestamp from each tweet. The tweets were filtered to count the number of mentions for each candidate and grouped into hours. 

## Plots

By comparing mentions of both presidential candidates we can see the eventual winner, Marcon, receiving more mentions over the 3 day span. 
![img](img/MLP_Marcon_together.png)


Graphing the total number of twitter mentions we can see Le Pen received 683 less mentions.


![img](img/mentions_2.png)


## Conclusion

Based on the analysis performed on the raw data, Emmanuael Macron received 61% of the mentions over the three day period.  This may have had an effect on the outcome of the election, given that Macron won receiving over 66% of the votes.