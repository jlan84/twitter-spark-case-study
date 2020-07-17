import pyspark as ps
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json


def load_data_rdd(fname):
    '''Initiates an RDD  created from the file provided.
    
    Parameters
    ----------
    fname (str): a string of the path to the file
    
    Returns
    -------
    file_rdd: RDD of the file
    
    '''
    sc = spark.sparkContext
    file_rdd = sc.textFile(fname)
    return file_rdd


def parse_json_first_key_pair(json_string, key):
    """Returns the value from a json encoded dictionary for the specified key.

    Parameters
    ----------
    json_string (str): a string encoding a json dictionary
    key (str): a string indicating which key to call from teh json dictionary

    Returns
    -------
    value: the value of the specified key from the json dictionary

    """
    try: 
        json_obj = json.loads(json_string)
        k = key
        value = json_obj[k]
    except:
        value = ''
    return value

def cleaned_rdd(rdd):
    '''Returns an RDD that is a count of all of the twitter mentions in an RDD
    
    Parameters
    ----------
    rdd (spark rdd): a spark rdd containing a list of tweets
    
    Returns
    -------
    clean_rdd (spark rdd): a spark rdd with a count of the number of times
        each twitter user is mentioned
    '''
    clean_rdd = rdd.map(lambda row: parse_json_first_key_pair(row))\
                .flatMap(lambda row: row.split(' '))\
                .filter(lambda row: row.startswith('@'))\
                .map(lambda x: (x, 1))\
                .reduceByKey(lambda a,b: a + b)\
                .sortBy(lambda x: x[1], ascending=False)
    return cleaned_rdd

if __name__ == '__main__':
    spark = (ps.sql.SparkSession.builder 
            .master("local[4]") 
            .appName("sparkSQL exercise") 
            .getOrCreate()
            )

    tweets_rdd = load_data_rdd('data/french_tweets.json')
    mentioned_rdd = cleaned_rdd(tweets_rdd)
    
    print(mentioned_rdd.take(10))
    print('complete')