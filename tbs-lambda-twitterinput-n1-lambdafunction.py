import json
import requests
import os
import time

import boto3


s3 = boto3.client('s3')

bucket = "tbs-s3-raw-n1"
 
#"ENGvIND" 

fileName = 'Input_to_S3_RAW.json'   
TweetDataList = []
ListToStr = ""

twt_i = []

# S3 dump filename, using hashtag 

def lambda_handler(event, context):
    
    i= 0
    max_tweet_result = 15
    
    HashTag = os.getenv("HashTag")
    
    ''' Handler = "from:Tokyo2020hi" '''
    response = []
    
    bearer_token_lambda = os.getenv("bearer_token")
    
    headers = {"Authorization": "Bearer {}".format(bearer_token_lambda)}
    response = requests.request("GET", "https://api.twitter.com/2/tweets/search/recent?max_results="+str(max_tweet_result)+"&query="+str(HashTag), headers=headers, verify=False)
    
    
    TopTweets = response.json()
    TweetIDRepo = []
    TweetOnlyRepo = []
    
    
    
    '''
    STEPS to be done
    
    1. Increase the max_tweet_result value to 500 and include it in the search query so that the search results will be useful
    2. Import OS package by putting the pip command from the onenote and putting all of this updated code in the local 
    3. Zip them all
    4. Make the bearer token come as an environment variable
    5. Create space for user input for hashtag of choice or a twitter handle
    
    '''
   
    for TopTweets_Key, TopTweets_Values in TopTweets.items():
        for twt_i in TopTweets_Values:
            i = i+1
            
            if i<max_tweet_result:
                
                if hasattr(twt_i,'get'):
                    
                    # print(str(i)+". "+str(twt_i.get("text")))
                    temp_string_variable = twt_i.get("text")
                    
                    TweetDataList.append(temp_string_variable)
                    temp_string_variable = ""
                    # print(TweetDataList)
                    

    
    
    #  ListToStr = ''.join([str(elem) for elem in TweetDataList])
    #Send data as a list as it is easier to call Comprehend without size issues
    
    #uploadByteStream = bytes(json.dumps(ListToStr).encode('UTF-8'))
    uploadByteStream = bytes(json.dumps(TweetDataList).encode('UTF-8'))
    
    s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)

    print("S3 PUT COMPLETE")
    




