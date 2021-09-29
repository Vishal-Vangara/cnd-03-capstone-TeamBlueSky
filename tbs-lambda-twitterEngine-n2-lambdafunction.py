import json
import boto3

bucket_list = []
All_Tweet_Entity_Text = []
All_Tweet_Entity_Type = []
All_Tweet_Entity_Score = []


def lambda_handler(event, context):
    
    CompClient = boto3.client(service_name='comprehend', region_name='us-east-1')
    
    s3_client = boto3.client('s3')
    s3_bucket_name = 'tbs-s3-raw-n1'
    
    s3 = boto3.resource('s3')
    
    my_bucket_raw = s3.Bucket(s3_bucket_name)
    # All the raw data from tbs-s3-raw-n1 is put into my_bucket_raw
    
    #print(my_bucket_raw.objects)

 
    for file in my_bucket_raw.objects.filter(Prefix = 'Input_to_S3_RAW'):
        file_name = file.key
        if file_name:
            bucket_list.append(file_name)
            #print(bucket_list)
            

    
    for file in bucket_list:
        obj = s3.Object(s3_bucket_name,file)
        data = obj.get()['Body'].read()
        #print(data)
                
            
  
    
    Tweet_Entities = CompClient.detect_entities(LanguageCode="en", Text=str(data))
    Tweet_Entities_List = Tweet_Entities["Entities"]
    
    # print(Tweet_Entities_List[0]['Text'])
    
    sample_counter = 0
    
    for Tweet_Entity_Dictionary in Tweet_Entities_List:
        
        # print(abc)
        All_Tweet_Entity_Text.append(Tweet_Entity_Dictionary['Text'])
        All_Tweet_Entity_Type.append(Tweet_Entity_Dictionary['Type'])
        All_Tweet_Entity_Score.append(Tweet_Entity_Dictionary['Score'])
        
        
    
    print("All_Tweet_Entity_Text:")
    print(All_Tweet_Entity_Text)
    
    print("All_Tweet_Entity_Type:")
    print(All_Tweet_Entity_Type)
    
    Count_Person = All_Tweet_Entity_Type.count('PERSON')
    Count_LOCATION = All_Tweet_Entity_Type.count('LOCATION')
    Count_EVENT = All_Tweet_Entity_Type.count('EVENT')
    Count_ORGANIZATION = All_Tweet_Entity_Type.count('ORGANIZATION')
    Count_QUANTITY = All_Tweet_Entity_Type.count('QUANTITY')
    Count_OTHER = All_Tweet_Entity_Type.count('OTHER')

    print(Count_Person)
    print(Count_LOCATION)
    print(Count_EVENT)
    print(Count_ORGANIZATION)
    print(Count_QUANTITY)
    print(Count_OTHER)
    
    
    
    Dictionary_Entities_Person = {"Twitter_Entity" :"PERSON" , "Count" : Count_Person}
    Dictionary_Entities_Location = {"Twitter_Entity" :"LOCATION" , "Count" : Count_LOCATION}
    Dictionary_Entities_Event = {"Twitter_Entity" :"EVENT" , "Count" : Count_EVENT}
    Dictionary_Entities_Org = {"Twitter_Entity" :"ORGANIZATION" , "Count" : Count_ORGANIZATION}
    Dictionary_Entities_Quantity = {"Twitter_Entity" :"QUANTITY" , "Count" : Count_QUANTITY}
    Dictionary_Entities_Other = {"Twitter_Entity" :"OTHER" , "Count" : Count_OTHER}
    
    List_Entities_All = [Dictionary_Entities_Person,Dictionary_Entities_Location,Dictionary_Entities_Event,Dictionary_Entities_Org,Dictionary_Entities_Quantity,Dictionary_Entities_Other]
    
    # print(Dictionary_Entities_All)
    
    
    
    print("All_Tweet_Entity_Score:")
    print(All_Tweet_Entity_Score)
    
    
    
    
    count = 0
    Temp_List = []
    List_Sentiment = []
    # print(data.pop())
    Comprehend_Positive = 0
    Comprehend_Negative = 0
    Comprehend_Neutral = 0
    Comprehend_Mixed = 0
    Comprehend_Total = 0
    
    
    
    
    sentiment = CompClient.detect_sentiment(Text=str(data), LanguageCode='en')
    sentRes = sentiment['Sentiment'] 
    sentScore = sentiment['SentimentScore']
    Comprehend_Positive = Comprehend_Positive + sentScore["Positive"]
    Comprehend_Negative = Comprehend_Negative + sentScore["Negative"]
    Comprehend_Neutral = Comprehend_Neutral + sentScore["Neutral"]
    Comprehend_Mixed = Comprehend_Mixed + sentScore["Mixed"]
    
    Write_Dictionary_Positive = { "Sentiment" : "Positive", "Value": Comprehend_Positive}
    Write_Dictionary_Negative = {"Sentiment" : "Negative", "Value": Comprehend_Negative}
    Write_Dictionary_Neutral = { "Sentiment" : "Neutral", "Value": Comprehend_Neutral}
    Write_Dictionary_Mixed = {"Sentiment" : "Mixed", "Value": Comprehend_Mixed}
    
    Write_Dictionary_List_All = [Write_Dictionary_Positive, Write_Dictionary_Negative, Write_Dictionary_Neutral, Write_Dictionary_Mixed]
    
    
    #print(Write_Dictionary)
    
    # Write_Text_One = "{"+"Sentiment: Positive,Value:"+str(Comprehend_Positive)+"}"
    # Write_Text_Two = "{"+"Sentiment: Negative,Value:"+str(Comprehend_Negative)+"}"
    # Write_Text_Three = "{"+"Sentiment: Neutral,Value:"+str(Comprehend_Neutral)+"}"
    # Write_Text_Four = "{"+"Sentiment: Mixed,Value:"+str(Comprehend_Mixed)+"}"
    
    # Write_file = Write_Text_One + Write_Text_Two + Write_Text_Three +  Write_Text_Four
    
    #print(Write_file)
    
    # print(List_Sentiment)
    Comprehend_Total = Comprehend_Positive +Comprehend_Negative +Comprehend_Neutral +Comprehend_Mixed
    
    
    print("1. positive score: "+str(Comprehend_Positive*100/Comprehend_Total))
    print("2. Neutral score: "+str(Comprehend_Neutral*100/Comprehend_Total))
    print("3. Negative score: "+str(Comprehend_Negative*100/Comprehend_Total))
    print("4. Mixed score: "+str(Comprehend_Mixed*100/Comprehend_Total)) 
        
    # sentiment = CompClient.detect_sentiment(Text=str(data), LanguageCode='en')
    
    # .detect_sentiment(Text=data,LanguageCode='en')['Sentiment']
    
    # print("SENTIMENT"+str(sentiment))
    #uploadByteStream_SEND = bytes(json.dumps(sentScore).encode('UTF-8'))
    #uploadByteStream_SEND = bytes(json.dumps(Write_file).encode('UTF-8'))
    
    List_Sentiment_Entities = [Write_Dictionary_Positive, Write_Dictionary_Negative, Write_Dictionary_Neutral, Write_Dictionary_Mixed,Dictionary_Entities_Person,Dictionary_Entities_Location,Dictionary_Entities_Event,Dictionary_Entities_Org,Dictionary_Entities_Quantity,Dictionary_Entities_Other]
  
  
    S3_SEND_CLIENT = boto3.client('s3')
    Bucket_SEND = "tbs-s3-sentiment-n3"
    fileName_SEND = 'SamplePositive.json'   
    
    
    uploadByteStream_SEND = bytes(json.dumps(Write_Dictionary_List_All).encode('UTF-8'))
    
    # uploadByteStream_SEND = bytes(json.dumps(List_Sentiment_Entities).encode('UTF-8'))
    
    S3_SEND_CLIENT.put_object(Bucket=Bucket_SEND, Key=fileName_SEND, Body=uploadByteStream_SEND)
    
    
    S3_SEND_ENTITIES_CLIENT = boto3.client('s3')
    Bucket_SEND_ENTITIES = "tbs-s3-entity-n2"
    fileName_SEND_ENTITIES = 'Entities.json' 
    
    uploadByteStream_Entities_SEND = bytes(json.dumps(List_Entities_All).encode('UTF-8'))
    S3_SEND_ENTITIES_CLIENT.put_object(Bucket=Bucket_SEND_ENTITIES, Key=fileName_SEND_ENTITIES, Body=uploadByteStream_Entities_SEND)
    

    
    
    # uploadByteStream_SEND = bytes(json.dumps(Write_Dictionary_Positive).encode('UTF-8'))
    
    
    # uploadByteStream_SEND = bytes(json.dumps(Write_Dictionary_Negative).encode('UTF-8'))
    # S3_SEND_CLIENT.put_object(Bucket=Bucket_SEND, Key=fileName_SEND, Body=uploadByteStream_SEND)
    
    # uploadByteStream_SEND = bytes(json.dumps(Write_Dictionary_Neutral).encode('UTF-8'))
    # S3_SEND_CLIENT.put_object(Bucket=Bucket_SEND, Key=fileName_SEND, Body=uploadByteStream_SEND)
    
    # uploadByteStream_SEND = bytes(json.dumps(Write_Dictionary_Mixed).encode('UTF-8'))
    # S3_SEND_CLIENT.put_object(Bucket=Bucket_SEND, Key=fileName_SEND, Body=uploadByteStream_SEND)
    
    
    print("S3 PUT COMPLETE")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    



