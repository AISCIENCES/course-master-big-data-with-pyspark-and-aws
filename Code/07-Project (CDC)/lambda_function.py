import json
import boto3

def lambda_handler(event, context):
    
    
    bucketName = event["Records"][0]["s3"]["bucket"]["name"]
    fileName = event["Records"][0]["s3"]["object"]["key"]
    
    print(bucketName, fileName)
        
    glue = boto3.client('glue')

    response = glue.start_job_run(
        JobName = 'glueCDC-pyspark',
        Arguments = {
            '--s3_target_path_key': fileName,
            '--s3_target_path_bucket': bucketName
        } 
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
