import boto3

s3 = boto3.client('s3', region_name='eu-west-2') 
s3.upload_file('/Users/DamonGill/OneDrive - TXM Group/Documents/Personal Folder/YRP Awards.jpg', 'my-first-bucket-damon', 'YRP Awards.jpg')