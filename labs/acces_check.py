import boto3

REGION = 'ap-southeast-1'

def count_buckets():
    try:
        s3 = boto3.resource('s3', region_name=REGION)
        return len(list(s3.buckets.all()))
    except Exception as e:
        print(e)
        return
    
if __name__ == '__main__':
    count = count_buckets()
    print(f"Found {count} buckets.")