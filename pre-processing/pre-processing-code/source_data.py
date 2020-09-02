import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool
from s3_md5_compare import md5_compare


def data_to_s3(frmt):
    # throws error occured if there was a problem accessing data
    # otherwise downloads and uploads to s3

    source_dataset_url = 'https://fred.stlouisfed.org/graph/fredgraph'
    url_end = '?id=TRUCKD11'
    try:
        response = urlopen(source_dataset_url + frmt + url_end)

    except HTTPError as e:
        raise Exception('HTTPError: ', e.code, frmt)

    except URLError as e:
        raise Exception('URLError: ', e.reason, frmt)

    else:
        data_set_name = os.environ['DATA_SET_NAME']
        filename = data_set_name + frmt
        file_location = '/tmp/' + filename

        with open(file_location, 'wb') as f:
            f.write(response.read())
            f.close()

        # variables/resources used to upload to s3
        s3_bucket = os.environ['S3_BUCKET']
        new_s3_key = data_set_name + '/dataset/' + filename
        s3 = boto3.client('s3')

        # If the md5 hash of our new file does NOT match the s3 etag, upload the new file
        has_changes = md5_compare(s3, s3_bucket, new_s3_key, file_location)
        if has_changes:
            s3.upload_file(file_location, s3_bucket, new_s3_key)
            print('Uploaded: ' + filename)
        else:
            print('No update needed for ' + filename)

        # deletes to preserve limited space in aws lamdba
        os.remove(file_location)

        # dicts to be used to add assets to the dataset revision
        asset_source = {'Bucket': s3_bucket, 'Key': new_s3_key}
        return {'has_changes': has_changes, 'asset_source': asset_source}


def source_dataset():

    # list of enpoints to be used to access data included with product
    data_endpoints = [
        '.xls',
        '.csv'
    ]
    asset_list = []

    # multithreading speed up accessing data, making lambda run quicker
    with (Pool(2)) as p:
        s3_uploads = p.map(data_to_s3, data_endpoints)

    # If any of the data has changed, we need to republish the adx product
    count_updated_data = sum(
        upload['has_changes'] == True for upload in s3_uploads)
    if count_updated_data > 0:
        asset_list = list(
            map(lambda upload: upload['asset_source'], s3_uploads))
        if len(asset_list) == 0:
            raise Exception('Something went wrong when uploading files to s3')
    # asset_list is returned to be used in lamdba_handler function
    # if it is empty, lambda_handler will not republish
    return asset_list
