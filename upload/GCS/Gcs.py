from http import client
from google.cloud import storage
from google.oauth2 import service_account
import json
import datetime
import os


#####################################################################################
#################################UPLOAD############################################
#################################UPLOAD#############################################
######################################################################################

class Gcs_backup:
    def __init__(self, keypath, bucketName, uploadPath) -> None:
        GOOGLE_APPLICATION_CREDENTIALS = open(keypath, 'r') #replace with file path to the json
        gcp_json_credentials_dict = json.loads(GOOGLE_APPLICATION_CREDENTIALS.read())
        credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)
        self.clien = storage.Client(project=gcp_json_credentials_dict['project_id'], credentials=credentials) #this is cli
        self.bucket_name = bucketName
        self.upload_path = uploadPath
        #pass
    def upload_files(self, file_path, cli, bucket_name):
        new_buck = cli.bucket(bucket_name)

        filetime = []
        for a in os.listdir(file_path):
            myList = os.path.join(file_path, a)
            stta = os.stat(myList) #.st_mtime 
            filetime.append({
                'Key': a,
                'time': stta.st_mtime })
            #print(myList)
            blob = new_buck.blob(a)
            blob.upload_from_filename(myList)
            print('uploaded')

    def blob_content(self, cli, bucket_name):
        buck = cli.bucket(bucket_name)
        nametime =[]
        for i in buck.list_blobs():
            nametime.append({
                'Key': i.name,
                'time': datetime.datetime.timestamp(i.time_created)})
        return nametime

    def bucket_sync(self, cli, bucket_name, file_path):
        filetime = []
        for a in os.listdir(file_path):
            myList = os.path.join(file_path, a)
            stta = os.stat(myList) #.st_mtime #getting system modification time 
            filetime.append({
                'Key': a,
                'time': stta.st_mtime})
        file_time = [i for i in filetime]
        blob_time = [i for i in self.blob_content(cli, bucket_name)]
        for i in blob_time:
            for j in file_time:
                if i['Key']==j['Key']:
                    if j['time']>i['time']: #if last modified time of item in bucket greater than the last modified time on the local dir 
                            new_ff = file_path +'/'+ i['Key']
                            #s3_func(client2, bucketname,new_ff, i['Key'])
                            buck = cli.bucket(bucket_name)
                            new_blob = 'new' + i['Key']
                            blob = buck.blob(new_blob)
                            blob.upload_from_filename(new_ff)
                            print('sync Done')
        return 'sync done'


    
    #upload_files('/Users/c1oud4o4/Documents/test folder/pictures/ff', clien, 'me3g47') # works 
    #print(blob_content(clien, 'me3g47')) #works
    def run(self):
        try:
            self.bucket_sync(self.clien, self.bucket_name, self.upload_path)
            return 'upload and sync done'
        except Exception as e:
            return f'an unexpected error occured: {e}'




          
#def bucket_content(cli, bucket_name):
#####################################################################################
#################################recovery############################################
#################################recovery#############################################
######################################################################################




class Gcs_Restore:
    def __init__(self, keypath, bucketName, recoverTo) -> None:
        GOOGLE_APPLICATION_CREDENTIALS = open(keypath, 'r') #replace with file path to the json
        gcp_json_credentials_dict = json.loads(GOOGLE_APPLICATION_CREDENTIALS.read())
        credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)
        self.clien = storage.Client(project=gcp_json_credentials_dict['project_id'], credentials=credentials) #this is cli
        self.bucket_name = bucketName
        self.recover_to = recoverTo
    def recovery(self, file_path, cli, bucket_name):
        blob_list = cli.bucket(bucket_name)
        #print(blob_list)
        for blob in blob_list.list_blobs():
            blob1 = blob_list.blob(blob.name)
            nn = file_path+'/'
            print(nn)
            blob1.download_to_filename(nn+blob.name)
            #print('recovered')
     
     

    def Run(self):
        try:
            self.recovery(self.recover_to, self.clien, self.bucket_name)
            return f'recovery to {self.recover_to} Done!!!'
        except Exception as e:
            return f'an unexpected error occured: {e}'
    #notes
    #cli is client
    #recovery('aa', client, 'me3g47')


backup = Gcs_backup('systempath to key', 'bucketname', 'path to backup from').run()
restore = Gcs_Restore('systempath to key', 'bucketname', 'path to restore to').run()
