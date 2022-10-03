import os 
from azure.storage.blob import BlobServiceClient
import datetime
#import yaml 

#####################################################################################
#################################BACKUP############################################
#################################BACKUP#############################################
######################################################################################

class backup:
    def __init__(self) -> None:
        self.connect_string = 'your connection string'
        self.container_name = 'container name'
        self.path = 'path to upload from'
    def get_files(self, dir):
        with os.scandir(dir) as entries:
            for entry in entries:
                if entry.is_file() and not entry.name.startswith('.'):
                    yield entry
        entries.close()

    def upload_files(self, path, connect_str, container_name):
        for a in os.listdir(path):
            myList = os.path.join(path, a)
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            blob_client = blob_service_client.get_blob_client(container= container_name, blob=a)
            print("\nUploading to Azure Storage as blob:\n\t" + a)
            with open(myList, "rb") as data:
                blob_client.upload_blob(data)

    
    def blob_content(self, connect_str, container_name):
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_container_client(container= container_name)
        blob_list = blob_client.list_blobs()
        nametime =[]
        for i in blob_list:
            #contents.append(i)
            #print(f"file name {i['Key']}  and last modified {datetime.datetime.timestamp(i['LastModified'])}")
            nametime.append({
                'Key': i.name,
                'time': datetime.datetime.timestamp(i.last_modified)
            })
        return nametime

    def container_sync(self,connect_str, container_name, path):
        filetime = []
        for a in os.listdir(path):
            myList = os.path.join(path, a)
            stta = os.stat(myList) #.st_mtime #getting system modification time 
            filetime.append({
                'Key': a,
                'time': stta.st_mtime})
        file_time = [i for i in filetime]
        blob_time = [i for i in self.blob_content(connect_str, container_name)]
        for i in blob_time:
                for j in file_time:
                    if i['Key']==j['Key']:
                        if j['time']>i['time']: #if last modified time of item in s3 bucket greater than the last modified time on the local dir 
                            new_ff = path +'/'+ i['Key']
                            #s3_func(client2, bucketname,new_ff, i['Key'])
                            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
                            new_blob = 'new' + i['Key']
                            blob_client = blob_service_client.get_blob_client(container= container_name, blob=new_blob)
                            with open(new_ff, "rb") as data:
                                blob_client.upload_blob(data)
                                print(f"{i['Key']} file synchronized")
        return 'all Done'

    def run(self):
        try:
            self.upload_files(self.path, self.connect_string, self.container_name)
            self.container_sync(self.connect_string, self.container_name, self.path)
        except Exception as e:
            print('there was an error with the connection')

#####################################################################################
#################################recovery############################################
#################################recovery#############################################
######################################################################################

class recovery:
    def __init__(self):
        self.file_path = 'your file path'
        self.connect_str = 'your azure connection string'
        self.container_name = 'container name'
        pass
    def restore(self, path,connect_str,container_name):
        print("\nListing blobs...")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_container_client(container= container_name)

        # List the blobs in the container
        blob_list = blob_client.list_blobs()
        #print(blob_list)
        for blob in blob_list:
            #print(blob)
            print(blob.last_modified)
            print("\t" + blob.name)
            with open(path+blob.name, 'wb') as download_file:
                download_file.write(blob_client.download_blob(blob.name).readall())
        print('restore done')

    def run(self):
        try:
            self.restore(self.file_path, self.connect_str, self.container_name)
        except Exception as e:
            print('there was an error with the connection')

#recovery() #works 

#backup()
#this is fully functional now and can be attached to the tkiunter app 
