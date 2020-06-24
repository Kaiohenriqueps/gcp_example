from glob import glob
from google.cloud import storage


class StorageWrapper():
    def __init__(self, project_name):
        self.client = storage.Client(project=project_name)

    def create_bucket(self, bucket_name):
        bucket = self.client.bucket(bucket_name)
        bucket.create(location='us-east1')

    def upload_file(self, bucket_name, file_to_upload):
        filename = file_to_upload.split("/")[2]
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.upload_from_filename(file_to_upload)
        print("{} uploaded!".format(filename))


if __name__ == "__main__":
    FILES_TO_UPLOAD = glob("src/files/*.csv")
    bucket_name = "teste_kaio"
    wrapper = StorageWrapper('festive-magpie-279021')
    wrapper.create_bucket(bucket_name)
    for file_to_upload in FILES_TO_UPLOAD:
        print("Uploading...")
        wrapper.upload_file(bucket_name, file_to_upload)
