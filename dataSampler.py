from dataclasses import dataclass
from azure.storage.blob import BlobServiceClient, generate_blob_sas, ContainerSasPermissions
import pandas as pd
from pyspark.sql import SparkSession
import json
from mimetypes import guess_extension
import os
from datetime import datetime, timedelta

@dataclass
class Sampler:
    account_name : str
    account_key : str
    def connect(self):
        try:
            return BlobServiceClient(account_url=f"https://{self.account_name}.blob.core.windows.net", credential=self.account_key)
        except ConnectionError as ce: 
            return TypeError(f"COnnection failed: {ce}")

    def get_sample(self, container_name, blob_name):

        #for creating blob client
        blob_client = self.connect().get_blob_client(container=container_name, blob=blob_name)
        blob_properties = blob_client.get_blob_properties()
        content_type = blob_properties.content_settings.content_type
        extension_with_dot = guess_extension(content_type)
        extension = extension_with_dot[1:] if extension_with_dot else ""
        params = Sampler.infer_file(extension)

        sample = Sampler.sample_data_strategy1(extension, params, blob_properties, blob_client, container_name, blob_name)
        return sample
    
    def generate_sas_token(blob_client, container_name,blob_name):
        sas_expiry = datetime.utcnow()+timedelta(hours=1)

        sas_token = generate_blob_sas(
            account_name="<ACCOUNT_NAME>",
            container_name=container_name,
            blob_name=blob_name,
            account_key="<ACCOUNT_KEY>",
            permission=ContainerSasPermissions(read=True),
            expiry=sas_expiry
        )
        return sas_token
    
    def read_blob_as_df(blob_url, delimiter=',', nrows=10):
        try:
            df = pd.read_csv(blob_url, delimiter=delimiter, nrows=nrows)
            return df
        except:
            return "Formate doesn't supproted."
    
    def infer_file(extension):
        file = open('reader.json')
        resources = json.load(file)
        params = resources[f'{extension}']

        return params

    def sample_data_strategy1(extension, params, blob_properties, blob_client, container_name, blob_name):
        blob_url = blob_client.url
        sas_token = Sampler.generate_sas_token(blob_client, container_name,blob_name)
        blob_url = f"{blob_client.url}?{sas_token}"
        df_sample = Sampler.read_blob_as_df(blob_url, nrows=10)
        return df_sample
    
    def sample_data_strategy2(extension, params):
        return