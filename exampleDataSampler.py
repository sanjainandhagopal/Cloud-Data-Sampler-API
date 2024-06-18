import dataSampler

if __name__ == '__main__':

    acc_name = "<ACCOUNT_NAME>"
    acc_key = "<ACCOUNT_KEY>"
    container_name = "abluva"
    blob_name = "entity_list.csv"
    
    connection = dataSampler.Sampler(acc_name,acc_key)
    print(connection.get_sample(container_name=container_name, blob_name=blob_name))