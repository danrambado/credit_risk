import boto3
import os
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Download the data")
    parser.add_argument(
        "data_path",
        type=str,
        help=(
            "data/"
        ),
    )
    parser.add_argument(
        "s3_bucket",
        type=str,
        help=(
            "Folder where the dataset folder is saved"
        ),
    )
    parser.add_argument(
        "dataset_folder",
        type=str,
        help=(
            "Folder or prefix where the dataset is saved"
        ),
    )
    parser.add_argument(
        "AWS_ACCESS_KEY_ID",
        type=str,
        help=(
            "AWS_ACCESS_KEY_ID"
        ),
    )
    parser.add_argument(
        "AWS_SECRET_ACCESS_KEY",
        type=str,
        help=(
            "AWS_SECRET_ACCESS_KEY"
        ),
    )
    args = parser.parse_args()

    return args

def download_data(data_path, s3_bucket, dataset_folder, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    """
    function that downloads all files from a subfolder of a bucket and saves them 
    in a folder predefined by the user.

    Parameters
    ----------
    data_path : str
        path of the folder where will storage the dataset.

    s3_bucket : str
        bucket where the dataset is stored.
    
    dataset_folder : str
        folder where the dataset is stored.
    
    AWS_ACCESS_KEY_ID : str
        AWS_ACCESS_KEY_ID to access
    
    AWS_SECRET_ACCESS_KEY : str
        AWS_SECRET_ACCESS_KEY to access

    Returns
    -------
    folder : folder
        The folder where all the dataset downloaded will be storage
    """
    # create the data folder where will storage the dataset
    os.makedirs(data_path, exist_ok= True)

    # setup a AWS S3 client/resource
    s3 = boto3.resource(
        's3',   
        aws_access_key_id= AWS_ACCESS_KEY_ID,
        aws_secret_access_key= AWS_SECRET_ACCESS_KEY,
        )

    # point the resource at the existing bucket
    bucket = s3.Bucket(s3_bucket)
    subfolder = dataset_folder

    # download all the files inside the bucket subfolder
    for object in bucket.objects.filter(Prefix = subfolder): #list and iterate over all the filepaths inside the bucket subfolder
        filename = object.key.split("/")[-1] #get the filename
        if len(filename) > 0: #check is not empty
            with open(str(os.path.join(data_path, filename)), 'wb') as data:
                print(f"Downloading {filename}...")
                bucket.download_fileobj(os.path.join(subfolder, filename), data)
                print(f"{filename} downloaded")

if __name__ == "__main__":
    args = parse_args()
    download_data(args.data_path , args.s3_bucket, args.dataset_folder, args.AWS_ACCESS_KEY_ID, args.AWS_SECRET_ACCESS_KEY)