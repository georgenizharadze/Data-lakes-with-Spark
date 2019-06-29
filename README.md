# Data lakes with Spark

This is an educational practice project to grasp the fundamentals of cloud data lakes. Here we work with one implemented on Amazon S3, with Spark as the processing engine deployed on an AWS ERM cluster. 

The project is based on a hypothetical case of a music streaming app start-up, which currently keeps all the logs of its clients' sessions in daily JSON files in S3. Hence, the objective of the project is to migrate the data into appropriately designed tables which will be stored in column-oriented parquet files on S3. We use a spark process to ETL the data from json into parquet files. Spark can also be used subsequently for loading the parquet files and querying the data. However, this project is limited to ETL.   

## Requirements

- AWS account
- Appropriately configured (with Spark) EMR cluster
- S3 bucket to write the parquet files to


## Running the code

For a relatively simple way to test the code, set up a jupyter notebook on EMR cluster (this service is now available with EMR) and copy and run the functions from the `etl.py` file. Note that if you run the code from EMR notebook, you do not need the AWS access and secret keys, nor do you need to configure the Spark driver process, as it is launched automatically in the notebook and the `spark` object is already available.   