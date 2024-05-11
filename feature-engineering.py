import subprocess
import sys
import os
import boto3

# install libraries
def install(package):
    print("===> Installing package: ", package)
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# load the requirements
def install_requirements():
    with open('/opt/ml/processing/input/req/requirements.txt', 'r') as f:
        for line in f.readlines():
            install(line.strip())

if __name__ == "__main__":
    print("-------------------- main ----------------------------")
    print("Sagemaker Pipeline version 05/05/2024-v1.0")

    base_dir = "/opt/ml/processing"

    print("----------------------------------------")
    print("1. install packages")

    install_requirements()

    print("----------------------------------------")
    print("2. load libraries")

    import sagemaker
    from sagemaker.feature_store.feature_group import FeatureGroup
    from sagemaker.feature_store.feature_store import FeatureStore
    from sagemaker.session import Session

    print("2.1. sagemaker version: ", sagemaker.__version__)

    boto_session = boto3.Session()
    sagemaker_session = sagemaker.Session(boto3.session.Session())

    region = boto3.Session().region_name
    print("region.2 ", region)

    print("----------------------------------------")
    print("3. feature store setup")

    sagemaker_client = boto_session.client(service_name='sagemaker',
                                           region_name=region)

    featurestore_runtime = boto_session.client(service_name='sagemaker-featurestore-runtime',
                                               region_name=region)

    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=sagemaker_client,
                                    sagemaker_featurestore_runtime_client=featurestore_runtime)

    feature_store = FeatureStore(sagemaker_session=feature_store_session)

    payment_feature_group_name = 'payment-fraud-feature-group'

    payment_feature_group = FeatureGroup(name=payment_feature_group_name,
                                         sagemaker_session=feature_store_session)

    print("----------------------------------------")
    print("4. query data")

    payment_query = payment_feature_group.athena_query()
    payment_table = payment_query.table_name

    query_string = 'SELECT fraud, amount, tx_1d, avg_1d, tx_7d, avg_7d, tx_30d, avg_30d, time_btw_cc_tx FROM ' + payment_table

    print("------------------------------")
    print("5. query_string: ", query_string)
    temp_location = "s3://eliezerraj-908671954593-dataset/payment-rcf/tmp"

    payment_query.run(query_string=query_string,
                      output_location=temp_location)

    payment_query.wait()

    print("------------------------------")
    print("6. saving dataframe ")
    payment_query.as_dataframe().to_csv(f"{base_dir}/train/train_data.csv",
                                        header=False,
                                        index=False)

    print("------------------------------")
    print("Process finished !!!!")
