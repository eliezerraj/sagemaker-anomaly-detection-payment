import boto3
import sagemaker
from sagemaker import Session
from sagemaker import get_execution_role

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
    print("Sagemaker create inference endpoint version 05/05/2024-v1.0")

    # Loading enviroment variables...
    print("----------------------------------")
    print("1. Loading enviroment variables... ")

    model_package_group_name = "PaymentAnomalyModelPackageGroup"
    role = get_execution_role()
    sm_client = boto3.client(service_name="sagemaker")
    sm_runtime = boto3.client(service_name="sagemaker-runtime")

    # find the latest approved model package
    print("-----------------------------------")
    print("2. Find the latest approved model package... ")

    # get a list of approved model packages from the model package group you specified earlier
    approved_model_packages = sm_client.list_model_packages(
          ModelApprovalStatus='Approved',
          ModelPackageGroupName=model_package_group_name,
          SortBy='CreationTime',
          SortOrder='Descending'
      )

    try:
        latest_approved_model_package_arn = approved_model_packages['ModelPackageSummaryList'][0]['ModelPackageArn']
    except Exception as e:
        print("===> Failed to retrieve an approved model package:", e)

    print("-----------------------------------")
    print("3. Latest_approved_model_package_arn: ",latest_approved_model_package_arn)

    # retrieve required information about the model
    latest_approved_model_package_descr = sm_client.describe_model_package(ModelPackageName = latest_approved_model_package_arn)

    # model artifact uri (tar.gz file)
    model_artifact_uri = latest_approved_model_package_descr['InferenceSpecification']['Containers'][0]['ModelDataUrl']
    # sagemaker image in ecr
    image_uri = latest_approved_model_package_descr['InferenceSpecification']['Containers'][0]['Image']

    print("-----------------------------------")
    print("4. model_artifact_uri: ", model_artifact_uri)
    print("5. image_uri: ", image_uri)

    # Create the model...
    print("-----------------------------------")
    print("6. Create the model... ")
    from time import gmtime, strftime

    model_name = "rfc-payment-anomaly-model-v1-" + strftime("%Y-%m-%d-%H-%M-%S", gmtime())

    print("-----------------------------------")
    print("7. Model name: " + model_name)

    #  environment variables
    env_vars = {"SAGEMAKER_CONTAINER_LOG_LEVEL": "20",
                "SAGEMAKER_ENABLE_CLOUDWATCH_METRICS": "false"}

    create_model_response = sm_client.create_model(
        ModelName=model_name,
        Containers=[
            {
                "Image": image_uri,
                "Mode": "SingleModel",
                "ModelDataUrl": model_artifact_uri,
                "Environment": env_vars,
            }
        ],
        #EnableNetworkIsolation=True,
        #VpcConfig={
        #    "SecurityGroupIds": [
        #        "sg-071e23394778596b2"
        #    ],
        #    "Subnets": [
        #        "subnet-0ca2d45be9d6ab5bc",
        #        "subnet-0b2b57acbb1da0623",
        #        "subnet-05d3e7f4611c12a23"
        #    ]
        #},
        ExecutionRoleArn=role,
    )

    print("-----------------------------------")
    print("8. Created Model Arn: " + create_model_response["ModelArn"])

    # Create endpoint configuration...
    print("-----------------------------------")
    print("9. Create endpoint configuration... ")

    model_epc_name = "mlops-epc-payment-anomaly-model-v1-" + strftime("%Y-%m-%d-%H-%M-%S", gmtime())

    endpoint_config_response = sm_client.create_endpoint_config(
        EndpointConfigName=model_epc_name,
        ProductionVariants=[
            {
                "VariantName": "byoVariant",
                "ModelName": model_name,
                "ServerlessConfig": {
                    "MemorySizeInMB": 4096,
                    "MaxConcurrency": 20,
                },
            },
        ],
    )

    print("-----------------------------------")
    print("10. Endpoint Configuration Arn: " + endpoint_config_response["EndpointConfigArn"])

    # Create endpoint configuration...
    print("-----------------------------------")
    print("11. Create endpoint ... ")

    endpoint_name = "rcf-serverless-ep-payment-anomaly-model-v1-" + strftime("%Y-%m-%d-%H-%M-%S", gmtime())

    create_endpoint_response = sm_client.create_endpoint(
        EndpointName=endpoint_name,
        EndpointConfigName=model_epc_name,
    )

    print("-----------------------------------")
    print("12. Endpoint Arn: " + create_endpoint_response["EndpointArn"])

    # wait for endpoint to reach a terminal state (InService) using describe endpoint
    print("-----------------------------------")
    print("13. Wait for endpoint to reach a terminal state (InService) using describe endpoint ... ")

    import time

    describe_endpoint_response = sm_client.describe_endpoint(EndpointName=endpoint_name)

    while describe_endpoint_response["EndpointStatus"] == "Creating":
        describe_endpoint_response = sm_client.describe_endpoint(EndpointName=endpoint_name)
        print(describe_endpoint_response["EndpointStatus"])
        time.sleep(15)

    describe_endpoint_response

    print("-----------------------------------")
    print("Process finished ... ")