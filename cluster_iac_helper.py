import configparser
import json
import time

import boto3

US_EAST_1 = 'us-east-1'
AVAILABLE = 'available'
DELETING = 'deleting'
CLUSTER_CREATE_POLL_TIME_MINUTES = 3
CLUSTER_DELETE_POLL_TIME_MINUTES = 2

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

AWS_KEY                 = config.get('AWS','KEY')
AWS_SECRET              = config.get('AWS','SECRET')

DWH_IAM_ROLE_NAME       = config.get("IAM_ROLE", "NAME")

DWH_CLUSTER_TYPE        = config.get("CLUSTER", "TYPE")
DWH_CLUSTER_IDENTIFIER  = config.get("CLUSTER", "IDENTIFIER")
DWH_NUM_NODES           = config.get("CLUSTER", "NUM_NODES")
DWH_NODE_TYPE           = config.get("CLUSTER", "NODE_TYPE")
# DHW_HOST                = config.get("CLUSTER", "DB_HOST")
DWH_DB                  = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER             = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD         = config.get("CLUSTER", "DB_PASSWORD")
DWH_DB_PORT             = config.get("CLUSTER", "DB_PORT")

ec2_resource = boto3.resource(
    'ec2',
    region_name=US_EAST_1,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)
s3_resource = boto3.resource(
    's3',
    region_name=US_EAST_1,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)
iam_resource = boto3.client(
    'iam',
    region_name=US_EAST_1,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)
redshift_resource = boto3.client(
    'redshift',
    region_name=US_EAST_1,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)

def create_iam_role():
    try:
        _ = iam_resource.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [
                    {
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': { 'Service': 'redshift.amazonaws.com' }
                    }
                ],
                'Version': '2012-10-17'
            })
        )
    except Exception as e:
        print(e)

    iam_resource.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    role_arn = iam_resource.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    config.set("IAM_ROLE", "ARN", role_arn)
    print(f"role_arn: {role_arn}")
    return role_arn


def cluster_props():
    return redshift_resource.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )['Clusters'][0]

def create_cluster(roleArn):
    try:
        _ = redshift_resource.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

    while True:
        cluster_status = cluster_props()['ClusterStatus']
        if cluster_status == AVAILABLE:
            break
        time.sleep(CLUSTER_CREATE_POLL_TIME_MINUTES * 60)
        print("Waiting for cluster creation...")
    print("Cluster has been created.")

    dwh_endpoint = cluster_props()['Endpoint']['Address']
    dwh_role_arn = cluster_props()['IamRoles'][0]['IamRoleArn']
    print("dwh_endpoint: ", dwh_endpoint)
    print("dwh_role_arn: ", dwh_role_arn)
    return (dwh_endpoint, dwh_role_arn)

def destroy_cluster():
    redshift_resource.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        SkipFinalClusterSnapshot=True
    )

    while True:
        cluster_status = cluster_props()['ClusterStatus']
        if cluster_status != DELETING:
            break
        time.sleep(CLUSTER_DELETE_POLL_TIME_MINUTES * 60)
        print("Waiting for cluster destruction...")
    print("Cluster has been destroyed.")

def destroy_iam_role():
    iam_resource.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam_resource.delete_role(
        RoleName=DWH_IAM_ROLE_NAME
    )


role_arn = create_iam_role()
create_cluster(role_arn)
# verify that we can connect to the cluster?
# destroy_cluster()
# destroy_iam_role()