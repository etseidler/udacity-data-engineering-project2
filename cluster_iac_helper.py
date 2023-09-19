import argparse
import configparser
import json
import time

import boto3
import psycopg2
from botocore.exceptions import ClientError

US_EAST_1 = 'us-east-1'
AVAILABLE = 'available'
DELETING = 'deleting'
CLUSTER_CREATE_POLL_TIME_MINUTES = 1
CLUSTER_DELETE_POLL_TIME_SECONDS = 15

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

AWS_KEY                 = config.get('AWS','KEY')
AWS_SECRET              = config.get('AWS','SECRET')

DWH_IAM_ROLE_NAME       = config.get("IAM_ROLE", "NAME")

DWH_CLUSTER_TYPE        = config.get("CLUSTER", "TYPE")
DWH_CLUSTER_IDENTIFIER  = config.get("CLUSTER", "IDENTIFIER")
DWH_NUM_NODES           = config.get("CLUSTER", "NUM_NODES")
DWH_NODE_TYPE           = config.get("CLUSTER", "NODE_TYPE")

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
iam_client = boto3.client(
    'iam',
    region_name=US_EAST_1,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)
redshift_client = boto3.client(
    'redshift',
    region_name=US_EAST_1,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)

def create_iam_role():
    try:
        _ = iam_client.create_role(
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

    iam_client.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    role_arn = iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    config.set("IAM_ROLE", "ARN", role_arn)
    print(f"role_arn: {role_arn}")
    return role_arn


def cluster_props():
    return redshift_client.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )['Clusters'][0]

def create_cluster(roleArn):
    try:
        _ = redshift_client.create_cluster(
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

    print("Creating cluster, please wait...")
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

def verify_redshift_connection(dwh_endpoint):
    try:
        vpc = ec2_resource.Vpc(id=cluster_props()['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_DB_PORT),
            ToPort=int(DWH_DB_PORT)
        )
    except Exception as e:
        print("Failed to authorize ingress? Or we already did it")

    try:
        conn = psycopg2.connect(
            dbname = DWH_DB,
            host = dwh_endpoint,
            port = DWH_DB_PORT,
            user = DWH_DB_USER,
            password = DWH_DB_PASSWORD,
        )
    except Exception as err:
        print(err)
    print(conn)
    print("LGTM!")

def destroy_cluster():
    redshift_client.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        SkipFinalClusterSnapshot=True
    )

    print("Destroying Cluster, please wait...")
    try:
        while True:
            cluster_status = cluster_props()['ClusterStatus']
            if cluster_status != DELETING:
                break
            time.sleep(CLUSTER_DELETE_POLL_TIME_SECONDS)
            print("Waiting for cluster destruction...")
    except ClientError:
        print("Cluster has been destroyed.")

def destroy_iam_role():
    try:
        iam_client.detach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        iam_client.delete_role(
            RoleName=DWH_IAM_ROLE_NAME
        )
    except ClientError as ce:
        print("Failed to destroy IAM role")
        print(ce)

if __name__ == "__main__":
    import argparse

# Instantiate the parser
    parser = argparse.ArgumentParser(description='Helper script to create and destroy Redshift cluster')
    parser.add_argument('-c', '--create', action='store_true', help='Create the cluster')
    parser.add_argument('-d', '--destroy', action='store_true', help='Destroy the cluster')
    parser.add_argument('-v', '--verify', help="Verify connection to an existing cluster via cluster endpoint")
    parser.add_argument('-x', '--exterminate', action='store_true', help='Destroy the cluster and the IAM role')
    args = parser.parse_args()

    if args.create:
        role_arn = create_iam_role()
        dwh_endpoint, dwh_role_arn = create_cluster(role_arn)
    if args.verify:
        verify_redshift_connection(args.verify)
    if args.destroy or args.exterminate:
        destroy_cluster()
    if args.exterminate:
        destroy_iam_role()