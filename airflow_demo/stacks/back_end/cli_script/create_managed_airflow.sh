#!/bin/bash
set -ex
set -o pipefail

# version: 1Jan2021

##################################################
#############     SET GLOBALS     ################
##################################################

REPO_NAME="airflow-demo"

GIT_REPO_URL="https://github.com/miztiik/${REPO_NAME}.git"

APP_DIR="/var/${REPO_NAME}"

LOG_DIR="/var/log/"
LOG_FILE="${LOG_DIR}miztiik-automation-apps-${REPO_NAME}.log"

VPC_ID="vpc-032fb4a0868a2c835"
PUB_SUBNET_1="subnet-077dc32745068f500"
PUB_SUBNET_2="subnet-09e443d292c3ac25f"
PVT_SUBNET_1="subnet-0b133df1e125ba985"
PVT_SUBNET_2="subnet-09ff0ddd10f30cb7c"
AIRFLOW_SG_1=""
S3_ARN="arn:aws:s3:::airflow-d"
DAGS_PATH="dags"
INST_TYPE="mw1.small"
MAX_WORKERS="2"
AIRFLOW_POLICY_NAME="airflowDemoFullAccessPolicy"
AIRFLOW_ROLE_NAME="airflowDemoIamRole"

function create_mwaa(){

    AIRFLOW_SG_1=$(aws ec2 create-security-group \
    --description "Allow Airflow Public Access" \
    --group-name "airflowSg01" \
    --vpc-id ${VPC_ID} \
    --tag-specifications 'ResourceType=security-group,Tags=[{Key=Project,Value=datascience}]' \
    --output=text --query 'GroupId'
    )

    allowAllTrafficWithSg=$(aws ec2 authorize-security-group-ingress \
    --group-id ${AIRFLOW_SG_1} \
    --source-group ${AIRFLOW_SG_1} \
    --protocol all \
    --port all
    )

    permsPolicyArn=$(aws iam create-policy \
    --policy-name ${AIRFLOW_POLICY_NAME} \
    --policy-document file://airflow_perms_policy.json \
    --output text \
    --query Policy.Arn
    )

    airflowRoleArn=$(aws iam create-role \
        --role-name ${AIRFLOW_ROLE_NAME} \
        --assume-role-policy-document file://airflow_trust_policy.json \
        --query Role.Arn
    )

    attachPermsToAirflowRole=$(
        aws iam attach-role-policy \
        --role-name ${AIRFLOW_ROLE_NAME} \
        --policy-arn ${permsPolicyArn}
    )

    mwaa_arn=$(aws mwaa create-environment \
        --name "airflowDemo02" \
        --airflow-version "1.10.12" \
        --environment-class ${INST_TYPE} \
        --source-bucket-arn ${S3_ARN} \
        --dag-s3-path ${DAGS_PATH} \
        --webserver-access-mode "PUBLIC_ONLY" \
        --max-workers ${MAX_WORKERS} \
        --execution-role-arn ${airflowRoleArn} \
        --network-configuration SecurityGroupIds=${AIRFLOW_SG_1},SubnetIds=${PVT_SUBNET_1},${PVT_SUBNET_2} \
        --tags 'project=datascience' \
        --output=text --query 'Arn'
    )
}


function nuke_all(){
    # delete role
    aws iam delete-role --role-name ${AIRFLOW_ROLE_NAME}
}

# Trigger deployment
create_mwaa


# Delete Resources
# nuke_all

