#!/usr/bin/env python3

from aws_cdk import core

from airflow_demo.stacks.back_end.airflow_demo_stack import AirflowDemoStack
from airflow_demo.stacks.back_end.redshift_demo_stack import RedshiftClusterStack
from airflow_demo.stacks.back_end.vpc_stack import VpcStack

app = core.App()

# VPC Stack for hosting Airflow/Glue/Redshift etc.
vpc_stack = VpcStack(
    app,
    f"{app.node.try_get_context('project')}-vpc-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: Custom Multi-AZ VPC"
)


# Deploy Airflow cluster and load data"

airflow_demo = AirflowDemoStack(
    app,
    f"{app.node.try_get_context('project')}-stack",
    vpc=vpc_stack,
    ec2_instance_type="dc2.large",
    stack_log_level="INFO",
    description="Miztiik Automation: Deploy Airflow cluster and load data"
)

# Deploy Redshift cluster and load data"

redshift_dwh = RedshiftClusterStack(
    app,
    f"{app.node.try_get_context('project')}-redshift-stack",
    vpc=vpc_stack,
    ec2_instance_type="dc2.large",
    stack_log_level="INFO",
    description="Miztiik Automation: Deploy Redshift cluster and load data"
)


# Stack Level Tagging
_tags_lst = app.node.try_get_context("tags")

if _tags_lst:
    for _t in _tags_lst:
        for k, v in _t.items():
            core.Tags.of(app).add(k, v, apply_to_launched_instances=True)


app.synth()
