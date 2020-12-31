#!/usr/bin/env python3

from aws_cdk import core

from airflow_demo.airflow_demo_stack import AirflowDemoStack


app = core.App()
AirflowDemoStack(app, "airflow-demo")

app.synth()
