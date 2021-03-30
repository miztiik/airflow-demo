from aws_cdk import aws_redshift as _redshift
from aws_cdk import aws_ec2 as _ec2
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_secretsmanager as _sm
from aws_cdk import core as cdk
from stacks.miztiik_global_args import GlobalArgs


class RedshiftClusterStack(cdk.Stack):

    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        vpc,
        ec2_instance_type: str,
        stack_log_level: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Cluster Password
        comments_cluster_secret = _sm.Secret(
            self,
            "setRedshiftClusterSecret",
            description="Redshift Cluster Secret",
            secret_name="RedshiftClusterSecret",
            generate_secret_string=_sm.SecretStringGenerator(
                exclude_punctuation=True
            ),
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        # Create RedShift cluster

        # Redshift IAM Role
        _rs_cluster_role = _iam.Role(
            self, "redshiftClusterRole",
            assumed_by=_iam.ServicePrincipal(
                "redshift.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3ReadOnlyAccess"
                )
            ]
        )

        # Subnet Group for Cluster
        rs_cluster_subnet_group = _redshift.CfnClusterSubnetGroup(
            self,
            "redshiftClusterSubnetGroup",
            subnet_ids=vpc.get_vpc_public_subnet_ids,
            description="Redshift Cluster Subnet Group"
        )

        # Create Security Group for QuickSight
        quicksight_to_redshift_sg = _ec2.SecurityGroup(
            self,
            id="redshiftSecurityGroup",
            vpc=vpc.get_vpc,
            security_group_name=f"redshift_sg_{id}",
            description="Security Group for Quicksight"
        )

        # https://docs.aws.amazon.com/quicksight/latest/user/regions.html
        quicksight_to_redshift_sg.add_ingress_rule(
            peer=_ec2.Peer.ipv4("52.23.63.224/27"),
            connection=_ec2.Port.tcp(5439),
            description="Allow QuickSight connetions"
        )

        rs_cluster = _redshift.CfnCluster(
            self,
            "redshiftCluster",
            cluster_type="single-node",
            # number_of_nodes=1,
            db_name="comments_cluster",
            master_username="airflow_user",
            master_user_password=comments_cluster_secret.secret_value.to_string(),
            iam_roles=[_rs_cluster_role.role_arn],
            node_type=f"{ec2_instance_type}",
            cluster_subnet_group_name=rs_cluster_subnet_group.ref,
            vpc_security_group_ids=[
                quicksight_to_redshift_sg.security_group_id]
        )

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = cdk.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page."
        )
        output_1 = cdk.CfnOutput(
            self,
            "RedshiftCluster",
            value=f"{rs_cluster.attr_endpoint_address}",
            description=f"RedshiftCluster Endpoint"
        )
        output_2 = cdk.CfnOutput(
            self,
            "RedshiftClusterPassword",
            value=(
                f"https://console.aws.amazon.com/secretsmanager/home?region="
                f"{cdk.Aws.REGION}"
                f"#/secret?name="
                f"{comments_cluster_secret.secret_arn}"
            ),
            description=f"Redshift Cluster Password in Secrets Manager"
        )
        output_3 = cdk.CfnOutput(
            self,
            "RedshiftIAMRole",
            value=(
                f"{_rs_cluster_role.role_arn}"
            ),
            description=f"Redshift Cluster IAM Role Arn"
        )
