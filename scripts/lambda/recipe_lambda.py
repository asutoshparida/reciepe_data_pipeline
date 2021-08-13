import json
import os
import boto3
import datetime
import time
import mysql.connector
import logging
import traceback
import sys


def lambda_handler(event, context):

    master_instance_type = os.environ['MASTER_INSTANCE_TYPE']
    slave_instance_type = os.environ['SLAVE_INSTANCE_TYPE']
    slave_instance_count = int(os.environ['SLAVE_INSTANCE_COUNT'])
    ebs_volume_size = int(os.environ['EBS_VOLUME_SIZE'])
    ec2_key_name = os.environ['EC2_KEY_NAME']
    ec2_subnet_id = os.environ['EC2_SUBNET_ID']
    cluster_name = os.environ['CLUSTER_NAME']
    job_flow_role = os.environ['JOB_FLOW_ROLE']
    service_role = os.environ['SERVICE_ROLE']
    run_env = os.environ['ENV']

    stream_steps = [
        {
            "Name": "01. STEP - Process Recipe Data",
            "spark": {
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "—- master", "yarn",
                    "-— conf", "spark.yarn.submit.waitAppCompletion=true",
                    "s3://bucket/key/recipes_etl_job.py"
                ],
               },
            "ActionOnFailure": "CONTINUE",
        }
        ]

    client = boto3.client('emr')
    cluster = client.run_job_flow(
        Name=cluster_name,
        ReleaseLabel='emr-5.22.0',
        LogUri='s3n://..../cert/logs/',
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Ganglia'}
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master Nodes',
                    # 'Market': 'ON_DEMAND',
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': ebs_volume_size
                                },
                            },
                        ],
                        'EbsOptimized': True
                    },
                    'Configurations': [
                        {
                            "Classification": "yarn-site",
                            "Properties": {
                                "yarn.nodemanager.vmem-check-enabled": "false",
                                "yarn.nodemanager.pmem-check-enabled": "false"
                            }
                        },
                        {
                            "Classification": "spark",
                            "Properties": {
                                "maximizeResourceAllocation": "false"
                            }
                        },
                        {
                            "Classification": "spark-defaults",
                            "Properties": {

                                "spark.driver.cores": "2",
                                "spark.executor.cores": "2",
                                "spark.driver.memory": "13G",
                                "spark.executor.memory": "13G",
                                "spark.driver.memoryOverhead": "1460M",
                                "spark.executor.memoryOverhead": "1460M",
                                "spark.executor.instances": "10",
                                "spark.default.parallelism": "50",
                                "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                                # "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                                "yarn.nodemanager.vmem-check-enabled": "false",
                                "spark.dynamicAllocation.enabled": "false",
                                "spark.memory.fraction": "0.80",
                                "spark.memory.storageFraction": "0.30",
                                "spark.yarn.scheduler.reporterThread.maxFailures": "5",
                                "spark.storage.level": "MEMORY_AND_DISK_SER",
                                "spark.rdd.compress": "true",
                                "spark.shuffle.compress": "true",
                                "spark.shuffle.spill.compress": "true",
                                "spark.streaming.backpressure.enabled": "true"
                            }
                        },
                        {
                            "Classification": "mapred-site",
                            "Properties": {
                                "mapreduce.map.output.compress": "true"
                            }
                        },
                        {
                            "Classification": "spark-log4j",
                            "Properties": {
                                "log4j.rootCategory": "INFO,console"
                            },
                        },
                        {
                            "Classification": "hadoop-log4j",
                            "Properties": {
                                "hadoop.root.logger": "INFO,console"
                            }
                        }
                    ]
                },
                {
                    'Name': 'Slave Nodes',
                    # 'Market': 'ON_DEMAND',
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': slave_instance_type,
                    'InstanceCount': slave_instance_count,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': ebs_volume_size
                                },
                            },
                        ],
                        'EbsOptimized': True
                    },
                    'Configurations': [

                    ]
                },
            ],
            'Ec2KeyName': ec2_key_name,
            'KeepJobFlowAliveWhenNoSteps': False, #True to keep cluster alive
            'TerminationProtected': False,
            'Ec2SubnetId': ec2_subnet_id,
            'AdditionalMasterSecurityGroups': [
                'XXXXX'
            ],
            'AdditionalSlaveSecurityGroups': [
                'XXXXX'
            ],
        },

        Steps=stream_steps,

        VisibleToAllUsers=True,
        JobFlowRole=job_flow_role,
        ServiceRole=service_role,
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
        Tags=[
            {
                'Key': 'Name',
                'Value': cluster_name,
            },
            {
                'Key': 'Environment',
                'Value': run_env,
            },
        ],
    )
    JobFlowId = cluster['JobFlowId']
    print(JobFlowId + " Created")