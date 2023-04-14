import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1681313965289 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lk-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681313965289",
)

# Script generated for node Join Customers
JoinCustomers_node1681313895863 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1681313965289,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomers_node1681313895863",
)

# Script generated for node Drop Fields
DropFields_node1681314047654 = DropFields.apply(
    frame=JoinCustomers_node1681313895863,
    paths=[],
    transformation_ctx="DropFields_node1681314047654",
)

# Script generated for node Amazon S3
AmazonS3_node1681317125676 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1681314047654,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lk-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1681317125676",
)

job.commit()
