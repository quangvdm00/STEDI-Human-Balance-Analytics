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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1687331027826 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://quang-aws-bucket/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1687331027826",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="test_db",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Privacy Filter Join
PrivacyFilterJoin_node1687331110326 = Join.apply(
    frame1=CustomerTrustedZone_node1687331027826,
    frame2=AccelerometerLanding_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyFilterJoin_node1687331110326",
)

# Script generated for node Drop Fields
DropFields_node1687331155355 = DropFields.apply(
    frame=PrivacyFilterJoin_node1687331110326,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1687331155355",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687331155355,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://quang-aws-bucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
