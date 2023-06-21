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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="test_db",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1687329456560 = glueContext.create_dynamic_frame.from_catalog(
    database="test_db",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1687329456560",
)

# Script generated for node Privacy Filter Join
PrivacyFilterJoin_node1687329511255 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrustedZone_node1687329456560,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyFilterJoin_node1687329511255",
)

# Script generated for node Drop Fields
DropFields_node1687329559351 = DropFields.apply(
    frame=PrivacyFilterJoin_node1687329511255,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1687329559351",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1687329559351,
    database="test_db",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
