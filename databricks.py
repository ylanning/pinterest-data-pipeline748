from pyspark.sql.functions import *
from pyspark.sql.types import *
import traceback
import urllib

dbutils = 'placeholder var'
display = 'placeholder var'
spark = 'placeholder var'

csv_schema = StructType([
    StructField("username", StringType(), True),
    StructField("password", StringType(), True),
    StructField("Access key ID", StringType(), True),
    StructField("Secret access key", StringType(), True)])

file_type = "csv"
first_row_is_header = "true"
delimiter = ","
aws_keys_df = spark.read.format(file_type)\
    .option("header", first_row_is_header)\
    .option("sep", delimiter)\
    .schema(csv_schema)\
    .load("/FileStore/tables/authentication_credentials.csv")

ACCESS_KEY = aws_keys_df.select('username').collect()[0]['username']
SECRET_KEY = aws_keys_df.select('password').collect()[0]['password']

# Print the content of the DataFrame to debug any issues
aws_keys_df.show()
  
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY.encode('utf-8'), safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0a2f66c3e41f-bucket"
# Mount name for the bucket
MOUNT_NAMES = ["/mnt/0a2f66c3e41f.df_pin","/mnt/0a2f66c3e41f.df_geo","/mnt/0a2f66c3e41f.df_user"]
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

for MOUNT_NAME in MOUNT_NAMES:
    # Check if mount already exists to avoid error
    if MOUNT_NAME not in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
      # Mount the drive
      dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
    else:
      print(f"Mount '{MOUNT_NAME}' already exists")
      #  Unmount the directory if it's already mounted
      dbutils.fs.unmount(MOUNT_NAME)

    # Mount the drive again
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

    # pprint([mnt.mountPoint for mnt in dbutils.fs.mounts()])

    # Check the content of mounted S3 bucket
    display(dbutils.fs.ls(MOUNT_NAME))

    if MOUNT_NAME.endswith(".df_pin"):
      topic = '0a2f66c3e41f.pin'

    if MOUNT_NAME.endswith(".df_geo"):
      topic = '0a2f66c3e41f.geo'

    if MOUNT_NAME.endswith(".df_user"):
      topic = '0a2f66c3e41f.user'
      
    # Read data from the mounted S3 bucket      
    try:
        display(dbutils.fs.ls(f"/{MOUNT_NAME}/topics/{topic}/partition=0/"))
    except Exception as e:
        print("FAILED")
        # Uncomment the line below to print the traceback
        traceback.print_exc()

    # File location and type
    file_location = f"{MOUNT_NAME}/topics/{topic}/partition=0/*.json"
    file_type = "json"
      
    # Ask Spark to infer the schema
    infer_schema = "true"

    # Read in JSONs from mounted S3 bucket
    df = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location)

    # Display Spark dataframe to check its content
    display(df)