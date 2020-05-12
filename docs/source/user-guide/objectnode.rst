Object Subsystem (ObjectNode)
==============================

How To start ObjectNode
------------------------

Start a ObjectNode process by execute the server binary of ChubaoFS you built with ``-c`` argument and specify configuration file.

.. code-block:: bash

   nohup cfs-server -c objectnode.json &


Configurations
-----------------------
Object Node using `JSON` format configuration file.


**Properties**

.. csv-table::
   :header: "Key", "Type", "Description", "Mandatory"

   "role", "string", "Role of process and must be set to ``objectnode``", "Yes"
   "listen", "string", "Listen and accept port of the server", "Yes"
   "domains", "string slice", "
   | Domain of S3-like interface which makes wildcard domain support
   | Format: ``DOMAIN``", "No"
   "logDir", "string", "Log directory", "Yes"
   "logLevel", "string", "
   | Level operation for logging.
   | Default: ``error``", "No"
   "masterAddr", "string slice", "
   | Format: ``HOST:PORT``.
   | HOST: Hostname, domain or IP address of master (resource manager).
   | PORT: port number which listened by this master", "Yes"
   "authNodes", "string slice", "
   | Format: *HOST:PORT*.
   | HOST: Hostname, domain or IP address of AuthNode.
   | PORT: port number which listened by this AuthNode", "Yes"
   "exporterPort", "string", "Port for monitor system", "No"
   "prof", "string", "Pprof port", "Yes"


**Example:**

.. code-block:: json

   {
        "role": "objectnode",
        "listen": "17410",
        "region": "cn_bj",
        "domains": [
            "object.cfs.local"
        ],
        "logDir": "/cfs/Logs/objectnode",
        "logLevel": "info",
        "masterAddr": [
            "10.196.59.198:17010",
            "10.196.59.199:17010",
            "10.196.59.200:17010"

        ],
        "exporterPort": 9503,
        "prof": "7013"
   }

Fetch Authentication Keys
----------------------------

Use Command Line Interface (CLI) tool to get user's AccessKey and SecretKey:

.. code-block:: bash

    $ cli user info USER_ID


Using Object Storage Interface
-------------------------------
Object Subsystem (ObjectNode) provides S3-compatible object storage interface, so that you can operate files by using native Amazon S3 SDKs.

For detail about list of supported APIs, see **Supported S3-compatible APIs** at :doc:`/design/objectnode`

For detail about list of supported SDKs, see **Supported SDKs** at :doc:`/design/objectnode`

Using S3cmd
***********

Use s3cmd to access the ObjectNode deployed locally.

**Installation**

Install the ``s3cmd`` from https://s3tools.org/s3cmd .

**Configuration**

Edit s3cmd configuration file ``$HOME/.s3cfg``

.. code-block:: bash

    host_base = 127.0.0.1
    host_bucket = 127.0.0.1
    use_https = False
    access_key = YOUR_ACCESS_KEY
    secret_key = YOUR_SECRET_KEY

**Example: making a bucket (volume)**

.. code-block:: bash

    s3cmd mb s3://my_volume
    Bucket 's3://my_volume/' created

**Example: uploading an local file to ChubaoFS**

.. code-block:: bash

    s3cmd put data.dat s3://my_volume
    upload: 'data.dat' -> 's3://my_volume/data.dat'

**Example: listing buckets (volumes)**

.. code-block:: bash

    s3cmd ls
    2020-05-10 15:29 s3://my_volume

**Example: listing files stored in a ChubaoFS volume**

.. code-block:: bash

    s3cmd ls s3://my_volume
                             DIR    s3://my_volume/backup/
    2020-05-10  15:31   10485760    s3://my_volume/data.dat
    2020005-10  15:33         10    s3://my_volume/hello.txt

**Example: deleting file stored in a ChubaoFS volume**

.. code-block:: bash

    s3cmd rm s3://my_volume/data.dat
    delete: 's3://my_volume/data.dat'


Detail usage for ``s3cmd`` see https://s3tools.org/usage .


Using AWS Java SDK
******************

Use AWS Java SDK to access the ObjectNode deployed locally.

**Install by Maven:**

.. code-block:: xml

    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>s3</artifactId>
        <version>2.10.71</version>
    </dependency>


**Example: uploading file to ChubaoFS volume (PutObject)**

.. code-block:: java

    Regions clientRegion = Region.of("*** Region name ***"); // Setup region
    String endpoint = "http://127.0.0.1"; // Setup endpoint
    String accessKey = "*** Access Key ***"; // Setup AccessKey
    String secretKey = "*** Secret Key ***"; // Setup SecretKey
    String bucketName = "*** Bucket name ***"; // Setup target bucket (ChubaoFS Volume)
    String objectKey = "*** File object key name ***"; // Setup object key
    []byte data = "*** Example File Data as String **".getBytes();

    try {
        // Setup credential
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));

        // Init S3 client
        S3Configuration configuration = S3Configuration.builder()
            .chunkedEncodingEnabled(true)
            .build();
        S3Client client = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(URI.create(endpoint))
            .serviceConfiguration(configuration)
            .build();

        // Upload file
        PutObjectRequest request = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(objectKey)
            .build();
        RequestBody body = RequestBody.fromBytes(data);
        s3Client.putObject(request, body)
    } catch (Exception e) {
        e.printStackTrace();
    }



**Example: downloading file stored in ChubaoFS volume (GetObject)**

.. code-block:: java

    Regions clientRegion = Region.of("*** Region name ***"); // Setup region
    String endpoint = "http://127.0.0.1"; // Setup endpoint
    String accessKey = "*** Access Key ***"; // Setup AccessKey
    String secretKey = "*** Secret Key ***"; // Setup SecretKey
    String bucketName = "*** Bucket name ***"; // Setup target bucket (ChubaoFS Volume)
    String objectKey = "*** File object key name ***"; // Setup object key

    try {
        // Setup credential
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));

        // Init S3 client
        S3Configuration configuration = S3Configuration.builder()
            .chunkedEncodingEnabled(true)
            .build();
        S3Client client = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(URI.create(endpoint))
            .serviceConfiguration(configuration)
            .build();

        // Get file data
        GetObjectRequest request = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(objectKey)
            .build();

        InputStream is = s3Client.getObject(request)
        while (true) {
            if (is.read() == -1) {
                break
            }
        }
    } catch (Exception e) {
        e.printStackTrace();
    }



Using AWS Python SDK (Boto3)
****************************

Use AWS Python SDK (Boto3) to access the ObjectNode deployed locally.

**Install Boto3 by PIP:**

.. code-block:: bash

    $ pip install boto3

**Example: uploading file to ChubaoFS volume (PutObject)**

.. code-block:: python

    import boto3

    endpoint = " ** endpoint url ** "  # example: http://127.0.0.1
    region_name = " ** region name ** "
    access_key = " ** your access key ** "  # your access key
    secret_key = " ** your secret key ** "  # your secret key
    bucket = " ** your bucket (volume) ** "  # your volume name
    key = " ** your object key (file path in CFS) ** "  # your object name

    def put_file():
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)
        client = session.client(service_name="s3", region_name=region_name, endpoint_url=endpoint)
        client.put_object(Bucket=bucket, Key=key, Body=bytes("hello world"))

**Example: downloading file stored in ChubaoFS volume (GetObject)**

.. code-block:: python

    import boto3

    endpoint = " ** endpoint url ** "  # example: http://127.0.0.1
    region_name = " ** region name ** "
    access_key = " ** your access key ** "  # your access key
    secret_key = " ** your secret key ** "  # your secret key
    bucket = " ** your bucket (volume) ** "  # your volume name
    key = " ** your object key (file path in CFS) ** "  # your object name

    def get_file():
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)
        client = session.client(service_name="s3", region_name=region_name, endpoint_url=endpoint)
        result = client.get_object(Bucket=bucket, Key=key)
        print(result["Body"].read())

