# pulsix

[pulsix](https://github.com/udhos/pulsix) is a high-performance streaming framework built on S3 as Blob Storage and on SQS as Reliable Notification Service.

We only support AWS for now.

We only support Golang for now.

# Why pulsix?

- **Cost-Effective**: Zero idle cost. Pay for data moved, not for cluster uptime. Need to move billions of messages? We got you covered.

- **Reliable**: Built on S3’s 11-nines of durability and SQS’s guaranteed delivery.

- **Scalable**: Inherits the huge elasticity of AWS serverless primitives.

- **High-Throughput**: Engineered to handle tens of thousands of messages per second as baseline.

- **Near Real-Time**: Configurable "Pulse" window (e.g., 1 second) provides a sweet balance of latency and batch efficiency.

- **Based on Serverless**: Leverage serverless nature of S3 and SQS. No brokers to manage. No shards to re-balance.

# How it works

## Producer

Producers group messages into batches. When the SendBatch API returns without error, the data is guaranteed to be durable in S3 and visible to consumers via SQS.

The SendBatch API writes messages in batches to S3, triggering a SQS notification for each batch. The SQS message contains the S3 object key, which serves as the pointer to the batch of messages.

Pulsix uses a Zero-Copy Streaming approach. It leverages a MultiReader to pipe message slices directly from the application to AWS.

Internally, Pulsix utilizes the AWS S3 Transfer Manager to handle multi-part uploads and automatic retries, ensuring that even gigabyte-scale batches are handled with a constant, minimal memory footprint.

## Consumer

Consumers listen for SQS notifications and fetch the corresponding batch of messages from S3 for processing.

## Storage Format

1 - One file stores one batch of messages.

2 - A file is a sequence of records.

3 - Every record has this format:

```bash
<2-bytes version><record>
```

4 - The first version is `p1` (pulsix version 1). So the first two bytes of every record are `0x70 0x31` (ASCII "p1").

5 - p1 record is defined as:

```bash
p1:<total_record_length>:<tlv1><tlv2>...<tlvn>
```

p1 record holds a single message.

`<total_record_length>` is the total length in ascii decimal, like "1234".

`<total_record_length>` is always surrounded by `:`.

The total_record_length accounts exactly the full number of bytes AFTER the `:` that follows the total_record_length, and up to-and-including the last TLV byte of the record.

That is to say the total_record_length is the byte-length of the list of TLVs, excluding the 2 bytes of version and the `<total_record_length>:` field itself.

tlv is defined as:

Each TLV field holds a piece of the message.

```bash
<type>:<length>:<value>
```

`<type>` is 1 byte. We define 3 types that are ascii friendly for now:

- Type 'm' means internal metadata.
- Type 'a' means user defined attributes.
- Type 'd' means the actual user message data.

For `m` and `a`, the value encoding is explicit and currently uses a single-byte marker:

```bash
m:<length>:j:<value>
a:<length>:j:<value>
```

`j` means JSON encoding.

Length is the length of the value in ascii decimal, like "1234".
Length is always surrounded by `:`.
Similar to total_record_length, the length field accounts exactly the byte-length of the value field.

### Storage Format Example

**Input Data:**
- User Attributes: `{"a":"b"}` (9 bytes)
- User Data: `hello` (5 bytes)

**Breakdown:**
- **Prefix:** `p1:24:` (The `24` represents the sum of all TLV bytes following this colon)
- **TLV 1 (Attributes):** `a:9:j:{"a":"b"}` (6 bytes of overhead + 9 bytes value = 15 bytes)
- **TLV 2 (Data):** `d:5:hello` (4 bytes of overhead + 5 bytes value = 9 bytes)

**Final Wire Record:**
`p1:24:a:9:j:{"a":"b"}d:5:hello`

**Multiple Messages:**
A record transports a single message.
If a producer batches two identical messages:
`p1:24:a:9:j:{"a":"b"}d:5:hellop1:24:a:9:j:{"a":"b"}d:5:hello`

# How to setup cross-account access

- Account A (Producer): Owns the S3 Bucket.
- Account B (Consumer): Owns the SQS Queue and the Subscriber workers.

## Step 1: The SQS Access Policy (Account B)

The SQS queue must explicitly allow the S3 service from Account A to send messages to it. Without this, the "Pulse" will never reach your consumer.

Queue Policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "s3.amazonaws.com" },
      "Action": "SQS:SendMessage",
      "Resource": "arn:aws:sqs:region:ACCOUNT_B_ID:pulsix-queue",
      "Condition": {
        "ArnLike": { "aws:SourceArn": "arn:aws:s3:::pulsix-bucket-account-a" }
      }
    }
  ]
}
```

## Step 2: The S3 Bucket Policy (Account A)

The Consumer workers in Account B need permission to GetObject from the bucket in Account A.

Bucket Policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::ACCOUNT_B_ID:role/pulsix-consumer-role" },
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::pulsix-bucket-account-a",
        "arn:aws:s3:::pulsix-bucket-account-a/*"
      ]
    }
  ]
}
```

## Step 3: Enable S3 Event Notifications

Once the policies are in place, you configure the S3 bucket in **Account A** to send a notification to the SQS ARN in **Account B** whenever a `.batch` file is created.

# Bucket Lifecycle

Do not forget to set a lifecycle policy on the S3 bucket to clean up old batches.

Pulsix does not automatically delete the batch files from S3, so this is crucial to prevent storage bloat.

Example lifecycle policy:

```json
{
  "Rules": [
    {
      "ID": "PulsixShortTermStorage",
      "Filter": {
        "Prefix": "events/"
      },
      "Status": "Enabled",
      "Expiration": {
        "Days": 7
      },
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 1
      }
    }
  ]
}
```

Save the above JSON to a file named `lifecycle.json` and apply it to your bucket with the AWS CLI:

```bash
aws s3api put-bucket-lifecycle-configuration \
    --bucket your-pulsix-bucket-name \
    --lifecycle-configuration file://lifecycle.json
```

# Running the example clients

```bash
# publisher
BUCKET=bucket-name pulsix-pub-aws

# consumer
BUCKET=bucket-name QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123412341234/queue-name pulsix-sub-aws
```

# TODO

- [ ] Benchmark tests.
- [ ] Large scale testing on AWS.
- [ ] Metrics.
- [ ] Replace DeleteMessage with DeleteMessageBatch for better efficiency.
- [ ] Review logs.
- [ ] Dispatcher is an app/service/daemon that consumes Pulsix and directs to other systems (possible targets: another Pulsix, SNS, SQS, S3).
- [ ] Sample injection tool (reads from SQS, injects into Pulsix).
- [x] Add explicit encoding for metadata and attribute.
- [ ] Add primary API that automatically accumulates messages into batches and flushes them on limited periods. It must somehow signal the caller when specific messages were secured into reliable delivery, allowing the caller to mark them as delivered.
