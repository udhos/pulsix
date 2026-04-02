# pulsix

[pulsix](https://github.com/udhos/pulsix) is a high-performance, serverless streaming framework built on S3 as Blob Storage and on SQS as Reliable Notification Service.

We only support AWS for now.

We only support Golang for now.

# Why pulsix?

- **Cost-Effective**: Zero idle cost. Pay for data moved, not for cluster uptime. Need to move billions of messages? We got you covered.

- **Reliable**: Built on S3’s 11-nines of durability and SQS’s guaranteed delivery.

- **Scalable**: Inherits the huge elasticity of AWS serverless primitives.

- **High-Throughput**: Engineered to handle tens of thousands of messages per second as baseline.

- **Near Real-Time**: Configurable "Pulse" window (e.g., 1 second) provides a sweet balance of latency and batch efficiency.

- **Truly Serverless**: No brokers to manage, no shards to re-balance.

# How it works

## Producer

Producers inject the messages with a pulse window. When the publish API returns without error, the message is reliably accepted for delivery.

Internally, the publish API writes messages in batches to S3, triggering a SQS notification for each batch. The SQS message contains the S3 object key, which serves as the pointer to the batch of messages.

## Consumer

Consumers listen for SQS notifications and fetch the corresponding batch of messages from S3 for processing.

## Storage Format

Pulsix uses a Streamable Length-Prefixed format. Each record is prefixed with its size, allowing consumers to stream and parse messages from S3 with zero-copy efficiency and no need to load the entire batch into memory.

```bash
PULSIX-SIZE:<N1>\n<N1_PAYLOAD_BYTES>
PULSIX-SIZE:<N2>\n<N2_PAYLOAD_BYTES>
```

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
