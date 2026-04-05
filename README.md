# pulsix

[pulsix](https://github.com/udhos/pulsix) is a high-performance streaming framework built on S3 as Blob Storage and on SQS as Reliable Notification Service.

We only support AWS for now.

We only support Golang for now.

* [Why pulsix?](#why-pulsix)
* [How it works](#how-it-works)
  * [Producer](#producer)
    * [Send API](#send-api)
    * [SendBatch API](#sendbatch-api)
  * [Consumer](#consumer)
  * [Storage Format](#storage-format)
    * [Storage Format Example](#storage-format-example)
* [How to setup cross\-account access](#how-to-setup-cross-account-access)
  * [Step 1: The SQS Access Policy (Account B)](#step-1-the-sqs-access-policy-account-b)
  * [Step 2: The S3 Bucket Policy (Account A)](#step-2-the-s3-bucket-policy-account-a)
  * [Step 3: Enable S3 Event Notifications](#step-3-enable-s3-event-notifications)
* [Bucket Lifecycle](#bucket-lifecycle)
* [Running the example clients](#running-the-example-clients)
* [TODO](#todo)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)

# Why pulsix?

- **Cost-Effective**: Zero idle cost. Pay for data moved, not for cluster uptime. Need to move billions of messages? We got you covered.

- **Reliable**: Built on S3’s 11-nines of durability and SQS’s guaranteed delivery.

- **Scalable**: Inherits the huge elasticity of AWS serverless primitives.

- **High-Throughput**: Engineered to handle tens of thousands of messages per second as baseline.

- **Near Real-Time**: Configurable "Pulse" window (e.g., 1 second) provides a sweet balance of latency and batch efficiency.

- **Based on Serverless**: Leverage serverless nature of S3 and SQS. No brokers to manage. No shards to re-balance.

# How it works

## Producer

### Send API

The primary sending API is `Send()`. It accumulates messages automatically and flushes them in batches based on configured thresholds (age, message count, bytes).

ABSTRACT

From the caller's perspective, there are three sets of messages:

1. Unsent: Messages yet to be sent to `Send()`.
2. Unacked: Messages sent via `Send()` but not yet acknowledged as durable.
3. Acked: Messages acknowledged as durably persisted.

The abstract caller's role is two-fold:

1. Move messages from Unsent to Unacked by calling `Send()`.
2. Move messages from Unacked to Acked by reading acknowledgments from `AckChan()`.

Consider these important rules for role 2:

1. When an AckedUpTo is received from AckChan, the caller must move all messages with ID <= AckedUpTo from Unacked to Acked.
2. When an error is received from AckChan, all messages in Unacked must be reverted to Unsent.
3. The caller must continuously drain the AckChan because failing to do so might eventually stall the sender progress.

SYNOPSIS

`Send()` is goroutine-safe and returns a monotonically increasing ID per message.

Batch durability is reported through acknowledgments carrying `AckedUpTo`.

If an acknowledgment reports `AckedUpTo = X`, then all messages with ID `<= X` are durably persisted.

Typical flow:

1. Call `Send(msg)` for each message and keep returned IDs as needed by your app.
2. Read acknowledgments and treat IDs `<= AckedUpTo` as safely delivered to Pulsix durable storage.
3. If `Ack.Err` is reported, stop expecting further durability for unacked IDs and decide whether to retry them.
4. Call `Close()` during shutdown to flush pending messages and release resources.

Example setup and send loop:

```go
ctx := context.Background()

sender := pub.NewSender(pub.SendOptions{
  Options: pub.Options{
    Storage: store,
    Prefix:  "events",
  },
  FlushThresholdAge:      time.Second,
  FlushThresholdMessages: 10_000,
  FlushThresholdBytes:    50 * 1024 * 1024,
  AckChannelSize:         100,
})

id, err := sender.Send(ctx, pulsix.Message{Data: []byte("hello")})
```

Example acknowledgment handling:

```go
for ack := range sender.AckChan() {
  if ack.Err != nil {
    // hard-fail boundary: caller should treat IDs > last successful
    // AckedUpTo as not durably confirmed and retry if needed.
    log.Printf("terminal boundary: %v", ack.Err)
    continue
  }

  // all IDs <= ack.AckedUpTo are durable
}

```

`Close()` is used to stop the sender and wait for shutdown.

Note: the sender uses blocking ack delivery. Callers must keep draining `AckChan`;
if `AckChan` is not drained, sender progress can stall once the ack buffer is full.

### SendBatch API

`SendBatch()` is available as a lower-level synchronous primitive, but `Send()` is the main API recommended for general usage.

When `SendBatch()` returns without error, the data in that explicit batch is guaranteed to be durable in S3 and eventually visible to consumers via SQS.

The SendBatch API writes messages in batches to S3, triggering a SQS notification for each batch. The SQS message contains the S3 object key, which serves as the pointer to the batch of messages.

Pulsix uses a Zero-Copy Streaming approach. It leverages a MultiReader to pipe message slices directly from the application to AWS.

Internally, Pulsix utilizes the AWS S3 Transfer Manager to handle multi-part uploads and automatic retries, ensuring that even gigabyte-scale batches are handled with a constant, minimal memory footprint.

## Consumer

Consumers listen for SQS notifications and fetch the corresponding batch of messages from S3 for processing.

One critical issue in consuming logic is to process all messages in the batch and then calling `Done()` before the SQS Visibility Timeout expires. If `Done()` is not called in time, the batch will be re-delivered, which can lead to duplicate processing. Benchmark that your consumer can process the batch within the Visibility Timeout (adjust the SQS timeout if needed).

SYNOPSIS

```golang
batches, err := subscriber.Receive(ctx)

// read forever
for {
  // scan batches for messages
  for _, b := range batches {
    // handle one batch

    for b.Next() {
      msg := b.Message()
      // handle message in msg
    }

    // Tell Pulsix we are done with this batch,
    // deleting the notification from SQS.
    err := b.Done()
  }
}
```

## Storage Format

1 - One file stores one batch of messages.

2 - A file begins with a single version prefix and then contains a sequence of records.

3 - A file has this format:

```bash
<2-bytes version>:<record><record>...<record>
```

4 - The first version is `p1` (pulsix version 1). So every p1 file starts with `0x70 0x31 0x3a` (ASCII `p1:`).

5 - p1 record is defined as:

```bash
<total_record_length>:<tlv1><tlv2>...<tlvn>
```

A p1 record holds a single message.

`<total_record_length>` is the total length in ascii decimal, like "1234".

`<total_record_length>` is always surrounded by `:`.

The total_record_length accounts exactly the full number of bytes AFTER the `:` that follows the total_record_length, and up to-and-including the last TLV byte of the record.

That is to say the total_record_length is the byte-length of the list of TLVs, excluding the file version prefix and the `<total_record_length>:` field itself.

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
Similar to total_record_length, the length field accounts exactly the byte-length of the TLV payload field.
For `m` and `a`, this payload is `<encoding>:<value>`, so length includes the `j:` marker.
For `d`, this payload is `<value>`.

### Storage Format Example

**Input Data:**
- User Attributes: `{"a":"b"}` (9 bytes)
- User Data: `hello` (5 bytes)

**Breakdown:**
**File Prefix:** `p1:`
- **Record Prefix:** `25:` (The `25` represents the sum of all TLV bytes following this colon)
- **TLV 1 (Attributes):** `a:11:j:{"a":"b"}` (7 bytes of overhead + 9 bytes value = 16 bytes)
- **TLV 2 (Data):** `d:5:hello` (4 bytes of overhead + 5 bytes value = 9 bytes)

**Final Wire File With One Message:**
`p1:25:a:11:j:{"a":"b"}d:5:hello`

**Multiple Messages:**
A record transports a single message.
If a producer batches two identical messages:
`p1:25:a:11:j:{"a":"b"}d:5:hello25:a:11:j:{"a":"b"}d:5:hello`

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
- [x] Add primary API that automatically accumulates messages into batches and flushes them on limited periods. It must somehow signal the caller when specific messages were secured into reliable delivery, allowing the caller to mark them as delivered.
