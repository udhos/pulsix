# pulsix

[pulsix](https://github.com/udhos/pulsix) is a high-performance streaming framework built on S3 as Blob Storage and on SQS as Reliable Notification Service.

We only support AWS for now.

We only support Golang for now.

* [Why pulsix?](#why-pulsix)
* [How it works](#how-it-works)
  * [Producer](#producer)
    * [Package inject](#Package-inject)
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
* [Programs](#programs)
* [Running the example clients](#running-the-example-clients)
* [FAQ](#faq)
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

There are three producing APIs, from high-level to low-level:

1. Package inject
2. Package pub Sender with Send() API
3. Package pub with SendBatch() API

### Package inject

The `Injector` from the `inject` package runs the state-machine to move messages across the states defined in the Send API. All the Injector needs is a channel of messages to be sent and a callback to report when messages are durably persisted. It strives to provide a simpler interface on top of the complex Send API, abstracting away the details of batching and acknowledgment handling.

### Send API

The API `Send()` accumulates messages automatically and flushes them in batches based on configured thresholds (age, message count, bytes).

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
2. Read acknowledgments and treat all IDs `<= AckedUpTo` as safely delivered to Pulsix durable storage.
3. If `Ack.Err` is reported, stop expecting further durability for unacked IDs and treat them as not sent at all.
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
  FlushThresholdBytes:    50 * 1024 * 1024,
  AckChannelSize:         100,
})

id, err := sender.Send(ctx, pulsix.Message{Data: []byte("hello")})
```

`Close()` is used to stop the sender and wait for shutdown.

Note: the sender uses blocking ack delivery. Callers must keep draining `AckChan`;
if `AckChan` is not drained, sender progress can stall once the ack buffer is full.

### SendBatch API

`SendBatch()` is available as a lower-level synchronous primitive.

When `SendBatch()` returns without error, the data in that explicit batch is guaranteed to be durable in S3 and eventually visible to consumers via SQS.

`SendBatch()` is easier to use but its direct usage is discouraged because it allows for inefficient small batches.

The SendBatch API writes messages in batches to S3, triggering a SQS notification for each batch. The SQS message contains the S3 object key, which serves as the pointer to the batch of messages.

Pulsix uses a Zero-Copy Streaming approach. It leverages a MultiReader to pipe message slices directly from the application to AWS.

Internally, Pulsix utilizes the AWS S3 Transfer Manager to handle multi-part uploads and automatic retries, ensuring that even gigabyte-scale batches are handled with a constant, minimal memory footprint.

## Consumer

Consumers listen for SQS notifications and fetch the corresponding batch of messages from S3 for processing.

One critical aspect in consuming logic is to process all messages in the batch and then calling `Done()` before the SQS Visibility Timeout expires. If `Done()` is not called in time, the batch will be re-delivered, which can lead to duplicate processing. Benchmark that your consumer can process the batch within the Visibility Timeout. A good "rule of thumb": Set SQS Visibility Timeout to at least 3x your expected batch processing time. This will prevent SQS from re-delivering batches before your consumer has a chance to call `Done()`, thus avoiding unnecessary duplicates.

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

    // Tell Pulsix we are done with this WHOLE batch
    // (not individual messages), causing deletion of
    // the notification from SQS.
    err := b.Done()
  }
}
```

## Storage Format

1 - One file stores one batch of messages.

Batch object key in S3 has the following structure:

    <prefix>/YYYY-MM/DD/HH/MM/<random_ksuid>.batch

2 - A file begins with a single version prefix and then contains a sequence of records.

3 - A file has this format:

```bash
<2-bytes version>:<record><record>...<record>
```

4 - The first version is `p1` (pulsix version 1). So every p1 file starts with `0x70 0x31 0x3a` (ASCII `p1:`). p1 aims to provide a balance of nice  properties: streamable, parsing performance, self-describing, simplicity, ascii debugability possible without tools in many cases, support for 8bit clean opaque user data, some extensibility with TLVs.

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

`<type>` is 1 byte. p1 defines 3 types that are ascii friendly:

- Type 'm' means internal metadata.
- Type 'a' means user defined attributes.
- Type 'd' means the actual user message data.

For `m` and `a`, the value encoding is an explicit single-byte marker.

```bash
m:<length>:<encoding>:<value>
a:<length>:<encoding>:<value>

# j stands for JSON encoding:
m:<length>:j:<value>
a:<length>:j:<value>

# k stands for key-value encoding:
m:<length>:k:<value>
a:<length>:k:<value>
```

- `j` stands for JSON encoding.
- `k` stands for key-value encoding.

**Encoding support status:**
Support for `j` is currently mandatory for `m` and `a` for both producer and consumer.
Support for `k` is optional and experimental.

Length is the length of the value in ascii decimal, like "1234".
Length is always surrounded by `:`.
Similar to total_record_length, the length field accounts exactly the byte-length of the TLV payload field.
For `m` and `a`, this payload is `<encoding>:<value>`, so length includes the `<encoding>:` marker.
For `d`, this payload is `<value>`.

The `j` encoding is plain JSON. For example, the metadata `id=1234` would be encoded as `m:25:j:{"id":"1234"}`.

The `k` encoding is a sequence of key-value pairs encoded as `<key-length>:<key-data><value-length>:<value-data>`.

Example:

- encoding: `k`
- encoding prefix: `k:` (2 bytes)
- attribute1: key=value => `3:key5:value` (12 bytes)
- attribute2: kk=vvv => `2:kk3:vvv` (9 bytes)

Total size: 2 (encoding prefix) + 12 (attribute1) + 9 (attribute2) = 23 bytes

Then the attribute TLV would be: `a:23:k:3:key5:value2:kk3:vvv`

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
8
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

# Programs

We provide some programs in the `cmd` directory.

Program | Status | Description
--- | --- | ---
`pulsix-pub-aws`        | ✅ Ready.   | Example producer that sends messages to Pulsix on AWS.
`pulsix-sub-aws`        | ✅ Ready.   | Example consumer that receives messages from Pulsix on AWS.
`pulsix-pub-example`    | ✅ Ready.   | Example producer that sends messages to Pulsix using filesystem storage (for testing).
`pulsix-sub-example`    | ✅ Ready.   | Example consumer that receives messages from Pulsix using filesystem storage (for testing).
`pulsix-ingress-sqs`    | ✅ Ready.   | Sample ingress tool that reads from SQS and injects into Pulsix using package inject.
`pulsix-ingress-random` | ✅ Ready.   | Reference ingress model that generates random batches and injects them into Pulsix using package inject.
`pulsix-bench`          | ✅ Ready.   | Benchmark tool to profile end-to-end flow from producer to consumer, measuring latency and throughput under various Pulsix parameters.
`pulsix-dispatcher`     | 🛠️ Planned. | It will forward messages from Pulsix to other systems (SNS, SQS, another Pulsix, etc). Important features: fanout, filtering.

# Running the example clients

```bash
# publisher
BUCKET=bucket-name pulsix-pub-aws

# consumer
BUCKET=bucket-name QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123412341234/queue-name pulsix-sub-aws

# inject random batches with Send API
BUCKET=bucket-name pulsix-ingress-random

# inject from source SQS into Pulsix using Send API
BUCKET=bucket-name QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123412341234/source-queue pulsix-ingress-sqs
```

# FAQ

**Q: When is Pulsix a good fit?**

Pulsix excels at providing cost-efficiency for abysmal streaming loads that can tolerate some increased latency.

- High-throughput, huge volumes.
- Cost-sensitive streaming workloads.
- Near real-time processing is acceptable (few seconds latency).
- Workloads that can tolerate duplicates or have idempotent processing.
- Preference to avoid managing clusters of brokers and shards.

**Q: When is Pulsix a bad fit?**

Pulsix might be inadequate when your events have stringent requirements that override cost considerations at scale.

- Tight low-latency budgets (sub-second).
- Strict ordering (FIFO).
- Duplicate-sensitive workloads without idempotent processing.
- You are comfortable managing clusters of brokers and shards.

**Q: What if I need both Pulsix cost efficiency and low latency?**

Consider a dual lane deployment.

- Low-cost lane: Pulsix for the main extreme-volume cost-sensitive lane.
- Low-latency lane: Low-latency streaming system (like Kafka or Kinesis) for the latency-sensitive lane.

# TODO

- [ ] Benchmark tests.
- [ ] Large scale testing on AWS.
- [ ] Metrics.
- [ ] Replace DeleteMessage with DeleteMessageBatch for better efficiency.
- [ ] Review logs.
- [ ] `pulsix-dispatcher` is an app/service/daemon that consumes Pulsix and directs to other systems (possible targets: another Pulsix, SNS, SQS, S3). Must support fanout and filtering. For example, it can read messages from Pulsix and forward to multiple SQS queues based on message attributes.
- [x] `pulsix-ingress-sqs`: sample injection tool (reads from SQS, injects into Pulsix).
- [x] Add explicit encoding for metadata and attribute.
- [x] Add primary API that automatically accumulates messages into batches and flushes them on limited periods. It must somehow signal the caller when specific messages were secured into reliable delivery, allowing the caller to mark them as delivered.
- [ ] One slow consumer = visibility timeout risk. Help the consumer to avoid duplication of messages when the Visibility Timeout expires before the consumer can call `Done()`. Make the SDK track the byte offset of the next unprocessed record in the batch. The consumer must checkpoint every message processed by calling a new API like `Checkpoint()`, which will update the checkpointed offset. When the SDK downloads a batch possibly due to Visibility Timeout expiration, it can use the checkpointed offset to skip already processed messages, thus avoiding duplicates. The SDK can detect the batch was redelivered by keeping a cache of recently processed batches. The S3 key is a perfect ID for identifying redelivered batches. Open problem: cross-pod case. Can a pod benefit from checkpointing of another pod?
- [X] Add FAQ to README to address common questions and best practices.
- [X] Write a benchmark tool `pulsix-bench` that can profile a complete end-to-end flow from producer to consumer, measuring latency and throughput under various pulsix parameters. It should support both in-memory or real AWS backends, chosen at runtime command-line flag. Its mode of operation is like this: 0) Spawn a consumer-side to receive our own messages cycled thru Pulsix. 1) Generate a number of messages to a limit. 2) Use the package `inject` to send those messages, similar to `pulsix-ingress-random`. 3) When finished all send/receive, report metrics about latency and throughput. 4) All parameters for the tool should be configurable from command-line flags, NOT environment variables.
- [X] Add experimental package `inject` that factors out the consuming logic from `pulsix-ingress-random`, making it easier to build custom ingress tools that read from other sources (Kafka, RabbitMQ, etc) and inject into Pulsix using the Send API. The package would build on the `pub.Sender`. It would take two inputs: 1) A channel for receiving messages to be sent. 2) A callback function to report the message was reliably sent.
- [X] Benchmark p1 encoding.
- [ ] Benchmark p1 decoding.
- [ ] If benchmarking proves we have much room for improvement, consider faster encoding formats for TLV types `m` and `a`, which currently only use JSON.
- [ ] In addition to the two existing batch closing triggers (age, bytes), add a third trigger: a silence in incoming messages. `batchCloseSilenceDuration`. If there is no new message to be sent for a configured duration, the current batch will be flushed. This adds natural batching. When messages are coming in bursts, they will be efficiently batched by the existing triggers. When messages are coming in a slow trickle, the silence trigger will ensure they don't get stuck in limbo for too long waiting for the other triggers to fire.
- [X] Review we are parsing the batch file in a streaming way, without loading the whole batch into memory. If not, refactor the code to achieve that.
- [ ] Design a StreamBatch API that starts uploading a batch as soon as the first message is sent, and then keeps streaming messages into that batch until it is closed. We would use the usual three signals (age, bytes, silence) to close the batch. This design has potential to bring down latency. Start by creating tests to validate the latency reduction compared to the existing  SendBatch API.
- [ ] Discover if we need to add some support for DLQ.
- [X] Remove message count as batch limit.

# Benchmark

## Simulation

```bash
sudo mount -t tmpfs -o size=2g tmpfs /mnt/ramdisk
```

```bash
#
# 1000 bytes
#
pulsix-bench -sim-base-dir /mnt/ramdisk/pulsix -disable-message-id -payload-size 1000 -messages 200000 -flush-bytes 100000000
2026/04/15 00:53:35 sim backend dir kept at: /mnt/ramdisk/pulsix
📣 SQS: Notifying new batch at events/2026-04/15/03/53/3CNTqCJ1tIHDjjQLLm5NbX0oRLp.batch
2026-04-15T00:53:36.540638172-03:00 - batch events/2026-04/15/03/53/3CNTqCJ1tIHDjjQLLm5NbX0oRLp.batch upload completed in 423.1012ms
2026-04-15T00:53:36.583304534-03:00 - recv SQS notification for batch events/2026-04/15/03/53/3CNTqCJ1tIHDjjQLLm5NbX0oRLp.batch
2026-04-15T00:53:36.670488573-03:00 - full batch parsing for events/2026-04/15/03/53/3CNTqCJ1tIHDjjQLLm5NbX0oRLp.batch took 87.155516ms (100000 matching messages)
2026-04-15T00:53:36.670544188-03:00 - batch events/2026-04/15/03/53/3CNTqCJ1tIHDjjQLLm5NbX0oRLp.batch stream first-byte delay: 16.134µs
2026-04-15T00:53:36.670547807-03:00 - batch events/2026-04/15/03/53/3CNTqCJ1tIHDjjQLLm5NbX0oRLp.batch stream download took 87.145996ms (101200003 bytes)
2026-04-15T00:53:36.670550106-03:00 - batch events/2026-04/15/03/53/3CNTqCJ1tIHDjjQLLm5NbX0oRLp.batch notify->get delay: 20.262µs
📣 SQS: Notifying new batch at events/2026-04/15/03/53/3CNTqGA4fFAQmOWaEG8MzVQWHMU.batch
2026-04-15T00:53:37.008476692-03:00 - batch events/2026-04/15/03/53/3CNTqGA4fFAQmOWaEG8MzVQWHMU.batch upload completed in 384.676814ms
2026-04-15T00:53:37.071703302-03:00 - recv SQS notification for batch events/2026-04/15/03/53/3CNTqGA4fFAQmOWaEG8MzVQWHMU.batch
2026-04-15T00:53:37.211180603-03:00 - full batch parsing for events/2026-04/15/03/53/3CNTqGA4fFAQmOWaEG8MzVQWHMU.batch took 139.42493ms (100000 matching messages)
2026-04-15T00:53:37.211266349-03:00 - batch events/2026-04/15/03/53/3CNTqGA4fFAQmOWaEG8MzVQWHMU.batch stream first-byte delay: 31.358µs
2026-04-15T00:53:37.211272065-03:00 - batch events/2026-04/15/03/53/3CNTqGA4fFAQmOWaEG8MzVQWHMU.batch stream download took 139.3993ms (101200003 bytes)
2026-04-15T00:53:37.211275398-03:00 - batch events/2026-04/15/03/53/3CNTqGA4fFAQmOWaEG8MzVQWHMU.batch notify->get delay: 44.985µs
=== pulsix-bench report ===
backend:              sim
messages target:      200000
payload size:         1000 bytes
elapsed:              1.232707957s
produced:             200000
acked:                200000
consumed unique:      200000
duplicates seen:      0
batches received:     2
throughput:           162244.43 msg/s
throughput payload:   154.73 MiB/s
throughput wire:      156.59 MiB/s
latency min:          555.717917ms
latency avg:          614.818223ms
latency p50:          572.317227ms
latency p95:          961.221973ms
latency p99:          965.743902ms
latency max:          966.401051ms

#
# 10000 bytes
#
pulsix-bench -sim-base-dir /mnt/ramdisk/pulsix -disable-message-id -payload-size 10000 -messages 200000 -flush-bytes 100000000
2026/04/15 00:54:14 sim backend dir kept at: /mnt/ramdisk/pulsix
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTuzSPGX7PCA6MrLZbP8LY0sb.batch
2026-04-15T00:54:14.631672546-03:00 - batch events/2026-04/15/03/54/3CNTuzSPGX7PCA6MrLZbP8LY0sb.batch upload completed in 70.328814ms
2026-04-15T00:54:14.694168208-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTuzSPGX7PCA6MrLZbP8LY0sb.batch
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTuzICcqexb17HJWc99P8EBrd.batch
2026-04-15T00:54:14.707697599-03:00 - batch events/2026-04/15/03/54/3CNTuzICcqexb17HJWc99P8EBrd.batch upload completed in 73.601194ms
2026-04-15T00:54:14.749590834-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTuzSPGX7PCA6MrLZbP8LY0sb.batch took 55.376892ms (10000 matching messages)
2026-04-15T00:54:14.74964908-03:00 - batch events/2026-04/15/03/54/3CNTuzSPGX7PCA6MrLZbP8LY0sb.batch stream first-byte delay: 18.008µs
2026-04-15T00:54:14.749652983-03:00 - batch events/2026-04/15/03/54/3CNTuzSPGX7PCA6MrLZbP8LY0sb.batch stream download took 55.366758ms (100140003 bytes)
2026-04-15T00:54:14.749655836-03:00 - batch events/2026-04/15/03/54/3CNTuzSPGX7PCA6MrLZbP8LY0sb.batch notify->get delay: 36.395µs
2026-04-15T00:54:14.749707201-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTuzICcqexb17HJWc99P8EBrd.batch
2026-04-15T00:54:14.790159944-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTuzICcqexb17HJWc99P8EBrd.batch took 40.442101ms (10000 matching messages)
2026-04-15T00:54:14.790228963-03:00 - batch events/2026-04/15/03/54/3CNTuzICcqexb17HJWc99P8EBrd.batch stream first-byte delay: 3.571µs
2026-04-15T00:54:14.790235215-03:00 - batch events/2026-04/15/03/54/3CNTuzICcqexb17HJWc99P8EBrd.batch stream download took 40.439909ms (100140003 bytes)
2026-04-15T00:54:14.790237602-03:00 - batch events/2026-04/15/03/54/3CNTuzICcqexb17HJWc99P8EBrd.batch notify->get delay: 6.57µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTuz8opddG9u2dMMGyALmZWqB.batch
2026-04-15T00:54:14.795028101-03:00 - batch events/2026-04/15/03/54/3CNTuz8opddG9u2dMMGyALmZWqB.batch upload completed in 83.581831ms
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv5j0R2zPAuYwDsea0JjRr0h.batch
2026-04-15T00:54:14.862816076-03:00 - batch events/2026-04/15/03/54/3CNTv5j0R2zPAuYwDsea0JjRr0h.batch upload completed in 65.364349ms
2026-04-15T00:54:14.890384801-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTuz8opddG9u2dMMGyALmZWqB.batch
2026-04-15T00:54:14.890399673-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv5j0R2zPAuYwDsea0JjRr0h.batch
2026-04-15T00:54:14.929539506-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTuz8opddG9u2dMMGyALmZWqB.batch took 39.11646ms (10000 matching messages)
2026-04-15T00:54:14.929592118-03:00 - batch events/2026-04/15/03/54/3CNTuz8opddG9u2dMMGyALmZWqB.batch stream first-byte delay: 13.8µs
2026-04-15T00:54:14.929597603-03:00 - batch events/2026-04/15/03/54/3CNTuz8opddG9u2dMMGyALmZWqB.batch stream download took 39.111324ms (100140003 bytes)
2026-04-15T00:54:14.929599546-03:00 - batch events/2026-04/15/03/54/3CNTuz8opddG9u2dMMGyALmZWqB.batch notify->get delay: 27.935µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTuzv4hgwJvG0CxvcZV5mrdbS.batch
2026-04-15T00:54:14.937700348-03:00 - batch events/2026-04/15/03/54/3CNTuzv4hgwJvG0CxvcZV5mrdbS.batch upload completed in 72.404058ms
2026-04-15T00:54:14.969144159-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv5j0R2zPAuYwDsea0JjRr0h.batch took 39.541671ms (10000 matching messages)
2026-04-15T00:54:14.969200457-03:00 - batch events/2026-04/15/03/54/3CNTv5j0R2zPAuYwDsea0JjRr0h.batch stream first-byte delay: 39.182216ms
2026-04-15T00:54:14.969203937-03:00 - batch events/2026-04/15/03/54/3CNTv5j0R2zPAuYwDsea0JjRr0h.batch stream download took 39.538537ms (100140003 bytes)
2026-04-15T00:54:14.969205814-03:00 - batch events/2026-04/15/03/54/3CNTv5j0R2zPAuYwDsea0JjRr0h.batch notify->get delay: 21.89µs
2026-04-15T00:54:14.969254952-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTuzv4hgwJvG0CxvcZV5mrdbS.batch
2026-04-15T00:54:15.010353543-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTuzv4hgwJvG0CxvcZV5mrdbS.batch took 41.088727ms (10000 matching messages)
2026-04-15T00:54:15.010417445-03:00 - batch events/2026-04/15/03/54/3CNTuzv4hgwJvG0CxvcZV5mrdbS.batch stream first-byte delay: 3.7µs
2026-04-15T00:54:15.010422537-03:00 - batch events/2026-04/15/03/54/3CNTuzv4hgwJvG0CxvcZV5mrdbS.batch stream download took 41.085799ms (100140003 bytes)
2026-04-15T00:54:15.010424655-03:00 - batch events/2026-04/15/03/54/3CNTuzv4hgwJvG0CxvcZV5mrdbS.batch notify->get delay: 6.331µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv3lRxBw09VdY3voraJh764J.batch
2026-04-15T00:54:15.0272163-03:00 - batch events/2026-04/15/03/54/3CNTv3lRxBw09VdY3voraJh764J.batch upload completed in 87.995725ms
2026-04-15T00:54:15.110662937-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv3lRxBw09VdY3voraJh764J.batch
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv6d5jEXcMrRJZ6dMaAj20fq.batch
2026-04-15T00:54:15.130650045-03:00 - batch events/2026-04/15/03/54/3CNTv6d5jEXcMrRJZ6dMaAj20fq.batch upload completed in 100.356108ms
2026-04-15T00:54:15.166251245-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv3lRxBw09VdY3voraJh764J.batch took 55.555625ms (10000 matching messages)
2026-04-15T00:54:15.16630695-03:00 - batch events/2026-04/15/03/54/3CNTv3lRxBw09VdY3voraJh764J.batch stream first-byte delay: 9.064µs
2026-04-15T00:54:15.166310245-03:00 - batch events/2026-04/15/03/54/3CNTv3lRxBw09VdY3voraJh764J.batch stream download took 55.550868ms (100140003 bytes)
2026-04-15T00:54:15.166312183-03:00 - batch events/2026-04/15/03/54/3CNTv3lRxBw09VdY3voraJh764J.batch notify->get delay: 27.315µs
2026-04-15T00:54:15.166360022-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv6d5jEXcMrRJZ6dMaAj20fq.batch
2026-04-15T00:54:15.199665033-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv6d5jEXcMrRJZ6dMaAj20fq.batch took 33.289104ms (10000 matching messages)
2026-04-15T00:54:15.19969569-03:00 - batch events/2026-04/15/03/54/3CNTv6d5jEXcMrRJZ6dMaAj20fq.batch stream first-byte delay: 10.179µs
2026-04-15T00:54:15.199698148-03:00 - batch events/2026-04/15/03/54/3CNTv6d5jEXcMrRJZ6dMaAj20fq.batch stream download took 33.287103ms (100140003 bytes)
2026-04-15T00:54:15.199699979-03:00 - batch events/2026-04/15/03/54/3CNTv6d5jEXcMrRJZ6dMaAj20fq.batch notify->get delay: 6.473µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTvCPcSxVyKDPpmIM6B55jwMV.batch
2026-04-15T00:54:15.204581532-03:00 - batch events/2026-04/15/03/54/3CNTvCPcSxVyKDPpmIM6B55jwMV.batch upload completed in 70.444621ms
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTvCx6uUTSfMNNsdLKWZF52tw.batch
2026-04-15T00:54:15.274776464-03:00 - batch events/2026-04/15/03/54/3CNTvCx6uUTSfMNNsdLKWZF52tw.batch upload completed in 68.295838ms
2026-04-15T00:54:15.299804025-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTvCPcSxVyKDPpmIM6B55jwMV.batch
2026-04-15T00:54:15.299816953-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTvCx6uUTSfMNNsdLKWZF52tw.batch
2026-04-15T00:54:15.333359637-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTvCPcSxVyKDPpmIM6B55jwMV.batch took 33.530724ms (10000 matching messages)
2026-04-15T00:54:15.333403708-03:00 - batch events/2026-04/15/03/54/3CNTvCPcSxVyKDPpmIM6B55jwMV.batch stream first-byte delay: 7.414µs
2026-04-15T00:54:15.333406753-03:00 - batch events/2026-04/15/03/54/3CNTvCPcSxVyKDPpmIM6B55jwMV.batch stream download took 33.526547ms (100140003 bytes)
2026-04-15T00:54:15.333409493-03:00 - batch events/2026-04/15/03/54/3CNTvCPcSxVyKDPpmIM6B55jwMV.batch notify->get delay: 19.505µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv8RJAopVf3URsj9o3Oxr8f8.batch
2026-04-15T00:54:15.351799377-03:00 - batch events/2026-04/15/03/54/3CNTv8RJAopVf3URsj9o3Oxr8f8.batch upload completed in 74.750206ms
2026-04-15T00:54:15.37499369-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTvCx6uUTSfMNNsdLKWZF52tw.batch took 41.580987ms (10000 matching messages)
2026-04-15T00:54:15.375060862-03:00 - batch events/2026-04/15/03/54/3CNTvCx6uUTSfMNNsdLKWZF52tw.batch stream first-byte delay: 33.586236ms
2026-04-15T00:54:15.375065264-03:00 - batch events/2026-04/15/03/54/3CNTvCx6uUTSfMNNsdLKWZF52tw.batch stream download took 41.577808ms (100140003 bytes)
2026-04-15T00:54:15.375068486-03:00 - batch events/2026-04/15/03/54/3CNTvCx6uUTSfMNNsdLKWZF52tw.batch notify->get delay: 10.578µs
2026-04-15T00:54:15.375134597-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv8RJAopVf3URsj9o3Oxr8f8.batch
2026-04-15T00:54:15.412618693-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv8RJAopVf3URsj9o3Oxr8f8.batch took 37.472499ms (10000 matching messages)
2026-04-15T00:54:15.412672822-03:00 - batch events/2026-04/15/03/54/3CNTv8RJAopVf3URsj9o3Oxr8f8.batch stream first-byte delay: 3.872µs
2026-04-15T00:54:15.412677001-03:00 - batch events/2026-04/15/03/54/3CNTv8RJAopVf3URsj9o3Oxr8f8.batch stream download took 37.469531ms (100140003 bytes)
2026-04-15T00:54:15.412678889-03:00 - batch events/2026-04/15/03/54/3CNTv8RJAopVf3URsj9o3Oxr8f8.batch notify->get delay: 8.215µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv9k3X5mz81Mfr52eY5Y06xR.batch
2026-04-15T00:54:15.447179273-03:00 - batch events/2026-04/15/03/54/3CNTv9k3X5mz81Mfr52eY5Y06xR.batch upload completed in 92.636118ms
2026-04-15T00:54:15.51277863-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv9k3X5mz81Mfr52eY5Y06xR.batch
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTvDEKuEW6MQh672fiCIeTpg3.batch
2026-04-15T00:54:15.53123847-03:00 - batch events/2026-04/15/03/54/3CNTvDEKuEW6MQh672fiCIeTpg3.batch upload completed in 81.300977ms
2026-04-15T00:54:15.558581489-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv9k3X5mz81Mfr52eY5Y06xR.batch took 45.779531ms (10000 matching messages)
2026-04-15T00:54:15.558638929-03:00 - batch events/2026-04/15/03/54/3CNTv9k3X5mz81Mfr52eY5Y06xR.batch stream first-byte delay: 6.46µs
2026-04-15T00:54:15.558642856-03:00 - batch events/2026-04/15/03/54/3CNTv9k3X5mz81Mfr52eY5Y06xR.batch stream download took 45.774683ms (100140003 bytes)
2026-04-15T00:54:15.558644754-03:00 - batch events/2026-04/15/03/54/3CNTv9k3X5mz81Mfr52eY5Y06xR.batch notify->get delay: 20.08µs
2026-04-15T00:54:15.558694519-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTvDEKuEW6MQh672fiCIeTpg3.batch
2026-04-15T00:54:15.593936217-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTvDEKuEW6MQh672fiCIeTpg3.batch took 35.231759ms (10000 matching messages)
2026-04-15T00:54:15.593993552-03:00 - batch events/2026-04/15/03/54/3CNTvDEKuEW6MQh672fiCIeTpg3.batch stream first-byte delay: 3.727µs
2026-04-15T00:54:15.59399686-03:00 - batch events/2026-04/15/03/54/3CNTvDEKuEW6MQh672fiCIeTpg3.batch stream download took 35.229509ms (100140003 bytes)
2026-04-15T00:54:15.594000452-03:00 - batch events/2026-04/15/03/54/3CNTvDEKuEW6MQh672fiCIeTpg3.batch notify->get delay: 6.715µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv8HrhGaAslDMeMENbs5QwVR.batch
2026-04-15T00:54:15.605827537-03:00 - batch events/2026-04/15/03/54/3CNTv8HrhGaAslDMeMENbs5QwVR.batch upload completed in 72.414918ms
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv6geblbWHbglYR0GzxEIVgC.batch
2026-04-15T00:54:15.671902858-03:00 - batch events/2026-04/15/03/54/3CNTv6geblbWHbglYR0GzxEIVgC.batch upload completed in 63.662176ms
2026-04-15T00:54:15.694195384-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv6geblbWHbglYR0GzxEIVgC.batch
2026-04-15T00:54:15.694207977-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv8HrhGaAslDMeMENbs5QwVR.batch
2026-04-15T00:54:15.727861384-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv6geblbWHbglYR0GzxEIVgC.batch took 33.63869ms (10000 matching messages)
2026-04-15T00:54:15.727907847-03:00 - batch events/2026-04/15/03/54/3CNTv6geblbWHbglYR0GzxEIVgC.batch stream first-byte delay: 6.679µs
2026-04-15T00:54:15.727912019-03:00 - batch events/2026-04/15/03/54/3CNTv6geblbWHbglYR0GzxEIVgC.batch stream download took 33.635156ms (100140003 bytes)
2026-04-15T00:54:15.727914122-03:00 - batch events/2026-04/15/03/54/3CNTv6geblbWHbglYR0GzxEIVgC.batch notify->get delay: 22.288µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv72DkoiwyAU49wYrRHQw2IN.batch
2026-04-15T00:54:15.749930774-03:00 - batch events/2026-04/15/03/54/3CNTv72DkoiwyAU49wYrRHQw2IN.batch upload completed in 75.952142ms
2026-04-15T00:54:15.762761858-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv8HrhGaAslDMeMENbs5QwVR.batch took 34.845142ms (10000 matching messages)
2026-04-15T00:54:15.762816108-03:00 - batch events/2026-04/15/03/54/3CNTv8HrhGaAslDMeMENbs5QwVR.batch stream first-byte delay: 33.696374ms
2026-04-15T00:54:15.762821925-03:00 - batch events/2026-04/15/03/54/3CNTv8HrhGaAslDMeMENbs5QwVR.batch stream download took 34.841925ms (100140003 bytes)
2026-04-15T00:54:15.762823633-03:00 - batch events/2026-04/15/03/54/3CNTv8HrhGaAslDMeMENbs5QwVR.batch notify->get delay: 13.498µs
2026-04-15T00:54:15.762872993-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv72DkoiwyAU49wYrRHQw2IN.batch
2026-04-15T00:54:15.803461634-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv72DkoiwyAU49wYrRHQw2IN.batch took 40.577801ms (10000 matching messages)
2026-04-15T00:54:15.803523254-03:00 - batch events/2026-04/15/03/54/3CNTv72DkoiwyAU49wYrRHQw2IN.batch stream first-byte delay: 4.278µs
2026-04-15T00:54:15.803527796-03:00 - batch events/2026-04/15/03/54/3CNTv72DkoiwyAU49wYrRHQw2IN.batch stream download took 40.574713ms (100140003 bytes)
2026-04-15T00:54:15.803530663-03:00 - batch events/2026-04/15/03/54/3CNTv72DkoiwyAU49wYrRHQw2IN.batch notify->get delay: 6.662µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTvAD1H3fIPyxnNIrPug8TL0a.batch
2026-04-15T00:54:15.843031006-03:00 - batch events/2026-04/15/03/54/3CNTvAD1H3fIPyxnNIrPug8TL0a.batch upload completed in 91.125118ms
2026-04-15T00:54:15.903638972-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTvAD1H3fIPyxnNIrPug8TL0a.batch
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv6eZ7zcEPWc9kqClgg4v7dx.batch
2026-04-15T00:54:15.91607184-03:00 - batch events/2026-04/15/03/54/3CNTv6eZ7zcEPWc9kqClgg4v7dx.batch upload completed in 69.537743ms
2026-04-15T00:54:15.941124784-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTvAD1H3fIPyxnNIrPug8TL0a.batch took 37.469591ms (10000 matching messages)
2026-04-15T00:54:15.941181765-03:00 - batch events/2026-04/15/03/54/3CNTvAD1H3fIPyxnNIrPug8TL0a.batch stream first-byte delay: 4.976µs
2026-04-15T00:54:15.941185606-03:00 - batch events/2026-04/15/03/54/3CNTvAD1H3fIPyxnNIrPug8TL0a.batch stream download took 37.466252ms (100140003 bytes)
2026-04-15T00:54:15.94118844-03:00 - batch events/2026-04/15/03/54/3CNTvAD1H3fIPyxnNIrPug8TL0a.batch notify->get delay: 13.394µs
2026-04-15T00:54:15.94123761-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv6eZ7zcEPWc9kqClgg4v7dx.batch
2026-04-15T00:54:15.986343302-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv6eZ7zcEPWc9kqClgg4v7dx.batch took 45.095425ms (10000 matching messages)
2026-04-15T00:54:15.986403899-03:00 - batch events/2026-04/15/03/54/3CNTv6eZ7zcEPWc9kqClgg4v7dx.batch stream first-byte delay: 3.583µs
2026-04-15T00:54:15.986409382-03:00 - batch events/2026-04/15/03/54/3CNTv6eZ7zcEPWc9kqClgg4v7dx.batch stream download took 45.093436ms (100140003 bytes)
2026-04-15T00:54:15.986411405-03:00 - batch events/2026-04/15/03/54/3CNTv6eZ7zcEPWc9kqClgg4v7dx.batch notify->get delay: 6.519µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTvCphy5aKRbuEle0wEg51HM8.batch
2026-04-15T00:54:15.996837117-03:00 - batch events/2026-04/15/03/54/3CNTvCphy5aKRbuEle0wEg51HM8.batch upload completed in 77.578239ms
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTv7CLnvpeO9Q8dasjbrzbfUC.batch
2026-04-15T00:54:16.059341926-03:00 - batch events/2026-04/15/03/54/3CNTv7CLnvpeO9Q8dasjbrzbfUC.batch upload completed in 60.324178ms
2026-04-15T00:54:16.086553997-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTv7CLnvpeO9Q8dasjbrzbfUC.batch
2026-04-15T00:54:16.086569136-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTvCphy5aKRbuEle0wEg51HM8.batch
2026-04-15T00:54:16.120597244-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTv7CLnvpeO9Q8dasjbrzbfUC.batch took 34.004123ms (10000 matching messages)
2026-04-15T00:54:16.120653831-03:00 - batch events/2026-04/15/03/54/3CNTv7CLnvpeO9Q8dasjbrzbfUC.batch stream first-byte delay: 14.358µs
2026-04-15T00:54:16.120657743-03:00 - batch events/2026-04/15/03/54/3CNTv7CLnvpeO9Q8dasjbrzbfUC.batch stream download took 33.998976ms (100140003 bytes)
2026-04-15T00:54:16.120660704-03:00 - batch events/2026-04/15/03/54/3CNTv7CLnvpeO9Q8dasjbrzbfUC.batch notify->get delay: 28.214µs
📣 SQS: Notifying new batch at events/2026-04/15/03/54/3CNTvG4JGuLwHXqEYqedLCXagHU.batch
2026-04-15T00:54:16.136121631-03:00 - batch events/2026-04/15/03/54/3CNTvG4JGuLwHXqEYqedLCXagHU.batch upload completed in 74.750566ms
2026-04-15T00:54:16.154181347-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTvCphy5aKRbuEle0wEg51HM8.batch took 33.518117ms (10000 matching messages)
2026-04-15T00:54:16.154221166-03:00 - batch events/2026-04/15/03/54/3CNTvCphy5aKRbuEle0wEg51HM8.batch stream first-byte delay: 34.07488ms
2026-04-15T00:54:16.154224484-03:00 - batch events/2026-04/15/03/54/3CNTvCphy5aKRbuEle0wEg51HM8.batch stream download took 33.514308ms (100140003 bytes)
2026-04-15T00:54:16.154226195-03:00 - batch events/2026-04/15/03/54/3CNTvCphy5aKRbuEle0wEg51HM8.batch notify->get delay: 21.61µs
2026-04-15T00:54:16.154272533-03:00 - recv SQS notification for batch events/2026-04/15/03/54/3CNTvG4JGuLwHXqEYqedLCXagHU.batch
2026-04-15T00:54:16.195527701-03:00 - full batch parsing for events/2026-04/15/03/54/3CNTvG4JGuLwHXqEYqedLCXagHU.batch took 41.245876ms (10000 matching messages)
2026-04-15T00:54:16.195592188-03:00 - batch events/2026-04/15/03/54/3CNTvG4JGuLwHXqEYqedLCXagHU.batch stream first-byte delay: 2.85µs
2026-04-15T00:54:16.195597103-03:00 - batch events/2026-04/15/03/54/3CNTvG4JGuLwHXqEYqedLCXagHU.batch stream download took 41.243424ms (100140003 bytes)
2026-04-15T00:54:16.19560258-03:00 - batch events/2026-04/15/03/54/3CNTvG4JGuLwHXqEYqedLCXagHU.batch notify->get delay: 6.837µs
=== pulsix-bench report ===
backend:              sim
messages target:      200000
payload size:         10000 bytes
elapsed:              1.703986558s
produced:             200000
acked:                200000
consumed unique:      200000
duplicates seen:      0
batches received:     20
throughput:           117371.82 msg/s
throughput payload:   1119.34 MiB/s
throughput wire:      1120.91 MiB/s
latency min:          175.451846ms
latency avg:          287.788826ms
latency p50:          284.141914ms
latency p95:          380.18997ms
latency p99:          419.553737ms
latency max:          432.83766ms
```

## AWS

```bash
```
