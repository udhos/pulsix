# Benchmark

## Simulation

```bash
sudo mount -t tmpfs -o size=2g tmpfs /mnt/ramdisk
```

```bash
#
# 1000 bytes - inject API
#

pulsix-bench -sim-base-dir /mnt/ramdisk/pulsix -disable-message-id -milestones=false -payload-size 1000 -messages 200000 -flush-bytes 100000000 
2026/04/18 01:53:42 sim backend dir kept at: /mnt/ramdisk/pulsix
=== pulsix-bench report ===
backend:              sim
messages target:      200000
payload size:         1000 bytes
elapsed:              1.298249192s
produced:             200000
acked:                200000
consumed unique:      200000
duplicates seen:      0
batches received:     2
throughput:           154053.63 msg/s
throughput payload:   146.92 MiB/s
throughput wire:      148.68 MiB/s
latency min:          519.748413ms
latency avg:          650.370538ms
latency p50:          648.28262ms
latency p95:          1.036849016s
latency p99:          1.04045643s
latency max:          1.04104951s

#
# 1000 bytes - streaminject API
#

pulsix-stream-bench -sim-base-dir /mnt/ramdisk/pulsix -disable-message-id -milestones=false -payload-size 1000 -messages 200000 -flush-bytes 100000000 
2026/04/18 01:53:15 sim backend dir kept at: /mnt/ramdisk/pulsix
=== pulsix-stream-bench report ===
backend:              sim
messages target:      200000
payload size:         1000 bytes
elapsed:              442.328462ms
produced:             200000
acked:                200000
consumed unique:      200000
duplicates seen:      0
batches received:     2
throughput:           452152.68 msg/s
throughput payload:   431.21 MiB/s
throughput wire:      436.38 MiB/s
latency min:          156.431566ms
latency avg:          180.384599ms
latency p50:          180.978308ms
latency p95:          196.999503ms
latency p99:          201.62451ms
latency max:          204.095578ms

#
# 10.000 bytes - inject API
#

pulsix-bench -sim-base-dir /mnt/ramdisk/pulsix -disable-message-id -milestones=false -payload-size 10000 -messages 200000 -flush-bytes 100000000 
2026/04/18 01:49:16 sim backend dir kept at: /mnt/ramdisk/pulsix
=== pulsix-bench report ===
backend:              sim
messages target:      200000
payload size:         10000 bytes
elapsed:              1.720500329s
produced:             200000
acked:                200000
consumed unique:      200000
duplicates seen:      0
batches received:     20
throughput:           116245.26 msg/s
throughput payload:   1108.60 MiB/s
throughput wire:      1110.15 MiB/s
latency min:          161.220498ms
latency avg:          273.10495ms
latency p50:          278.436351ms
latency p95:          352.645029ms
latency p99:          392.050557ms
latency max:          396.525074ms

#
# 10.000 bytes - streaminject API
#
pulsix-stream-bench -sim-base-dir /mnt/ramdisk/pulsix -disable-message-id -milestones=false -payload-size 10000 -messages 200000 -flush-bytes 100000000 
2026/04/18 01:50:45 sim backend dir kept at: /mnt/ramdisk/pulsix
=== pulsix-stream-bench report ===
backend:              sim
messages target:      200000
payload size:         10000 bytes
elapsed:              1.183472701s
produced:             200000
acked:                200000
consumed unique:      200000
duplicates seen:      0
batches received:     20
throughput:           168994.18 msg/s
throughput payload:   1611.65 MiB/s
throughput wire:      1613.91 MiB/s
latency min:          48.303798ms
latency avg:          83.744858ms
latency p50:          64.539224ms
latency p95:          200.905996ms
latency p99:          260.545685ms
latency max:          264.09735ms
```

## AWS

```bash
```
