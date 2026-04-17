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
