Read Table Multi-Threaded
=========================

This example shows a program that reads a table using multiple threads. This shows the use of the
`scan_files` and `scan_state` methods on a Scan, that can be used to partition work to either
multiple threads, or workers (in the case of a distributed engine).

We use a single-producer-multi-consumer channel to send each file and its metadata (in the form of a
[`ScanFile`]) that needs to be read out to a pool of threads. Each thread reads from the channel,
and then processes any files it receives. The results are sent back as Arrow `RecordBatch`s on a
mutli-producer-single-consumer channel.

Once the main thread has sent all the files out, we close the `ScanFile` sender, which means that
once the last `ScanFile` has been received by a thread, subsequent `recv` calls in any thread will
start to return errors. The threads take this as a signal to shut down.

We also ensure that _only_ the threads have copies of the `Sender`s used to send the `RecordBatch`s,
by closing the copy that the main thread has once all the threads have been created. This means that
we can simply loop over our `RecordBatch` receiver, because it will return results until the last
thread has exited (which closes that last sender).
