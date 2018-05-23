# Simple_Dynamo

Implemented a simplified version of Amazon Dynamo DB.
Implemented a disrtibuted key-value pair storage system that could handle crash-recovery failure.
Used Android Studio to create 5 android emulators using AVDs to simulate a phone, tablet, etc.
Each port has a server constantly-on and listening in the background for messages, by running an async task on each node.
On each AVD theres also a client process running which creates a connection with or opens a socket to the port it needs to
contact and sends messages back and forth.

The three main concepts that were implemented are : 1) Partitioning, 2) Replication, and 3) Crash-recovery Failure handling.
The main goal is to provide both availability and linearizability at the same time. Essentially it should always perform read 
and write operations successfully even under failures. At the same time, a read operation should always return the most recent 
value.

The main commands issued to the simplified Dynamo storage system are insert, query and delete commands.


1) Partitioning : Consistent hashing for node and key distribution. ID space partitioning/re-partitioning based on hash values
                  generated by SHA-1. 5 nodes created whose port numbers are known. The port values are hashed to create a 
                  larger partition space so each node is responsible for a large portion of numbers. During insert command, the
                  inserted key is hashed and compared to the available hashed ports in the chord. Each node responsible for keys
                  with values lesser than its value and greater than its predecessor's hashed value. Once the responsible port is
                  identified, the input key-value pair is sent to it along with the timestamp.
2) Replication : After identifying which port a key belongs to, a Quorum is maintained. The key is also inserted into the two
                 successive nodes as well. In this way data is replicated incase of port failure. Timestamp based object 
                 versioning handled for eventually-consistent data objects.
3) Failure handling : Dealt with crash-recovery failure with the Replica synchronization mechanism after recovery from a failure.
                      Everytime a key is inserted, it is inserted to 3 nodes. And on querying, all 3 nodes are asked for the 
                      associated value and pick the value with the latest timestamped key. Only once we receive acks from all
                      3 nodes is the value decided. One to way identify a node is down is if we recieve a null as an ack or 
                      theres a socket timeout. Down nodes on recovery ask predecessor for all missed keys is handled.
                      