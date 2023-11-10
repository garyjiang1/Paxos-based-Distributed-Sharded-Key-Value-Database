# Paxos-based Sharded Key/Value Database

*This project is an excerpt of an individual course assignment implementation of Fundamentals of Large-scale Distributed Systems at Columbia University, Fall 2023.* [Course website](https://systems.cs.columbia.edu/ds1-class/ "Course website")

## Introduction

The project implements a distributed key/value storage system supporting concurrent *Put*, *Get*, and *PutHash* requests from multiple clients with sequential consistency. Scalability and fault tolerance are achieved by sharding keys over a set of replica groups and incorporating the Paxos protocol in each replica group.

A shard is a subset of all key/value pairs in the database, facilitating scalability. Each replica group handles client requests for specific shards, operating in parallel to increase total system throughput.

### Main Components

1. **Replica Groups**: Each responsible for a subset of all the shards, consisting of several servers (kvserver) that use Paxos to replicate the group's shard.

2. **Shard Master**: Decides which replica group serves each shard. This configuration changes over time and is implemented as a fault-tolerant service using Paxos.

### Shardmaster

The shard master is in charge of configuration, determining which server handles client requests for a specific shard. It manages a sequence of numbered configurations, each describing a set of replica groups and their shard assignments.

### Paxos

Paxos is a consensus protocol supporting an indefinite sequence of agreement instances. It ensures that all kvservers within a replica group agree on the order of operations.

### KVServer

Kvserver is the fundamental unit for data storage in a replica group. Each kvserver is a replica of others within the group, ensuring data availability and durability.

### Client

Clients can send *Put*, *Get*, and *PutHash* requests via RPC to replica groups. The system provides sequential consistency and at-most-once semantics to applications using its client interface.

## Usage

To test the script, navigate to the corresponding folder and set GOPATH before running:

```bash
go test
