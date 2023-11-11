# Paxos-based Distributed Sharded Key/Value Database

*This project is an excerpt of an individual course assignment implementation of Fundamentals of Large-scale Distributed Systems at Columbia University, Fall 2023.* [Course website](https://systems.cs.columbia.edu/ds1-class/ "Course website")

## Table of Contents
- [Introduction](#introduction)
- [Main Components](#main-components)
- [The Journey of Data: A Story of the System](#the-journey-of-data-a-story-of-the-system)
- [Usage](#usage)
- 
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

## The Journey of Data: A Story of the System

In the vast digital landscape where data is the lifeblood, we find our Paxos-based Sharded Key/Value Database, a system designed to manage this data with efficiency and reliability.

### Chapter 1: The Realm of Shards

In the kingdom of Distributed Data, shards are like individual territories, each holding a portion of the data treasure. Managed independently, these shards ensure rapid processing of data requests.

### Chapter 2: The Paxos Protocol - The Law of the Land

The Paxos protocol acts like a democratic system within each shard, where each server has a say in the decision-making process. It ensures consensus even in adverse conditions, maintaining peace and order.

### Chapter 3: The Shardmaster - The Overseer

The Shardmaster, overseeing the realm, ensures balance among the shards. It redistributes responsibilities among replica groups to maintain efficiency and handle capacity changes.

### Chapter 4: The KVServer - The Keeper of Data

KVServers are the storers of data within each shard, acting as fortresses that keep the data safe and available. They stay in sync to maintain uniformity and data integrity.

### Chapter 5: The Client - The Messenger

Clients, acting as messengers, traverse the land to interact with shards, carrying requests like 'Put', 'Get', and 'PutHash'. They consult the Shardmaster to determine their destinations and deliver their messages.

### Epilogue: The Dance of Data

In this digital realm, our Paxos-based Sharded Key/Value Database ensures that data flows gracefully and efficiently. Each component plays a vital role in keeping the system scalable, fault-tolerant, and consistent - a marvel of modern distributed systems.

## Usage

To test the script, navigate to the corresponding folder and set GOPATH before running:

```bash
go test
