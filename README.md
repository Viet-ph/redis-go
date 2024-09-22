# Redis-Go

**Redis-Go** is a custom implementation of the Redis key-value store built in Go. This project provides a lightweight, simplified version of Redis to help understand how in-memory data stores work and demonstrate the implementation of an event-driven architecture to handle multiple clients.

## What is this Project About?

Redis-Go mimics core functionalities of the original Redis, such as storing, retrieving key-value pairs, io-multiplexing, replication and more, but is implemented from scratch using Go. 

### Key Features:
- **Basic Redis Commands**: Supports a wide range of Redis-like commands, including string and hash operations.
- **Event-Driven Architecture**: Handles multiple client connections through a single-threaded event loop using low-level system calls (`epoll` on Linux, `kqueue` on macOS).
- **In-Memory Storage**: All data is stored in memory for fast access.

## Installation

To get started, you need to have Go installed on your system. You can download Go from [here](https://golang.org/dl/).

1. **Clone the Repository**:
```sh
$ git clone https://github.com/Viet-ph/redis-go.git
$ cd redis-go
```
2. **Build the Project**:
This will compile the source code and create an executable binary named redis-go in your project directory.
```sh
$ make build
```
## Quick Start

1. **Running the Redis-Go Server**:
Once the project is built, you can start the server with default settings by running the following command:
```sh
$ make run
```
```sh
$ # Or manually execute go binary with custom port 
$ ./bin/redis-go --port <YOUR_PORT>
```
**By default, the server listens on localhost:6379 (the standard Redis port). You can connect to it using the official Redis CLI or any Redis client**: 
```sh
$ redis-cli
```
1. **See available Commands:**
```sh
$ #List all commands
$ redis-cli command list

$ #Read command document
$ redis-cli command docs <COMMAND>
```

2. **Start Replication**:
After the master instance is fully initialized. You can run a replication with master host:
```sh
$ ./bin/redis-go --port <YOUR_PORT> --replicaof "<MASTER_IP> <MASTER_PORT>" 
```

## TODO:
- [ ] RDB encoding for hash datatype
- [ ] Implement Redis List datatype
- [ ] Implement Redis Stream datatype
- [ ] Implement Redis Transaction


   