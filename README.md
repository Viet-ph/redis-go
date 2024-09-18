# Redis-Go

**Redis-Go** is a custom implementation of the Redis key-value store built in Go. This project provides a lightweight, simplified version of Redis to help understand how in-memory data stores work and demonstrate the implementation of an event-driven architecture to handle multiple clients.

## What is this Project About?

Redis-Go mimics core functionalities of the original Redis, such as storing and retrieving key-value pairs, but is implemented from scratch using Go. Unlike Redis, which uses a multi-threaded event loop model, this project implements a single-threaded event loop for handling multiple clients efficiently without using goroutines.

### Key Features:
- **Basic Redis Commands**: Supports a wide range of Redis-like commands, including string and hash operations.
- **Event-Driven Architecture**: Handles multiple client connections through a single-threaded event loop using low-level system calls (`epoll` on Linux, `kqueue` on macOS).
- **In-Memory Storage**: All data is stored in memory for fast access.

## Installation

To get started, you need to have Go installed on your system. You can download Go from [here](https://golang.org/dl/).

1. **Clone the Repository**:
    ```bash
   git clone https://github.com/Viet-ph/redis-go.git
   cd redis-go
   

2. **Build the Project:**:
    ```bash
   go build