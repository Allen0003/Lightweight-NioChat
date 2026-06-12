# Lightweight-NioChat

A high-performance, concurrent, multi-player chat room application built entirely from scratch using Java's native **Non-blocking I/O (NIO)** API. This project serves as a deep-dive exploration into the mechanics of low-level network programming, asynchronous event loops, and memory buffer management without the abstraction of high-level frameworks like Netty or Apache Mina.

---

## 🚀 Features

* **Single-Threaded Event Loop:** Leverages `java.nio.channels.Selector` to monitor thousands of concurrent connections efficiently.
* **Asynchronous Non-Blocking I/O:** Uses `ServerSocketChannel` and `SocketChannel` in non-blocking mode to prevent thread-starvation.
* **Zero-Dependency:** Written strictly in standard JDK native APIs.
* **Real-time Broadcasting:** Instant message forwarding to all active, connected peers.
* **Bidirectional Client:** Decoupled architecture allowing simultaneous user input handling and asynchronous server message consumption.

---

## 🏛️ Architecture & Mechanics

The core engine relies on the **Java NIO "Holy Trinity"**:
1.  **Channels:** Unbuffered data conduits (e.g., `SocketChannel`) supporting asynchronous read/write operations.
2.  **Buffers:** Memory blocks (`ByteBuffer`) utilized for granular control over raw byte payloads using pointer-based mechanics (`flip()`, `clear()`).
3.  **Selector:** A multiplexer implementing the reactor pattern, capturing operational states (`OP_ACCEPT`, `OP_READ`) in a single execution thread loop.

---

## 🛠️ Getting Started

### Prerequisites
* Java Development Kit (JDK) 8 or higher.

### Installation & Execution

#### 1. Clone the Repository
```bash
git clone [https://github.com/your-username/Lightweight-NioChat.git](https://github.com/your-username/Lightweight-NioChat.git)
cd Lightweight-NioChat
