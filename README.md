# Protocol Buffers Example

This extends the Project 1 template code with a simple server and client that send messages via Protocol Buffers.

To work with Protocol Buffers on your own machine, you'll need to install the ```protoc``` compiler. Install via your package manager:

* **protobuf** on homebrew 
* **protobuf-compiler** on RedHat-based distributions

...or download via the [Protocol Buffers Homepage](https://developers.google.com/protocol-buffers/)

Assumptions:
1. The whole system will has at least 1 datanode working;
2. The system does not support multiple storing the same file;
3. The system has at least one chunk replica successfully stored for each chunk;

