# Raft Consensus Protocol Implementation 
CS 542 : Topics in Distributed systems Course Term Project
## Members
- Dhanesh V - 210101117
- Ketan Singh - 210101118
- Shivam Agrawal - 210101119
## Things implemented 
- Leader Election after Election Timeout
- Log Replication and Committing
- Log Application to StateMachines
- Basic KV Client to submit operations
- State persistence using MongoDB as stable storage
- RPCs are implemented using `gRPC` framework
- Prevote phase to aid in disruptive servers in case of partition.
- Add and Remove Server RPCs
#### Note
For compiling protos, 
```
protoc --go_out=. --go-grpc_out=. --proto_path=./protos ./protos/raft.proto
```
