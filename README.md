# Raft Consensus Protocol Implementation 

For compiling protos, 
```
protoc --go_out=. --go-grpc_out=. --proto_path=./protos ./protos/raft.proto
```

**Note:**
1. https://github.com/botketan/Raft-Implementation/commit/78ef42e3fcf2b3225d2bc028f7a032baee0d5cd7 For RPCs (Need to create a function for registering the RPC handlers)
