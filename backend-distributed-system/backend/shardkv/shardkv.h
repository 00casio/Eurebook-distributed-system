#ifndef SHARDING_SHARDKV_H
#define SHARDING_SHARDKV_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "../common/common.h"
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <fstream>

#include "../build/shardkv.grpc.pb.h"
#include "../build/shardmaster.grpc.pb.h"

class ShardkvServer : public Shardkv::Service {
  using Empty = google::protobuf::Empty;

 public:
  explicit ShardkvServer(std::string addr, const std::string& shardmanager_addr)
      : address(std::move(addr)), shardmanager_address(shardmanager_addr) {

    // This thread will query the shardmaster every 100 milliseconds for updates
    std::thread query(
            [this]() {
                // TODO: Assignment 2 Implement the QueryShardmaster(...) function
                std::chrono::milliseconds timespan(100);
                while (shardmaster_address.empty()) {
                    std::this_thread::sleep_for(timespan);
                }
                auto stub = Shardmaster::NewStub(
                        grpc::CreateChannel(shardmaster_address, grpc::InsecureChannelCredentials()));
                while (true) {
                    this->QueryShardmaster(stub.get());
                    std::this_thread::sleep_for(timespan);
                }
            }
            );
    // we detach the thread so we don't have to wait for it to terminate later
    query.detach();


    std::thread heartbeat(
        [this](const std::string sm_addr) {
            // TODO: Assignment 2 - Implement PingShardmanager(...) function
            // TODO: Assignment 3 - Extends PingShardmanager(...) function as described in the instructions
            std::chrono::milliseconds timespan(100);
            auto stub = Shardkv::NewStub(
                    grpc::CreateChannel(sm_addr, grpc::InsecureChannelCredentials()));
            while (true) {
                PingShardmanager(stub.get());
                std::this_thread::sleep_for(timespan);
            }
        },
        shardmanager_addr);
    // we detach the thread so we don't have to wait for it to terminate later
    heartbeat.detach();
  };

  
  // TODO implement these three methods, should be fairly similar to your simple_shardkv
  ::grpc::Status Get(::grpc::ServerContext* context,
                     const ::GetRequest* request,
                     ::GetResponse* response) override;
  ::grpc::Status Put(::grpc::ServerContext* context,
                     const ::PutRequest* request, Empty* response) override;
  ::grpc::Status Append(::grpc::ServerContext* context,
                        const ::AppendRequest* request,
                        Empty* response) override;
  ::grpc::Status Delete(::grpc::ServerContext* context,
                        const ::DeleteRequest* request,
                        Empty* response) override;
    ::grpc::Status Dump(::grpc::ServerContext* context,
                        const ::google::protobuf::Empty* request,
                        ::DumpResponse* response);

  // TODO this will be called in a separate thread, here is where you want to
  // query the shardmaster for configuration updates and respond to changes
  // appropriately (i.e. transferring keys, no longer serving keys, etc.)
  void QueryShardmaster(Shardmaster::Stub* stub);
  bool keyassignstatus(string key,vector<shard_t>& shards_assigned);

  // TODO this will be called in a separate thread, here is where you want to
  // ping the shardmanager to get updates about the sharmaster (part 2) and the views changes (part 3)
  void PingShardmanager(Shardkv::Stub* stub);
  
 private:
  // address we're running on (hostname:port)
  const std::string address;
  // address of shardmanager passed as constructor's parameter
  std::string shardmanager_address;
  // address of shardmaster sent by the shardmanager
  std::string shardmaster_address;
  std::unique_ptr<Shardmaster::Stub> stub;
  map<string, string> users;//key, value
  map<string, post_t> posts;
  std::unordered_map<std::string, std::vector<shard_t>> other_managers_shard;
  vector<shard_t> shards_assigned;
};

#endif  // SHARDING_SHARDKV_H
