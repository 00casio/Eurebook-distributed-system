#ifndef SHARDING_SHARDMASTER_H
#define SHARDING_SHARDMASTER_H

#include "../common/common.h"

#include <grpcpp/grpcpp.h>
#include <unordered_map>
#include <string>
#include <vector>
#include <mutex>
#include "../build/shardmaster.grpc.pb.h"

class StaticShardmaster : public Shardmaster::Service {
  using Empty = google::protobuf::Empty;

 public:
  // TODO implement these four methods!
  ::grpc::Status Join(::grpc::ServerContext* context,
                      const ::JoinRequest* request, Empty* response) override;
  ::grpc::Status Leave(::grpc::ServerContext* context,
                       const ::LeaveRequest* request, Empty* response) override;
  ::grpc::Status Move(::grpc::ServerContext* context,
                      const ::MoveRequest* request, Empty* response) override;
  ::grpc::Status Query(::grpc::ServerContext* context, const Empty* request,
                       ::QueryResponse* response) override;

 private:
  struct ServerInfo {
    std::string address;
    std::vector<shard_t> allocatedShards;
  };

  std::vector<ServerInfo> serverInfo;
  std::mutex serverInfoMutex;
};

#endif  // SHARDING_SHARDMASTER_H
