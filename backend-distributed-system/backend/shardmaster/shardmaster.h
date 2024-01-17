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
  std::mutex mutex; 
  std::unordered_map<std::string, std::vector<shard_t>> server_shards_map;
  std::vector<std::string> server_list; 
};

#endif  // SHARDING_SHARDMASTER_H
