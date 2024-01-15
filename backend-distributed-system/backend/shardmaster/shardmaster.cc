#include "shardmaster.h"

/**
 * Based on the server specified in JoinRequest, you should update the
 * shardmaster's internal representation that this server has joined. Remember,
 * you get to choose how to represent everything the shardmaster tracks in
 * shardmaster.h! Be sure to rebalance the shards in equal proportions to all
 * the servers. This function should fail if the server already exists in the
 * configuration.
 *
 * @param context - you can ignore this
 * @param request A message containing the address of a key-value server that's
 * joining
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
std::vector<shard_t> calculateNewShards() {
  std::vector<shard_t> newShards;

  // Sort the existing shards in ascending order of their lower bound
  sortAscendingInterval(newShards);

  // Calculate the size of each shard after the new server joins
  size_t shardSize = MAX_KEY / serverInfo.size();

  for (size_t i = 0; i < serverInfo.size(); ++i) {
    // Calculate the lower and upper bounds for each shard
    unsigned int lower = i * shardSize;
    unsigned int upper = (i == serverInfo.size() - 1) ? MAX_KEY : (i + 1) * shardSize - 1;

    // Create the shard and add it to the vector
    shard_t shard = {lower, upper};
    newShards.push_back(shard);
  }

  return newShards;
}

::grpc::Status StaticShardmaster::Join(::grpc::ServerContext* context,
                                       const ::JoinRequest* request,
                                       Empty* response) {
    
     std::string serverAddress = request->server_address();

    for (const ServerInfo& existingServer : servers_) {
        if (existingServer.server_address == serverAddress) {
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server already exists in the configuration");
        }
    }
    int i = 0; 
    std::vector<shard_t> newShards = calculateNewShards();
    for (auto& server : serverInfo) {
        server.allocatedShards = newShards[i];
        i++;
  }
serverInfo.push_back(newServer);
return ::grpc::Status::OK;
}

/**
 * LeaveRequest will specify a list of servers leaving. This will be very
 * similar to join, wherein you should update the shardmaster's internal
 * representation to reflect the fact the server(s) are leaving. Once that's
 * completed, be sure to rebalance the shards in equal proportions to the
 * remaining servers. If any of the specified servers do not exist in the
 * current configuration, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containing a list of server addresses that are
 * leaving
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Leave(::grpc::ServerContext* context,
                                        const ::LeaveRequest* request,
                                        Empty* response) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}

/**
 * Move the specified shard to the target server (passed in MoveRequest) in the
 * shardmaster's internal representation of which server has which shard. Note
 * this does not transfer any actual data in terms of kv-pairs. This function is
 * responsible for just updating the internal representation, meaning whatever
 * you chose as your data structure(s).
 *
 * @param context - you can ignore this
 * @param request A message containing a destination server address and the
 * lower/upper bounds of a shard we're putting on the destination server.
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Move(::grpc::ServerContext* context,
                                       const ::MoveRequest* request,
                                       Empty* response) {
  // Hint: Take a look at get_overlap in common.{h, cc}
  // Using the function will save you lots of time and effort!
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}

/**
 * When this function is called, you should store the current servers and their
 * corresponding shards in QueryResponse. Take a look at
 * 'protos/shardmaster.proto' to see how to set QueryResponse correctly. Note
 * that its a list of ConfigEntry, which is a struct that has a server's address
 * and a list of the shards its currently responsible for.
 *
 * @param context - you can ignore this
 * @param request An empty message, as we don't need to send any data
 * @param response A message that specifies which shards are on which servers
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Query(::grpc::ServerContext* context,
                                        const StaticShardmaster::Empty* request,
                                        ::QueryResponse* response) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}
