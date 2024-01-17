#include "shardmaster.h"
#include <vector>

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
::grpc::Status StaticShardmaster::Join(::grpc::ServerContext* context,
                                       const ::JoinRequest* request,
                                       Empty* response) {
    
    // Lock the mutex to ensure thread safety
    std::unique_lock<std::mutex> lock(this-> mutex);
    // Check if the server already exists in the configuration
    if (this->server_shards_map.find(request->server()) != this->server_shards_map.end()) {
        lock.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server already exists in the configuration");
    }
    int num_servers = this->server_shards_map.size() + 1;
    this->server_list.push_back(request->server());
    for (auto& server : this->server_list) {
        this->server_shards_map[server].clear();
    }
    int num_keys = MAX_KEY - MIN_KEY + 1;
    int per_shard = num_keys / num_servers;
    int extra_keys = num_keys % num_servers;
    int lower_bound = MIN_KEY;
    int upper_bound = lower_bound + per_shard - 1;
    for (auto& server : this->server_list) {
        extra_keys > 0 ? (upper_bound++, extra_keys--) : (void)0;
        shard_t new_shard = {lower_bound, upper_bound};
        this->server_shards_map[server].push_back(new_shard);
        lower_bound = upper_bound + 1;
        upper_bound = lower_bound + per_shard - 1;
    }

    lock.unlock();
    
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

