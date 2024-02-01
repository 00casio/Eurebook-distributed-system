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
    this->server_list.push_back(request->server());
    for (auto& server : this->server_list) {
        this->server_shards_map[server].clear();
    }
    bool add = true;
    resizeShards(server_list, server_shards_map,add);
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

    std::unique_lock<std::mutex> lock(this-> mutex);
    for (int i = 0; i < request->servers_size(); i++) {
        if (this->server_shards_map.find(request->servers(i)) == this->server_shards_map.end()) {
            lock.unlock();
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "The given server does not exist!");
        }
        this->server_shards_map.erase(request->servers(i));
        this->server_list.erase(std::find(this->server_list.begin(), this->server_list.end(), request->servers(i)));
    }
    bool add = false;
    resizeShards(server_list, server_shards_map,add);

    lock.unlock();
    return ::grpc::Status::OK;
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
    std::unique_lock<std::mutex> lock(this->mutex);
    if (this->server_shards_map.find(request->server()) == this->server_shards_map.end()) {
        lock.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server doesn't exist. Move Error!");
    }
    shard_t moved_shard;
    shard_t new_shard;
    shard_t existing_shard;
    moved_shard.lower = request->shard().lower();
    moved_shard.upper = request->shard().upper();
    for (auto& server_shards : this->server_shards_map) {
        std::vector<shard_t> updated_shards;
        for (auto& existing_shard : server_shards.second) {
            auto overlap_status = get_overlap(existing_shard, moved_shard);
            switch (overlap_status) {
                case OverlapStatus::OVERLAP_START:
                    existing_shard.lower = moved_shard.upper + 1;
                    updated_shards.push_back(existing_shard);
                    break;
                case OverlapStatus::OVERLAP_END:
                    existing_shard.upper = moved_shard.lower - 1;
                    updated_shards.push_back(existing_shard);
                    break;
                case OverlapStatus::COMPLETELY_CONTAINS:
                    new_shard.lower = moved_shard.upper + 1;
                    new_shard.upper = existing_shard.upper;
                    existing_shard.upper = moved_shard.lower - 1;
                    updated_shards.push_back(existing_shard);
                    updated_shards.push_back(new_shard);
                    break;
                case OverlapStatus::NO_OVERLAP:
                    updated_shards.push_back(existing_shard);
                    break;
            }
        }
        this->server_shards_map[server_shards.first] = updated_shards;
    }
    this->server_shards_map[request->server()].push_back(moved_shard);
    sortAscendingInterval(this->server_shards_map[request->server()]);

    lock.unlock();
    return ::grpc::Status::OK;
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
    std::unique_lock<std::mutex> lock(this->mutex);
    for (const auto& currentServer : this->server_list) {
        auto currentConfigEntry = response->add_config();
        currentConfigEntry->set_server(currentServer);
        for (const auto& currentShard : this->server_shards_map[currentServer]) {
            auto currentShardEntry = currentConfigEntry->add_shards();
            currentShardEntry->set_lower(currentShard.lower);
            currentShardEntry->set_upper(currentShard.upper);
        }
    }
    lock.unlock();
    return ::grpc::Status::OK;
}


