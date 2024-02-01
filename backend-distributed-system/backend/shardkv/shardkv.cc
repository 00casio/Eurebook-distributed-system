#include <grpcpp/grpcpp.h>
#include "shardkv.h"
#include "../build/shardkv.grpc.pb.h"
#include <grpcpp/grpcpp.h>
using grpc::Channel;
using grpc::Status;
using grpc::ClientContext;

enum class RequestType {
    ALL_USERS,
    POST,
    USER,
    OTHER
};
mutex mutex_shards_assigned;


bool keyassignstatus(string key,vector<shard_t>& shards_assigned){
    string key_str = key.substr(5);
    if (key.find("posts") != std::string::npos) {
        key_str = key_str.substr(0, key_str.length()-6);
    }
    unsigned int key_int = stoul(key_str);
    mutex_shards_assigned.lock();
    for(shard s: shards_assigned){ if(s.lower <= key_int && s.upper >= key_int){
            mutex_shards_assigned.unlock();
            return true;
        }
    }
    mutex_shards_assigned.unlock();
    return false;
}


RequestType GetRequestType(const std::string& key) {
    if (key.find("all_users") != std::string::npos) {
        return RequestType::ALL_USERS;
    } else if (key.find("post") == 0) {
        return RequestType::POST;
    } else if (key.find("posts") != std::string::npos) {
        return RequestType::USER;
    } else {
        return RequestType::OTHER;
    }
}

/**
 * This method is analogous to a hashmap lookup. A key is supplied in the
 * request and if its value can be found, we should either set the appropriate
 * field in the response Otherwise, we should return an error. An error should
 * also be returned if the server is not responsible for the specified key
 *
 * @param context - you can ignore this
 * @param request a message containing a key
 * @param response we store the value for the specified key here
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
    string key = request->key();
    bool NO_REQ = false;
    if (key.find("_no_req") != std::string::npos) {
        key = key.substr(0, key.length()-7);
        NO_REQ = true;
    }

    cout << "in shardkv, get, key: " << request->key() << endl;
    bool is_all_users = key.compare("all_users") == 0;
    if(!NO_REQ && (!is_all_users && !::keyassignstatus(key, shards_assigned))) {
        cerr << "shrdkv not responsible of this key" << endl;
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "key is not assigned to this shardkv");
    }

    try {
        switch (GetRequestType(key)) {
            case RequestType::ALL_USERS: {
                // List all users
                cout << "listing all users" << endl;
                string res = "";
                for (const auto& it : users) {
                    res += it.first + ",";
                }
                response->set_data(res);
                return ::grpc::Status(::grpc::Status::OK);
            }
            case RequestType::POST: {
                // Get a post
                if (posts.count(key) == 0) {
                    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "post does not exist");
                }
                response->set_data(posts[key].content);
                return ::grpc::Status(::grpc::Status::OK);
            }
            case RequestType::USER: {
                // Get user posts
                string user_key = key.substr(0, key.size()-6);
                string res = "";
                for (const auto& it : posts) {
                    if (it.second.user_id == user_key) {
                        res += it.first + ",";
                    }
                }

                if (!NO_REQ) {
                    for (const auto& entry : other_managers_shard) {
                        const std::string& serv_name = entry.first;

                        cout << "asking to other managers" << endl;
                        ClientContext cc;
                        GetRequest req;
                        std::string new_key = key + "_no_req";
                        req.set_key(new_key);
                        GetResponse get_resp;

                        auto channel = grpc::CreateChannel(serv_name, grpc::InsecureChannelCredentials());
                        auto kvStub = Shardkv::NewStub(channel);
                        auto status = kvStub->Get(&cc, req, &get_resp);

                        if (status.ok()) {
                            cout << serv_name << " answered with " << get_resp.data() << endl;
                            res += get_resp.data();
                        } else {
                            cout << serv_name << " DID NOT ANSWER " << status.error_message() << endl;
                        }
                    }
                }

                if (res.empty()) {
                    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "user does not have posts");
                } else {
                    response->set_data(res);
                    return ::grpc::Status(::grpc::Status::OK);
                }
            }
            case RequestType::OTHER: {
                // Get user
                if (users.count(key) == 0) {
                    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "user does not exist");
                } else {
                    response->set_data(users[key]);
                    return ::grpc::Status(::grpc::Status::OK);
                }
            }
            default: {
                return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "invalid request type");
            }
        }
    } catch (const std::exception& e) {
        response->set_data(e.what());
        return ::grpc::Status(::grpc::Status::OK);
    }

    return ::grpc::Status(::grpc::Status::OK);
}

    


/**
 * Insert the given key-value mapping into our store such that future gets will
 * retrieve it
 * If the item already exists, you must replace its previous value.
 * This function should error if the server is not responsible for the specified
 * key.
 *
 * @param context - you can ignore this
 * @param request A message containing a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
    // Log the key, data, and user of the put request
    cout << "Processing put request in shardkv. Key: " << request->key() << ", Data: " << request->data() << ", User: " << request->user() << endl;

    // Extract key from the request
    string key = request->key();

    // Check if the key is assigned to this shardkv server
    if (!::keyassignstatus(key, shards_assigned))
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "the key is not assigned to this shardkv");

    // Process the put request based on the key type
    if (key.rfind("post", 0) == 0) {
        // Put request for a post
        post_t post = post_t();
        post.content = request->data();
        post.user_id = request->user();
        posts[key] = post; // Store the post in the posts map
    } else {
        users[request->key()] = request->data(); 
    }
    cout << "Updated users map:" << endl;
    for_each(users.begin(),
             users.end(),
             [](const std::pair<string, string> &p) {
                 std::cout << "{" << p.first << ": " << p.second << "}\n";
             });

    return ::grpc::Status(::grpc::Status::OK);
}

/**
 * Appends the data in the request to whatever data the specified key maps to.
 * If the key is not mapped to anything, this method should be equivalent to a
 * put for the specified key and value. If the server is not responsible for the
 * specified key, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containngi a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>"
 */
::grpc::Status ShardkvServer::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
    cout << "in the shardkv append, key: " << request->key() << ", data: " << request->data() << endl;
    return ::grpc::Status(::grpc::Status::OK);
    }

/**
 * Deletes the key-value pair associated with this key from the server.
 * If this server does not contain the requested key, do nothing and return
 * the error specified
 *
 * @param context - you can ignore this
 * @param request A message containing the key to be removed
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Delete(::grpc::ServerContext* context,
                                     const ::DeleteRequest* request,
                                     Empty* response) {
    cout << "Processing delete request in shardkv. Key: " << request->key() << endl;

    // Extract key from the request
    string key = request->key();
    // Check if the key is for a post or a user, and delete accordingly
    if (key.rfind("post", 0) == 0) {
        // Delete request for a post
        if (posts.count(key) == 0)
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Post does not exist");
        posts.erase(key); // Remove the post from the posts map
    } else {
        if (users.count(key) == 0)
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "User does not exist");
        users.erase(key); // Remove the user from the users map
    }

    // Log the updated users map
    cout << "Updated users map:" << endl;
    for_each(users.begin(),
             users.end(),
             [](const std::pair<string, string> &p) {
                 std::cout << "{" << p.first << ": " << p.second << "}\n";
             });

    return ::grpc::Status(::grpc::Status::OK);
}
/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done). It should query the shardmaster
 * for an updated configuration of how shards are distributed. You should then
 * find this server in that configuration and look at the shards associated with
 * it. These are the shards that the shardmaster deems this server responsible
 * for. Check that every key you have stored on this server is one that the
 * server is actually responsible for according to the shardmaster. If this
 * server is no longer responsible for a key, you should find the server that
 * is, and call the Put RPC in order to transfer the key/value pair to that
 * server. You should not let the Put RPC fail. That is, the RPC should be
 * continually retried until success. After the put RPC succeeds, delete the
 * key/value pair from this server's storage. Think about concurrency issues like
 * potential deadlock as you write this function!
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 */
void ShardkvServer::QueryShardmaster(Shardmaster::Stub* stub) {
    ClientContext cc;
    Empty req;
    QueryResponse res;
    auto status = stub->Query(&cc, req, &res);

    switch (status.error_code()) {
        case grpc::StatusCode::OK: {
            other_managers_shard.clear();
            for (const ConfigEntry& config : res.config()) {
                const std::string& currentServer = config.server();

                if (currentServer != shardmanager_address) {
                    // Server entry for other managers
                    std::vector<shard_t> serverShards;

                    for (const Shard& s : config.shards()) {
                        shard_t new_shard;
                        new_shard.lower = s.lower();
                        new_shard.upper = s.upper();
                        serverShards.push_back(new_shard);
                    }

                    other_managers_shard[currentServer] = serverShards;
                } else {
                    // Shards assigned to the current manager
                    mutex_shards_assigned.lock();
                    shards_assigned.clear();

                    for (const Shard& s : config.shards()) {
                        shard_t new_shard;
                        new_shard.lower = s.lower();
                        new_shard.upper = s.upper();
                        shards_assigned.push_back(new_shard);
                    }

                    mutex_shards_assigned.unlock();
                }
            }

            // Transfer keys that are not assigned to this server anymore
            vector<string> keys_to_remove;
            for (const auto& entry : users) {
                const string& key = entry.first;
                if (!::keyassignstatus(key, shards_assigned)) {
                    keys_to_remove.push_back(key);
                }
            }

            for (const string& key : keys_to_remove) {
                users.erase(key);
                cout << "Removed key: " << key << endl;
            }
            break;
        }
        default: {
            logError("Query", status);
            break;
        }
    }
}

/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done).
 * BASIC LOGIC - PART 2
 * It pings the shardmanager to signal the it is alive and available to receive Get, Put, Append and Delete RPCs.
 * The first time it pings the sharmanager, it will  receive the name of the shardmaster to contact (by means of a QuerySharmaster).
 *
 * PART 3
 *
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 * */
void ShardkvServer::PingShardmanager(Shardkv::Stub* stub) {
    PingRequest pingReq;
    pingReq.set_server(address);

    PingResponse pingResponse;
    ClientContext cc;

    Status status = stub->Ping(&cc, pingReq, &pingResponse);

    switch (status.error_code()) {
        case grpc::StatusCode::OK: {
            shardmaster_address = pingResponse.shardmaster();
            break;
        }
        default: {
            logError("Ping request", status);
            break;
        }
    }
}



/**
 * PART 3 ONLY
 *
 * This method is called by a backup server when it joins the system for the firt time or after it crashed and restarted.
 * It allows the server to receive a snapshot of all key-value pairs stored by the primary server.
 *
 * @param context - you can ignore this
 * @param request An empty message
 * @param response the whole database
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Dump(::grpc::ServerContext* context, const Empty* request, ::DumpResponse* response) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}
