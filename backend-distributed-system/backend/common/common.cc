#include "common.h"
#include <algorithm>
#include <cassert>
#include <regex>
#include <cstring>

void sortAscendingInterval(std::vector<shard_t>& shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t& a, const shard_t& b) { return a.lower < b.lower; });
}

size_t size(const shard_t& s) { return s.upper - s.lower + 1; }

std::pair<shard_t, shard_t> split_shard(const shard_t& s) {
  // can't get midpoint of size 1 shard
  assert(s.lower < s.upper);
  unsigned int midpoint = s.lower + ((s.upper - s.lower) / 2);
  return std::make_pair<shard_t, shard_t>({s.lower, midpoint},
                                          {midpoint + 1, s.upper});
}

void sortAscendingSize(std::vector<shard_t>& shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t& a, const shard_t& b) { return size(a) < size(b); });
}

void sortDescendingSize(std::vector<shard_t>& shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t& a, const shard_t& b) { return size(b) < size(a); });
}

size_t shardRangeSize(const std::vector<shard_t>& vec) {
  size_t tot = 0;
  for (const shard_t& s : vec) {
    tot += size(vec);
  }
  return tot;
}

OverlapStatus get_overlap(const shard_t& a, const shard_t& b) {
  if (a.upper < b.lower || b.upper < a.lower) {
    /**
     * A: [-----]
     * B:         [-----]
     */
    return OverlapStatus::NO_OVERLAP;
  } else if (b.lower <= a.lower && a.upper <= b.upper) {
    /**
     * A:    [----]
     * B:  [--------]
     * Note: This also includes the case where the two shards are equal!
     */
    return OverlapStatus::COMPLETELY_CONTAINED;
  } else if (a.lower < b.lower && a.upper > b.upper) {
    /**
     * A: [-------]
     * B:   [---]
     */
    return OverlapStatus::COMPLETELY_CONTAINS;
  } else if (a.lower >= b.lower && a.upper > b.upper) {
    /**
     * A:    [-----]
     * B: [----]
     */
    return OverlapStatus::OVERLAP_START;
  } else if (a.lower < b.lower && a.upper <= b.upper) {
    /**
     * A: [-------]
     * B:    [------]
     */
    return OverlapStatus::OVERLAP_END;
  } else {
    throw std::runtime_error("bad case in get_overlap\n");
  }
}

std::vector<std::string> split(const std::string& s) {
  std::vector<std::string> v;
  std::regex ws_re("\\s+");  // whitespace
  std::copy(std::sregex_token_iterator(s.begin(), s.end(), ws_re, -1),
            std::sregex_token_iterator(), std::back_inserter(v));
  return v;
}

std::vector<std::string> parse_value(std::string val, std::string delim) {
  std::vector<std::string> tokens;

  char *save;
  char *tok = strtok_r((char *)val.c_str(), delim.c_str(), &save);
  while(tok != NULL){
    tokens.push_back(std::string(tok));
    tok = strtok_r(NULL, delim.c_str(), &save);
  }

  return tokens;
}

int extractID(std::string key){
  std::vector<std::string> tokens;

  char *save;
  char *tok = strtok_r((char *)key.c_str(), "_", &save);
  while(tok != NULL){
    tokens.push_back(std::string(tok));
    tok = strtok_r(NULL, "_", &save);
  }
  
  assert(tokens.size() > 1); //illformed key

  return stoi(tokens[1]);
}

void resizeShards(const std::vector<std::string>& server_list,
                  std::unordered_map<std::string, std::vector<shard_t>>& server_shards_map, bool add) {
    server_shards_map.clear();

    int num_servers = server_list.size();
    int num_keys = MAX_KEY - MIN_KEY + 1;
    int per_shard = num_keys / num_servers;
    int extra_keys = num_keys % num_servers;
    int lower_bound = MIN_KEY;
    int upper_bound = lower_bound + per_shard - 1;
  if(add){
    for (const auto& server : server_list) {
        if (extra_keys > 0) {
            upper_bound++;
            extra_keys--;
        }
        shard_t new_shard = {lower_bound, upper_bound};
        server_shards_map[server].push_back(new_shard);
        lower_bound = upper_bound + 1;
        upper_bound = lower_bound + per_shard - 1;
    }
  }
    else{
    int upper = per_shard - 1;

    for (auto v : server_list) {
        if (extra_keys) {
            upper++;
            extra_keys--;
        } else if (!per_shard) {
            break;
        }

        shard_t new_shard;
        new_shard.lower = lower_bound;
        new_shard.upper = upper;
        server_shards_map[v].clear();
        server_shards_map[v].push_back(new_shard);
        lower_bound = upper + 1;
        upper = lower_bound + per_shard - 1;
    }
    }
    
}
