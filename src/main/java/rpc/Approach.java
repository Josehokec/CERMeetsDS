package rpc;

public enum Approach {
    OURS,                   // our multi-round filtering approach using dual filtering strategy
    EXTEND_OURS,            // predicate-cache-extended multi-round filtering approach
    PUSH_DOWN,              // push-down approach
    PUSH_PULL,              // predicate push-pull
    NAIVE_TWO_TRIPS,        // two-trips approach using interval array and hash table
    NAIVE_MULTI_TRIPS,      // multi-trips approach using interval array and hash table
    NAIVE_SWF_HASH_TABLE,   // SWF-based multi-round filtering using hash table
    PULL_ALL                // pull all events to the compute server
}
