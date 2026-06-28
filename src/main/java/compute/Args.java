package compute;

import filter.SFTableMode;

public class Args {

    public static int maxMassageLen = 200 * 1024 * 1024;
    public static int recursionLimit = 64;

    // true -> sase engine, false -> flink sql engine
    public static boolean isSaseEngine = true;

    // data chunk size: 8 MB
    public static int MAX_CHUNK_SIZE = 8 * 1024 * 1024; //8 * 1024 * 1024;

    public static boolean isOptimizedSwf = true;
    public static SFTableMode sfTableMode = SFTableMode._12_10_10;

    // Number of leading pages the compute node asks each storage node to send as cache.
    // Storage nodes learn the effective skip range from the cache-fetch RPC, not from startup args.
    public static int computeCachePageNum = 0;
}
