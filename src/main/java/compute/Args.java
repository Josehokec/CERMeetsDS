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
}
