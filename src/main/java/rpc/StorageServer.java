package rpc;

import compute.Args;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import rpc.iface.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class StorageServer {
    public static void main(String[] args) throws FileNotFoundException {
        /*
        OURS,                   // our multi-round filtering approach using dual filtering strategy
        EXTEND_OURS,            // predicate-cache-extended multi-round filtering approach
        PUSH_DOWN,              // push-down approach
        PUSH_PULL,              // predicate push-pull
        NAIVE_TWO_TRIPS,        // two-trips approach using interval array and hash table
        NAIVE_MULTI_TRIPS,      // multi-trips approach using interval array and hash table
        NAIVE_SWF_HASH_TABLE,   // SWF-based multi-round filtering using hash table
        PULL_ALL                // pull all events to the compute server
         */
        int port = intArg(args, new String[]{"--port", "port"}, 9090);
        String nodeId = stringArg(args, new String[]{"--node-id", "nodeId", "--nodeId", "node-id"}, "_0");
        String filename = stringArg(args, new String[]{"--filename", "filename", "--output", "output"}, "Storage"+nodeId+".txt");
        String approachName = stringArg(args, new String[]{"--approach", "approach"}, "OURS");
        System.setProperty("nodeId", nodeId);

        redirectOutput(filename);

        Approach approach = parseApproach(approachName);
        System.out.println("approach: " + approach);
        System.out.println("port: " + port + ", nodeId: " + nodeId + ", filename: " + filename);
        System.out.println("compute cache pages are configured by the compute node");
        startService(approach, port);
    }

    public static void startService(Approach approach, int port){
        TNonblockingServerSocket serverTransport = null;
        try{
            int clientTimeOut = 180_000; // 3min
            serverTransport = new TNonblockingServerSocket(port, clientTimeOut, Args.maxMassageLen);
            TThreadedSelectorServer.Args targs = new TThreadedSelectorServer.Args(serverTransport);

            switch(approach){
                case OURS:
                    FilterBasedRPC.Processor ours = new FilterBasedRPC.Processor(new FilterBasedRPCImpl());
                    targs.processor(ours);
                    break;
                case EXTEND_OURS:
                    ExtendFilterBasedRPC.Processor extendOurs = new ExtendFilterBasedRPC.Processor(new ExtendFilterBasedRPCImpl());
                    targs.processor(extendOurs);
                    break;
                case NAIVE_TWO_TRIPS:
                    TwoTripsRPC.Processor two_trips = new TwoTripsRPC.Processor(new TwoTripsRPCImpl());
                    targs.processor(two_trips);
                    break;
                case PUSH_DOWN:
                    PushDownRPC.Processor pushdown = new PushDownRPC.Processor(new PushDownRPCImpl());
                    targs.processor(pushdown);
                    break;
                case PUSH_PULL:
                    PushPullRPC.Processor pushpull = new PushPullRPC.Processor(new PushPullRPCImpl());
                    targs.processor(pushpull);
                    break;
                case NAIVE_MULTI_TRIPS:
                    BasicFilterBasedRPC.Processor multi_trips = new BasicFilterBasedRPC.Processor(new NaiveFilterBasedRPCImpl());
                    targs.processor(multi_trips);
                    break;
                case PULL_ALL:
                    PushDownRPC.Processor pull_all = new PushDownRPC.Processor(new PullAllDataImpl());
                    targs.processor(pull_all);
                    break;
                case NAIVE_SWF_HASH_TABLE:
                    BasicFilterBasedRPC.Processor swf_hash = new BasicFilterBasedRPC.Processor(new SwfFilterBasedRPCImpl());
                    targs.processor(swf_hash);
                    break;
            }
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory(Args.maxMassageLen, Args.maxMassageLen);
            targs.protocolFactory(protocolFactory);

            TFramedTransport.Factory tTransport = new TFramedTransport.Factory(Args.maxMassageLen);
            targs.transportFactory(tTransport);

            TServer server = new TThreadedSelectorServer(targs);
            System.out.println("Starting filter service...");
            // System.out.println("Starting filter service on port " + port + "...");
            server.serve();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(serverTransport != null){
                serverTransport.close();
            }
        }
    }

    private static void redirectOutput(String filename) throws FileNotFoundException {
        if(filename == null || filename.isEmpty()){
            return;
        }

        File outputFile = new File(filename);
        if(!outputFile.isAbsolute() && outputFile.getParentFile() == null){
            String sep = File.separator;
            outputFile = new File(System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output", filename);
        }

        File parent = outputFile.getParentFile();
        if(parent != null && !parent.exists()){
            parent.mkdirs();
        }
        System.setOut(new PrintStream(outputFile));
    }

    private static String stringArg(String[] args, String[] names, String defaultValue) {
        for(String name : names){
            String value = findStringArg(args, name);
            if(value != null){
                return value;
            }
        }
        return defaultValue;
    }

    private static String findStringArg(String[] args, String name) {
        String prefix = name + "=";
        for(int i = 0; i < args.length; i++){
            if(args[i].startsWith(prefix)){
                return args[i].substring(prefix.length());
            }
            if(args[i].equals(name) && i + 1 < args.length){
                return args[i + 1];
            }
        }
        return null;
    }

    private static int intArg(String[] args, String[] names, int defaultValue) {
        String value = stringArg(args, names, Integer.toString(defaultValue));
        return Integer.parseInt(value);
    }

    private static Approach parseApproach(String value) {
        String normalized = value.trim().replace('-', '_').toUpperCase();
        return Approach.valueOf(normalized);
    }
}

/*
Runtime runtime = Runtime.getRuntime();
        //The memory size of the Java virtual machine that has now been mined from the operating system
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();

        System.out.println("Total memory: " + totalMemory / (1024 * 1024) + " MB");
        System.out.println("Free memory: " + freeMemory / (1024 * 1024) + " MB");
        System.out.println("Max memory: " + maxMemory / (1024 * 1024) + " MB");
 */
