package rpc;

import compute.Args;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import rpc.iface.FilterBasedRPC;
import rpc.iface.PushDownRPC;
import rpc.iface.PushPullRPC;
import rpc.iface.TwoTripsRPC;

import java.io.FileNotFoundException;

public class StorageServer {
    public static void main(String[] args) throws FileNotFoundException {
        // please modify this to change various approach
        // TWO_TRIPS, PUSH_DOWN, PUSH_PULL, OURS
        Approach approach = Approach.OURS;
        System.out.println("approach: " + approach);
        StorageServer.startService(approach);
    }

    public static void startService(Approach approach){
        TNonblockingServerSocket serverTransport = null;
        try{
            int clientTimeOut = 180_000; // 3min
            serverTransport = new TNonblockingServerSocket(9090, clientTimeOut, Args.maxMassageLen);
            TThreadedSelectorServer.Args targs = new TThreadedSelectorServer.Args(serverTransport);

            switch(approach){
                case OURS:
                    FilterBasedRPC.Processor ours = new FilterBasedRPC.Processor(new FilterBasedRPCImpl());
                    targs.processor(ours);
                    break;
                case TWO_TRIPS:
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
            }
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory(Args.maxMassageLen, Args.maxMassageLen);
            targs.protocolFactory(protocolFactory);

            TFramedTransport.Factory tTransport = new TFramedTransport.Factory(Args.maxMassageLen);
            targs.transportFactory(tTransport);

            TServer server = new TThreadedSelectorServer(targs);
            System.out.println("Starting filter service...");
            server.serve();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            serverTransport.close();
        }
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

//        String sep = File.separator;
//        String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output" + sep + "test.txt";
//        System.setOut(new PrintStream(filePath));
