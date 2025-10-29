package rpc2;

import compute.Args;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import rpc2.iface.FilterBasedRPC2;
import rpc2.iface.PushDownRPC2;
import rpc2.iface.PushPullRPC2;
import rpc2.iface.TwoTripsRPC2;

public class SNService {

    public static void main(String[] args) {
        Approach approach = Approach.CONCURRENT_OURS;
        System.out.println("approach: " + approach);
        startService(approach);
    }

    public static void startService(Approach approach){
        TNonblockingServerSocket serverTransport = null;
        try {
            int clientTimeOut = 180_000; // 3min
            serverTransport = new TNonblockingServerSocket(9090, clientTimeOut, Args.maxMassageLen);

            TThreadedSelectorServer.Args targs = new TThreadedSelectorServer.Args(serverTransport);
            switch (approach){
                case CONCURRENT_OURS:
                    FilterBasedRPC2.Processor concurrentFilterBased = new FilterBasedRPC2.Processor(new ConcurrentFilterBasedRPCImpl());
                    targs.processor(concurrentFilterBased);
                    break;
                case CONCURRENT_PUSH_DOWN:
                    PushDownRPC2.Processor concurrentPushDown = new PushDownRPC2.Processor(new ConcurrentPushDownRPCImpl());
                    targs.processor(concurrentPushDown);
                    break;
                case CONCURRENT_NAIVE_TWO_TRIPS:
                    TwoTripsRPC2.Processor concurrentTwoTrips = new TwoTripsRPC2.Processor(new ConcurrentTwoTripsRPCImpl());
                    targs.processor(concurrentTwoTrips);
                    break;
                case CONCURRENT_PUSH_PULL:
                    PushPullRPC2.Processor concurrentPushPull = new PushPullRPC2.Processor(new ConcurrentPushPullRPCImpl());
                    targs.processor(concurrentPushPull);
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
