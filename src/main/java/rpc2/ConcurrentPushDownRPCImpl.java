package rpc2;

import compute.Args;
import org.apache.thrift.TException;
import rpc2.iface.DataChunk;
import rpc2.iface.PushDownRPC2;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentPushDownRPCImpl implements PushDownRPC2.Iface{
    // scan lock to avoid multiple scans at the same time
    private final AtomicBoolean scanLock = new AtomicBoolean(false);

    private boolean isScanLocked() {
        return scanLock.get();
    }

    private boolean tryAcquireScanLock() {
        return scanLock.compareAndSet(false, true);
    }

    private void releaseScanLock() {
        scanLock.set(false);
    }

    public static boolean canRead() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        long usedMemory = totalMemory - freeMemory;
        System.out.println("Memory Status: Used " + usedMemory / (1024 * 1024) + " MB, Max " + maxMemory / (1024 * 1024) + " MB");
        return 0.8 * maxMemory > usedMemory;
    }

    public static final AtomicInteger queryId = new AtomicInteger(0);
    private final ConcurrentHashMap<String, ClientState> sessions = new ConcurrentHashMap<>();

    private static class ClientState {
        List<byte[]> filteredRecords;
        int chunkId;
        int recordSize;
        int MAX_RECORD_NUM;

        ClientState() {
            this.filteredRecords = new ArrayList<>();
            this.chunkId = 0;
            this.recordSize = 0;
            this.MAX_RECORD_NUM = 10_000;
        }
    }

    @Override
    public DataChunk pullEvents(String clientId, String tableName, int offset, Map<String, List<String>> ipMap) throws TException {
        ClientState state = sessions.computeIfAbsent(clientId, k -> new ClientState());

        // another scan is in progress, wait
        while (isScanLocked()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TException("Interrupted while waiting for scan lock");
            }
        }

        // if offset is 0, it means a new query arrives
        if (offset == 0) {
            int qid = queryId.getAndIncrement();
            System.out.println(clientId + "," + qid + "-th query arrives");
            state.chunkId = 0;

            while (!canRead()) {
                System.out.println("Waiting for enough memory to do full scan...");
                System.gc();
                try {Thread.sleep(100);}
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            while (!tryAcquireScanLock()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TException("Interrupted while acquiring scan lock");
                }
            }

            long startRead = System.currentTimeMillis();
            List<byte[]> scanned = null;
            int maxRetries = 10;
            long sleepMs = 100;
            try{
                for (int attempt = 0; attempt < maxRetries; attempt++) {
                    FullScan fullscan = null;
                    try {
                        fullscan = new FullScan(tableName);
                        scanned = fullscan.concurrentScan(ipMap);
                        break;
                    } catch (OutOfMemoryError oom) {
                        System.err.println("OOM during concurrentScan, attempt " + attempt + ", sleeping " + sleepMs + "ms");
                        fullscan = null;
                        scanned = null;
                        System.gc();
                        try {
                            Thread.sleep(sleepMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        sleepMs = Math.min(sleepMs * 2, 5000L);
                    }
                }
            }finally {
                releaseScanLock();
            }
            long endRead = System.currentTimeMillis();
            System.out.println(qid + "-th query read cost: " + (endRead - startRead) + "ms");

            if (scanned == null || scanned.isEmpty()) {
                state.recordSize = 0;
                state.MAX_RECORD_NUM = 0;
                sessions.remove(clientId);
                ByteBuffer empty = ByteBuffer.allocate(0);
                return new DataChunk(0, empty, true);
            } else {
                state.filteredRecords = scanned;
                state.recordSize = state.filteredRecords.get(0).length;
                state.MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / state.recordSize;
            }
        }

        int remaining = state.filteredRecords.size() - offset;
        DataChunk dataChunk;
        if (remaining <= state.MAX_RECORD_NUM) {
            // when it is the last chunk
            ByteBuffer buffer = ByteBuffer.allocate(Math.max(0, remaining * state.recordSize));
            for (int i = offset; i < state.filteredRecords.size(); i++) {
                buffer.put(state.filteredRecords.get(i));
                state.filteredRecords.set(i, null); // help GC
            }
            buffer.flip();
            dataChunk = new DataChunk(state.chunkId, buffer, true);
            // clear session
            sessions.remove(clientId);
        } else {
            // it is not the last chunk
            ByteBuffer buffer = ByteBuffer.allocate(state.MAX_RECORD_NUM * state.recordSize);
            for (int i = offset; i < offset + state.MAX_RECORD_NUM; i++) {
                buffer.put(state.filteredRecords.get(i));
                state.filteredRecords.set(i, null); // help GC
            }
            buffer.flip();
            // keep session, wait for next pull
            dataChunk = new DataChunk(state.chunkId++, buffer, false);
        }

        return dataChunk;
    }
}
