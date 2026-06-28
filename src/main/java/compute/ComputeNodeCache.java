package compute;

import engine.NFA;
import engine.SelectionStrategy;
import filter.BasicBF;
import filter.OptimizedSWF;
import filter.SWF;
import filter.WindowWiseBF;
import parser.EqualDependentPredicate;
import parser.IndependentPredicate;
import parser.QueryParse;
import rpc.iface.DataChunk;
import rpc.iface.ExtendDataChunk;
import rpc.iface.ExtendFilterBasedRPC;
import rpc.iface.FilterBasedRPC;
import rpc.iface.FilteredResult;
import rpc.iface.PushDownRPC;
import rpc.iface.PushPullDataChunk;
import rpc.iface.PushPullRPC;
import rpc.iface.SameDataChunk;
import rpc.iface.TsAttrPair;
import rpc.iface.TwoTripsDataChunk;
import rpc.iface.TwoTripsRPC;
import store.ColumnInfo;
import store.EventCache;
import store.EventSchema;
import store.EventStore;
import store.FullScan;
import utils.Pair;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Compute-side cache for leading storage pages.
 *
 * A loaded instance stores raw cached pages in compute-node memory. Each query
 * scans that in-memory content as one logical cache and returns a query-local
 * instance backed by EventCache.
 */
public class ComputeNodeCache {
    private static final long DISABLE_STORAGE_CACHE_WINDOW = Long.MIN_VALUE;
    private static final String PUSH_PULL_CACHE_PAGE_KEY = "__compute_cache_pages__";

    private final EventSchema schema;
    private final List<byte[]> cachedPages;
    private final List<List<byte[]>> cachedPagesByNode;
    private final long cachedBytes;

    private final EventCache cache;
    private final List<String> hasFilteredVarNames;
    private OptimizedSWF optimizedSWF;
    private SWF swf;

    private QueryParse pushPullQuery;
    private Set<String> pushPullProcessedVarNames;
    private List<byte[]> pushPullAllEvents;
    private Map<String, List<byte[]>> pushPullFilteredRecordsMap;

    private ComputeNodeCache(EventSchema schema, List<byte[]> cachedPages, long cachedBytes) {
        this.schema = schema;
        this.cachedPages = cachedPages;
        this.cachedPagesByNode = null;
        this.cachedBytes = cachedBytes;
        this.cache = null;
        this.hasFilteredVarNames = null;
    }

    private ComputeNodeCache(EventSchema schema, List<List<byte[]>> cachedPagesByNode, long cachedBytes, boolean groupedByNode) {
        this.schema = schema;
        this.cachedPages = null;
        this.cachedPagesByNode = cachedPagesByNode;
        this.cachedBytes = cachedBytes;
        this.cache = null;
        this.hasFilteredVarNames = null;
    }

    private ComputeNodeCache(EventCache cache) {
        this.schema = cache.getSchema();
        this.cachedPages = null;
        this.cachedPagesByNode = null;
        this.cachedBytes = 0;
        this.cache = cache;
        this.hasFilteredVarNames = new ArrayList<>(8);
    }

    public static ComputeNodeCache load(String tableName, List<FilterBasedRPC.Client> clients, int cachePageNum) {
        if(clients == null || clients.isEmpty()){
            return null;
        }

        EventSchema schema = EventSchema.getEventSchema(tableName);
        List<CachePageFetchThread> threads = new ArrayList<>(clients.size());
        for(FilterBasedRPC.Client client : clients){
            CachePageFetchThread thread = new CachePageFetchThread(client, tableName, cachePageNum);
            thread.start();
            threads.add(thread);
        }

        List<byte[]> cachedPages = new ArrayList<>(Math.max(16, clients.size() * Math.min(cachePageNum, 8192)));
        long loadedBytes = 0;
        for(CachePageFetchThread thread : threads){
            try{
                thread.join();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while fetching compute cache pages", e);
            }
            if(thread.getError() != null){
                throw thread.getError();
            }
            if(cachePageNum <= 0){
                continue;
            }
            cachedPages.addAll(thread.getPages());
            loadedBytes += thread.getLoadedBytes();
        }
        if(cachePageNum <= 0){
            return null;
        }
        // System.out.println("compute cache loaded pages: " + cachedPages.size() + ", bytes: " + loadedBytes);
        return new ComputeNodeCache(schema, cachedPages, loadedBytes);
    }

    public static ComputeNodeCache loadForExtend(String tableName, List<ExtendFilterBasedRPC.Client> clients, int cachePageNum) {
        if(clients == null || clients.isEmpty()){
            return null;
        }

        EventSchema schema = EventSchema.getEventSchema(tableName);
        List<ExtendCachePageFetchThread> threads = new ArrayList<>(clients.size());
        for(ExtendFilterBasedRPC.Client client : clients){
            ExtendCachePageFetchThread thread = new ExtendCachePageFetchThread(client, tableName, cachePageNum);
            thread.start();
            threads.add(thread);
        }

        List<byte[]> cachedPages = new ArrayList<>(Math.max(16, clients.size() * Math.min(Math.max(cachePageNum, 0), 8192)));
        long loadedBytes = 0;
        for(ExtendCachePageFetchThread thread : threads){
            try{
                thread.join();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while fetching extend compute cache pages", e);
            }
            if(thread.getError() != null){
                throw thread.getError();
            }
            if(cachePageNum <= 0){
                continue;
            }
            cachedPages.addAll(thread.getPages());
            loadedBytes += thread.getLoadedBytes();
        }
        if(cachePageNum <= 0){
            return null;
        }
        return new ComputeNodeCache(schema, cachedPages, loadedBytes);
    }

    public static ComputeNodeCache loadForPushDown(String tableName, List<PushDownRPC.Client> clients, int cachePageNum) {
        if(clients == null || clients.isEmpty()){
            return null;
        }

        EventSchema schema = EventSchema.getEventSchema(tableName);
        List<PushDownCachePageFetchThread> threads = new ArrayList<>(clients.size());
        for(PushDownRPC.Client client : clients){
            PushDownCachePageFetchThread thread = new PushDownCachePageFetchThread(client, tableName, cachePageNum);
            thread.start();
            threads.add(thread);
        }

        List<byte[]> cachedPages = new ArrayList<>(Math.max(16, clients.size() * Math.min(Math.max(cachePageNum, 0), 8192)));
        long loadedBytes = 0;
        int loadedPageNum = 0;
        for(PushDownCachePageFetchThread thread : threads){
            try{
                thread.join();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while fetching push-down compute cache pages", e);
            }
            if(thread.getError() != null){
                throw thread.getError();
            }
            if(cachePageNum <= 0){
                continue;
            }
            cachedPages.addAll(thread.getPages());
            loadedPageNum += thread.getPages().size();
            loadedBytes += thread.getLoadedBytes();
        }
        if(cachePageNum <= 0){
            return null;
        }
        System.out.println("push-down compute cache loaded pages: " + loadedPageNum + ", bytes: " + loadedBytes);
        return new ComputeNodeCache(schema, cachedPages, loadedBytes);
    }

    public static ComputeNodeCache loadForPushPull(String tableName, List<PushPullRPC.Client> clients, int cachePageNum) {
        if(clients == null || clients.isEmpty()){
            return null;
        }

        EventSchema schema = EventSchema.getEventSchema(tableName);
        List<PushPullCachePageFetchThread> threads = new ArrayList<>(clients.size());
        for(PushPullRPC.Client client : clients){
            PushPullCachePageFetchThread thread = new PushPullCachePageFetchThread(client, tableName, cachePageNum);
            thread.start();
            threads.add(thread);
        }

        List<List<byte[]>> cachedPagesByNode = new ArrayList<>(clients.size());
        long loadedBytes = 0;
        int loadedPageNum = 0;
        for(PushPullCachePageFetchThread thread : threads){
            try{
                thread.join();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while fetching push-pull compute cache pages", e);
            }
            if(thread.getError() != null){
                throw thread.getError();
            }
            if(cachePageNum <= 0){
                continue;
            }
            cachedPagesByNode.add(thread.getPages());
            loadedPageNum += thread.getPages().size();
            loadedBytes += thread.getLoadedBytes();
        }
        if(cachePageNum <= 0){
            return null;
        }
        System.out.println("push-pull compute cache loaded pages: " + loadedPageNum + ", bytes: " + loadedBytes);
        return new ComputeNodeCache(schema, cachedPagesByNode, loadedBytes, true);
    }

    public static ComputeNodeCache loadForTwoTrips(String tableName, List<TwoTripsRPC.Client> clients, int cachePageNum) {
        if(clients == null || clients.isEmpty()){
            return null;
        }

        EventSchema schema = EventSchema.getEventSchema(tableName);
        List<TwoTripsCachePageFetchThread> threads = new ArrayList<>(clients.size());
        for(TwoTripsRPC.Client client : clients){
            TwoTripsCachePageFetchThread thread = new TwoTripsCachePageFetchThread(client, tableName, cachePageNum);
            thread.start();
            threads.add(thread);
        }

        List<byte[]> cachedPages = new ArrayList<>(Math.max(16, clients.size() * Math.min(Math.max(cachePageNum, 0), 8192)));
        long loadedBytes = 0;
        for(TwoTripsCachePageFetchThread thread : threads){
            try{
                thread.join();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while fetching two-trips compute cache pages", e);
            }
            if(thread.getError() != null){
                throw thread.getError();
            }
            if(cachePageNum <= 0){
                continue;
            }
            cachedPages.addAll(thread.getPages());
            loadedBytes += thread.getLoadedBytes();
        }
        if(cachePageNum <= 0){
            return null;
        }
        // System.out.println("two-trips compute cache loaded pages: " + cachedPages.size() + ", bytes: " + loadedBytes);
        return new ComputeNodeCache(schema, cachedPages, loadedBytes);
    }

    static class CachePageFetchThread extends Thread {
        private final FilterBasedRPC.Client client;
        private final String tableName;
        private final int cachePageNum;
        private final List<byte[]> pages;
        private RuntimeException error;
        private long loadedBytes;

        CachePageFetchThread(FilterBasedRPC.Client client, String tableName, int cachePageNum) {
            this.client = client;
            this.tableName = tableName;
            this.cachePageNum = cachePageNum;
            this.pages = new ArrayList<>(Math.min(Math.max(cachePageNum, 0), 8192));
        }

        public List<byte[]> getPages() {
            return pages;
        }

        public RuntimeException getError() {
            return error;
        }

        public long getLoadedBytes() {
            return loadedBytes;
        }

        @Override
        public void run() {
            try{
                if(cachePageNum <= 0){
                    ByteBuffer requestBuffer = ByteBuffer.wrap(tableName.getBytes(StandardCharsets.UTF_8));
                    client.getAllFilteredEvents(DISABLE_STORAGE_CACHE_WINDOW, requestBuffer);
                    return;
                }
                boolean isLastChunk = false;
                boolean isFirstChunk = true;
                while(!isLastChunk){
                    ByteBuffer requestBuffer = isFirstChunk ?
                            ByteBuffer.wrap(tableName.getBytes(StandardCharsets.UTF_8)) : null;
                    SameDataChunk chunk = client.getAllFilteredEvents(-cachePageNum, requestBuffer);
                    appendPages(chunk.data);
                    isLastChunk = chunk.isLastChunk;
                    isFirstChunk = false;
                }
            }catch (Exception e){
                error = new RuntimeException("Failed to fetch compute cache pages from storage node", e);
            }
        }

        private void appendPages(ByteBuffer buffer) {
            while(buffer != null && buffer.remaining() > 0){
                int pageLen = Math.min(EventStore.pageSize, buffer.remaining());
                byte[] page = new byte[pageLen];
                buffer.get(page);
                pages.add(page);
                loadedBytes += pageLen;
            }
        }
    }

    static class ExtendCachePageFetchThread extends Thread {
        private final ExtendFilterBasedRPC.Client client;
        private final String tableName;
        private final int cachePageNum;
        private final List<byte[]> pages;
        private RuntimeException error;
        private long loadedBytes;

        ExtendCachePageFetchThread(ExtendFilterBasedRPC.Client client, String tableName, int cachePageNum) {
            this.client = client;
            this.tableName = tableName;
            this.cachePageNum = cachePageNum;
            this.pages = new ArrayList<>(Math.min(Math.max(cachePageNum, 0), 8192));
        }

        public List<byte[]> getPages() {
            return pages;
        }

        public RuntimeException getError() {
            return error;
        }

        public long getLoadedBytes() {
            return loadedBytes;
        }

        @Override
        public void run() {
            try{
                ByteBuffer requestBuffer = ByteBuffer.wrap(tableName.getBytes(StandardCharsets.UTF_8));
                if(cachePageNum <= 0){
                    client.getAllFilteredEvents(DISABLE_STORAGE_CACHE_WINDOW, requestBuffer);
                    return;
                }
                boolean isLastChunk = false;
                boolean isFirstChunk = true;
                while(!isLastChunk){
                    ByteBuffer tableNameBuffer = isFirstChunk ?
                            ByteBuffer.wrap(tableName.getBytes(StandardCharsets.UTF_8)) : null;
                    ExtendDataChunk chunk = client.getAllFilteredEvents(-cachePageNum, tableNameBuffer);
                    appendPages(chunk.data);
                    isLastChunk = chunk.isLastChunk;
                    isFirstChunk = false;
                }
            }catch (Exception e){
                error = new RuntimeException("Failed to fetch extend compute cache pages from storage node", e);
            }
        }

        private void appendPages(ByteBuffer buffer) {
            while(buffer != null && buffer.remaining() > 0){
                int pageLen = Math.min(EventStore.pageSize, buffer.remaining());
                byte[] page = new byte[pageLen];
                buffer.get(page);
                pages.add(page);
                loadedBytes += pageLen;
            }
        }
    }

    static class PushDownCachePageFetchThread extends Thread {
        private static final int DISABLE_COMPUTE_CACHE_OFFSET = Integer.MIN_VALUE;

        private final PushDownRPC.Client client;
        private final String tableName;
        private final int cachePageNum;
        private final List<byte[]> pages;
        private RuntimeException error;
        private long loadedBytes;

        PushDownCachePageFetchThread(PushDownRPC.Client client, String tableName, int cachePageNum) {
            this.client = client;
            this.tableName = tableName;
            this.cachePageNum = cachePageNum;
            this.pages = new ArrayList<>(Math.min(Math.max(cachePageNum, 0), 8192));
        }

        public List<byte[]> getPages() {
            return pages;
        }

        public RuntimeException getError() {
            return error;
        }

        public long getLoadedBytes() {
            return loadedBytes;
        }

        @Override
        public void run() {
            try{
                if(cachePageNum <= 0){
                    client.pullEvents(tableName, DISABLE_COMPUTE_CACHE_OFFSET, new HashMap<String, List<String>>(0));
                    return;
                }

                boolean isLastChunk;
                int cacheRequestOffset = -cachePageNum;
                do {
                    DataChunk chunk = client.pullEvents(tableName, cacheRequestOffset, new HashMap<String, List<String>>(0));
                    appendPages(chunk.data);
                    isLastChunk = chunk.isLastChunk;
                }while(!isLastChunk);
            }catch (Exception e){
                error = new RuntimeException("Failed to fetch push-down compute cache pages from storage node", e);
            }
        }

        private void appendPages(ByteBuffer buffer) {
            while(buffer != null && buffer.remaining() > 0){
                int pageLen = Math.min(EventStore.pageSize, buffer.remaining());
                byte[] page = new byte[pageLen];
                buffer.get(page);
                pages.add(page);
                loadedBytes += pageLen;
            }
        }
    }

    static class PushPullCachePageFetchThread extends Thread {
        private static final int DISABLE_COMPUTE_CACHE_OFFSET = Integer.MIN_VALUE;

        private final PushPullRPC.Client client;
        private final String tableName;
        private final int cachePageNum;
        private final List<byte[]> pages;
        private RuntimeException error;
        private long loadedBytes;

        PushPullCachePageFetchThread(PushPullRPC.Client client, String tableName, int cachePageNum) {
            this.client = client;
            this.tableName = tableName;
            this.cachePageNum = cachePageNum;
            this.pages = new ArrayList<>(Math.min(Math.max(cachePageNum, 0), 8192));
        }

        public List<byte[]> getPages() {
            return pages;
        }

        public RuntimeException getError() {
            return error;
        }

        public long getLoadedBytes() {
            return loadedBytes;
        }

        @Override
        public void run() {
            try{
                List<String> request = Collections.singletonList(tableName);
                if(cachePageNum <= 0){
                    client.getEventsByVarName(request, DISABLE_COMPUTE_CACHE_OFFSET);
                    return;
                }

                boolean isLastChunk;
                int cacheRequestOffset = -cachePageNum;
                do {
                    PushPullDataChunk chunk = client.getEventsByVarName(request, cacheRequestOffset);
                    appendPages(getCachePageBuffer(chunk));
                    isLastChunk = chunk.isLastChunk;
                }while(!isLastChunk);
            }catch (Exception e){
                error = new RuntimeException("Failed to fetch push-pull compute cache pages from storage node", e);
            }
        }

        private ByteBuffer getCachePageBuffer(PushPullDataChunk chunk) {
            if(chunk == null || chunk.varEventMap == null || chunk.varEventMap.isEmpty()){
                return null;
            }
            ByteBuffer buffer = chunk.varEventMap.get(PUSH_PULL_CACHE_PAGE_KEY);
            if(buffer != null){
                return buffer;
            }
            return chunk.varEventMap.values().iterator().next();
        }

        private void appendPages(ByteBuffer buffer) {
            while(buffer != null && buffer.remaining() > 0){
                int pageLen = Math.min(EventStore.pageSize, buffer.remaining());
                byte[] page = new byte[pageLen];
                buffer.get(page);
                pages.add(page);
                loadedBytes += pageLen;
            }
        }
    }

    static class TwoTripsCachePageFetchThread extends Thread {
        private static final int DISABLE_COMPUTE_CACHE_OFFSET = Integer.MIN_VALUE;

        private final TwoTripsRPC.Client client;
        private final String tableName;
        private final int cachePageNum;
        private final List<byte[]> pages;
        private RuntimeException error;
        private long loadedBytes;

        TwoTripsCachePageFetchThread(TwoTripsRPC.Client client, String tableName, int cachePageNum) {
            this.client = client;
            this.tableName = tableName;
            this.cachePageNum = cachePageNum;
            this.pages = new ArrayList<>(Math.min(Math.max(cachePageNum, 0), 8192));
        }

        public List<byte[]> getPages() {
            return pages;
        }

        public RuntimeException getError() {
            return error;
        }

        public long getLoadedBytes() {
            return loadedBytes;
        }

        @Override
        public void run() {
            try{
                if(cachePageNum <= 0){
                    client.pullBasicInfo(tableName, "", DISABLE_COMPUTE_CACHE_OFFSET);
                    return;
                }

                boolean isLastChunk;
                int cacheRequestOffset = -cachePageNum;
                do {
                    TwoTripsDataChunk chunk = client.pullBasicInfo(tableName, "", cacheRequestOffset);
                    appendPages(getCachePageBuffer(chunk));
                    isLastChunk = chunk.isLastChunk;
                }while(!isLastChunk);
            }catch (Exception e){
                error = new RuntimeException("Failed to fetch two-trips compute cache pages from storage node", e);
            }
        }

        private ByteBuffer getCachePageBuffer(TwoTripsDataChunk chunk) {
            if(chunk == null || chunk.intervalMap == null || chunk.intervalMap.isEmpty()){
                return null;
            }
            ByteBuffer buffer = chunk.intervalMap.get(PUSH_PULL_CACHE_PAGE_KEY);
            if(buffer != null){
                return buffer;
            }
            return chunk.intervalMap.values().iterator().next();
        }

        private void appendPages(ByteBuffer buffer) {
            while(buffer != null && buffer.remaining() > 0){
                int pageLen = Math.min(EventStore.pageSize, buffer.remaining());
                byte[] page = new byte[pageLen];
                buffer.get(page);
                pages.add(page);
                loadedBytes += pageLen;
            }
        }
    }

    public List<byte[]> filterForPushDown(Map<String, List<String>> ipStrMap) {
        List<List<byte[]>> recordsByNode = filterForPushDownByNode(ipStrMap);
        List<byte[]> filteredRecords = new ArrayList<>(4096);
        for(List<byte[]> records : recordsByNode){
            filteredRecords.addAll(records);
        }
        return filteredRecords;
    }

    public List<List<byte[]>> filterForPushDownByNode(Map<String, List<String>> ipStrMap) {
        if(cachedPages == null && cachedPagesByNode == null){
            throw new IllegalStateException("Only a loaded compute cache can filter push-down predicates.");
        }

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = FullScan.parseIpString(ipStrMap, schema);
        if(cachedPagesByNode != null){
            List<List<byte[]>> filteredRecordsByNode = new ArrayList<>(cachedPagesByNode.size());
            for(List<byte[]> nodePages : cachedPagesByNode){
                filteredRecordsByNode.add(filterPagesForPushDown(nodePages, ipMap));
            }
            return filteredRecordsByNode;
        }
        List<List<byte[]>> filteredRecordsByNode = new ArrayList<>(1);
        filteredRecordsByNode.add(filterPagesForPushDown(cachedPages, ipMap));
        return filteredRecordsByNode;
    }

    public List<List<byte[]>> filterForPushDownByNodeInParallel(Map<String, List<String>> ipStrMap, int threadNum) {
        if(cachedPages == null && cachedPagesByNode == null){
            throw new IllegalStateException("Only a loaded compute cache can filter push-down predicates.");
        }
        if(threadNum <= 1){
            return filterForPushDownByNode(ipStrMap);
        }

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = FullScan.parseIpString(ipStrMap, schema);
        List<List<byte[]>> pageGroups = cachedPagesByNode == null ?
                Collections.singletonList(cachedPages) : cachedPagesByNode;

        List<List<ComputeCachePushDownWorker>> workersByNode = new ArrayList<>(pageGroups.size());
        for(List<byte[]> nodePages : pageGroups){
            workersByNode.add(startPushDownWorkers(nodePages, ipMap, threadNum));
        }

        List<List<byte[]>> filteredRecordsByNode = new ArrayList<>(workersByNode.size());
        for(List<ComputeCachePushDownWorker> workers : workersByNode){
            int estimatedSize = 0;
            for(ComputeCachePushDownWorker worker : workers){
                try{
                    worker.join();
                }catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while filtering push-down compute cache pages", e);
                }
                if(worker.getError() != null){
                    throw worker.getError();
                }
                estimatedSize += worker.getFilteredRecords().size();
            }

            List<byte[]> nodeRecords = new ArrayList<>(estimatedSize);
            for(ComputeCachePushDownWorker worker : workers){
                nodeRecords.addAll(worker.getFilteredRecords());
            }
            filteredRecordsByNode.add(nodeRecords);
        }
        return filteredRecordsByNode;
    }

    private List<ComputeCachePushDownWorker> startPushDownWorkers(List<byte[]> pages,
            Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap, int threadNum) {
        if(pages == null || pages.isEmpty()){
            return new ArrayList<>(0);
        }

        int workerNum = Math.max(1, Math.min(threadNum, pages.size()));
        List<ComputeCachePushDownWorker> workers = new ArrayList<>(workerNum);
        int pagesPerWorker = (pages.size() + workerNum - 1) / workerNum;
        for(int i = 0; i < workerNum; i++){
            int startPage = i * pagesPerWorker;
            int endPage = Math.min(pages.size(), startPage + pagesPerWorker);
            if(startPage >= endPage){
                break;
            }
            ComputeCachePushDownWorker worker = new ComputeCachePushDownWorker(pages, startPage, endPage, schema, ipMap);
            worker.start();
            workers.add(worker);
        }
        return workers;
    }

    private List<byte[]> filterPagesForPushDown(List<byte[]> pages,
                                                Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap) {
        List<byte[]> filteredRecords = new ArrayList<>(4096);

        int recordLen = schema.getFixedRecordLen();
        for(byte[] page : pages){
            int curRecordNum = page.length / recordLen;
            ByteBuffer bb = ByteBuffer.wrap(page);
            for(int i = 0; i < curRecordNum; i++){
                int pagePos = i * recordLen;
                for(List<Pair<IndependentPredicate, ColumnInfo>> ips : ipMap.values()){
                    boolean satisfied = true;
                    for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                        IndependentPredicate ip = pair.getKey();
                        ColumnInfo columnInfo = pair.getValue();
                        Object obj = schema.getColumnValue(columnInfo, bb, pagePos);
                        if(!ip.check(obj, columnInfo.getDataType())){
                            satisfied = false;
                            break;
                        }
                    }

                    if(satisfied){
                        byte[] record = new byte[recordLen];
                        System.arraycopy(page, pagePos, record, 0, recordLen);
                        filteredRecords.add(record);
                        break;
                    }
                }
            }
        }
        return filteredRecords;
    }

    static class ComputeCachePushDownWorker extends Thread {
        private final List<byte[]> pages;
        private final int startPage;
        private final int endPage;
        private final EventSchema schema;
        private final Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap;
        private final List<byte[]> filteredRecords;
        private RuntimeException error;

        ComputeCachePushDownWorker(List<byte[]> pages, int startPage, int endPage, EventSchema schema,
                                   Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap) {
            this.pages = pages;
            this.startPage = startPage;
            this.endPage = endPage;
            this.schema = schema;
            this.ipMap = ipMap;
            this.filteredRecords = new ArrayList<>(4096);
        }

        public List<byte[]> getFilteredRecords() {
            return filteredRecords;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public void run() {
            try{
                int recordLen = schema.getFixedRecordLen();
                for(int pageIdx = startPage; pageIdx < endPage; pageIdx++){
                    byte[] page = pages.get(pageIdx);
                    int curRecordNum = page.length / recordLen;
                    ByteBuffer bb = ByteBuffer.wrap(page);
                    for(int i = 0; i < curRecordNum; i++){
                        int pagePos = i * recordLen;
                        for(List<Pair<IndependentPredicate, ColumnInfo>> ips : ipMap.values()){
                            boolean satisfied = true;
                            for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                                IndependentPredicate ip = pair.getKey();
                                ColumnInfo columnInfo = pair.getValue();
                                Object obj = schema.getColumnValue(columnInfo, bb, pagePos);
                                if(!ip.check(obj, columnInfo.getDataType())){
                                    satisfied = false;
                                    break;
                                }
                            }

                            if(satisfied){
                                byte[] record = new byte[recordLen];
                                System.arraycopy(page, pagePos, record, 0, recordLen);
                                filteredRecords.add(record);
                                break;
                            }
                        }
                    }
                }
            }catch (RuntimeException e){
                error = e;
            }
        }
    }

    public ComputeNodeCache scan(Map<String, List<String>> ipStrMap) {
        if(cachedPages == null){
            throw new IllegalStateException("Only a loaded compute cache can scan query predicates.");
        }

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = FullScan.parseIpString(ipStrMap, schema);
        return new ComputeNodeCache(scanPages(cachedPages, ipMap));
    }

    private EventCache scanPages(List<byte[]> pages,
                                 Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap) {
        List<byte[]> filteredRecords = new ArrayList<>(4096);
        Map<String, List<Integer>> varPointers = new HashMap<>(ipMap.size() << 2);
        for(String varName : ipMap.keySet()){
            varPointers.put(varName, new ArrayList<>(2048));
        }

        int recordLen = schema.getFixedRecordLen();
        int recordNum = 0;
        for(byte[] page : pages){
            int curRecordNum = page.length / recordLen;
            ByteBuffer bb = ByteBuffer.wrap(page);
            for(int i = 0; i < curRecordNum; i++){
                boolean noInserted = true;
                int pagePos = i * recordLen;
                for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()){
                    List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                    boolean satisfied = true;
                    for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                        IndependentPredicate ip = pair.getKey();
                        ColumnInfo columnInfo = pair.getValue();
                        Object obj = schema.getColumnValue(columnInfo, bb, pagePos);
                        if(!ip.check(obj, columnInfo.getDataType())){
                            satisfied = false;
                            break;
                        }
                    }

                    if(satisfied){
                        if(noInserted){
                            byte[] record = new byte[recordLen];
                            System.arraycopy(page, pagePos, record, 0, recordLen);
                            filteredRecords.add(record);
                            varPointers.get(entry.getKey()).add(recordNum);
                            recordNum++;
                            noInserted = false;
                        }else{
                            varPointers.get(entry.getKey()).add(recordNum - 1);
                        }
                    }
                }
            }
        }

        return new EventCache(schema, filteredRecords, varPointers);
    }

    private EventCache scanPages(List<byte[]> pages,
                                 Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap,
                                 ReplayIntervals replayIntervals) {
        List<byte[]> filteredRecords = new ArrayList<>(4096);
        Map<String, List<Integer>> varPointers = new HashMap<>(ipMap.size() << 2);
        for(String varName : ipMap.keySet()){
            varPointers.put(varName, new ArrayList<>(2048));
        }

        int recordLen = schema.getFixedRecordLen();
        int recordNum = 0;
        for(byte[] page : pages){
            int curRecordNum = page.length / recordLen;
            ByteBuffer bb = ByteBuffer.wrap(page);
            for(int i = 0; i < curRecordNum; i++){
                int pagePos = i * recordLen;
                long timestamp = schema.getTimestamp(bb, pagePos);
                if(!replayIntervals.contains(timestamp)){
                    continue;
                }

                boolean noInserted = true;
                for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()){
                    List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                    boolean satisfied = true;
                    for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                        IndependentPredicate ip = pair.getKey();
                        ColumnInfo columnInfo = pair.getValue();
                        Object obj = schema.getColumnValue(columnInfo, bb, pagePos);
                        if(!ip.check(obj, columnInfo.getDataType())){
                            satisfied = false;
                            break;
                        }
                    }

                    if(satisfied){
                        if(noInserted){
                            byte[] record = new byte[recordLen];
                            System.arraycopy(page, pagePos, record, 0, recordLen);
                            filteredRecords.add(record);
                            varPointers.get(entry.getKey()).add(recordNum);
                            recordNum++;
                            noInserted = false;
                        }else{
                            varPointers.get(entry.getKey()).add(recordNum - 1);
                        }
                    }
                }
            }
        }

        return new EventCache(schema, filteredRecords, varPointers);
    }

    public ComputeNodeCache scanInParallel(Map<String, List<String>> ipStrMap, int threadNum) {
        if(cachedPages == null){
            throw new IllegalStateException("Only a loaded compute cache can scan query predicates.");
        }

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = FullScan.parseIpString(ipStrMap, schema);
        return new ComputeNodeCache(scanPagesInParallel(cachedPages, ipMap, null, threadNum));
    }

    public ComputeNodeCache scanInParallel(Map<String, List<String>> ipStrMap,
                                           ReplayIntervals replayIntervals,
                                           int threadNum) {
        if(cachedPages == null){
            throw new IllegalStateException("Only a loaded compute cache can scan query predicates.");
        }

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = FullScan.parseIpString(ipStrMap, schema);
        return new ComputeNodeCache(scanPagesInParallel(cachedPages, ipMap, replayIntervals, threadNum));
    }

    private EventCache scanPagesInParallel(List<byte[]> pages,
                                           Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap,
                                           ReplayIntervals replayIntervals,
                                           int threadNum) {
        if(pages.isEmpty()){
            return replayIntervals == null ? scanPages(pages, ipMap) : scanPages(pages, ipMap, replayIntervals);
        }

        int workerNum = Math.max(1, Math.min(threadNum, pages.size()));
        if(workerNum == 1){
            return replayIntervals == null ? scanPages(pages, ipMap) : scanPages(pages, ipMap, replayIntervals);
        }

        List<ComputeCacheScanWorker> workers = new ArrayList<>(workerNum);
        int pagesPerWorker = (pages.size() + workerNum - 1) / workerNum;
        for(int i = 0; i < workerNum; i++){
            int startPage = i * pagesPerWorker;
            int endPage = Math.min(pages.size(), startPage + pagesPerWorker);
            if(startPage >= endPage){
                break;
            }
            ComputeCacheScanWorker worker = new ComputeCacheScanWorker(pages, startPage, endPage, schema, ipMap, replayIntervals);
            worker.start();
            workers.add(worker);
        }

        for(ComputeCacheScanWorker worker : workers){
            try{
                worker.join();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while scanning compute cache pages", e);
            }
            if(worker.getError() != null){
                throw worker.getError();
            }
        }

        int estimatedSize = 0;
        for(ComputeCacheScanWorker worker : workers){
            estimatedSize += worker.getFilteredRecords().size();
        }
        List<byte[]> filteredRecords = new ArrayList<>(estimatedSize);
        Map<String, List<Integer>> mergedVarPointers = new HashMap<>(ipMap.size() << 2);
        for(String varName : ipMap.keySet()){
            mergedVarPointers.put(varName, new ArrayList<Integer>(1024));
        }

        for(ComputeCacheScanWorker worker : workers){
            int offset = filteredRecords.size();
            filteredRecords.addAll(worker.getFilteredRecords());
            for(Map.Entry<String, List<Integer>> entry : worker.getVarPointers().entrySet()){
                List<Integer> mergedPointers = mergedVarPointers.get(entry.getKey());
                for(Integer pointer : entry.getValue()){
                    mergedPointers.add(pointer + offset);
                }
            }
        }

        return new EventCache(schema, filteredRecords, mergedVarPointers);
    }

    static class ComputeCacheScanWorker extends Thread {
        private final List<byte[]> pages;
        private final int startPage;
        private final int endPage;
        private final EventSchema schema;
        private final Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap;
        private final ReplayIntervals replayIntervals;
        private final List<byte[]> filteredRecords;
        private final Map<String, List<Integer>> varPointers;
        private RuntimeException error;

        ComputeCacheScanWorker(List<byte[]> pages, int startPage, int endPage, EventSchema schema,
                               Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap,
                               ReplayIntervals replayIntervals) {
            this.pages = pages;
            this.startPage = startPage;
            this.endPage = endPage;
            this.schema = schema;
            this.ipMap = ipMap;
            this.replayIntervals = replayIntervals;
            this.filteredRecords = new ArrayList<>(4096);
            this.varPointers = new HashMap<>(ipMap.size() << 2);
            for(String varName : ipMap.keySet()){
                this.varPointers.put(varName, new ArrayList<Integer>(2048));
            }
        }

        public List<byte[]> getFilteredRecords() {
            return filteredRecords;
        }

        public Map<String, List<Integer>> getVarPointers() {
            return varPointers;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public void run() {
            try{
                int recordLen = schema.getFixedRecordLen();
                int recordNum = 0;
                for(int pageIdx = startPage; pageIdx < endPage; pageIdx++){
                    byte[] page = pages.get(pageIdx);
                    int curRecordNum = page.length / recordLen;
                    ByteBuffer bb = ByteBuffer.wrap(page);
                    for(int i = 0; i < curRecordNum; i++){
                        boolean noInserted = true;
                        int pagePos = i * recordLen;
                        if(replayIntervals != null){
                            long timestamp = schema.getTimestamp(bb, pagePos);
                            if(!replayIntervals.contains(timestamp)){
                                continue;
                            }
                        }
                        for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()){
                            List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                            boolean satisfied = true;
                            for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                                IndependentPredicate ip = pair.getKey();
                                ColumnInfo columnInfo = pair.getValue();
                                Object obj = schema.getColumnValue(columnInfo, bb, pagePos);
                                if(!ip.check(obj, columnInfo.getDataType())){
                                    satisfied = false;
                                    break;
                                }
                            }

                            if(satisfied){
                                if(noInserted){
                                    byte[] record = new byte[recordLen];
                                    System.arraycopy(page, pagePos, record, 0, recordLen);
                                    filteredRecords.add(record);
                                    varPointers.get(entry.getKey()).add(recordNum);
                                    recordNum++;
                                    noInserted = false;
                                }else{
                                    varPointers.get(entry.getKey()).add(recordNum - 1);
                                }
                            }
                        }
                    }
                }
            }catch (RuntimeException e){
                error = e;
            }
        }
    }

    public ComputeNodeCache scanForPushPull(QueryParse query) {
        ComputeNodeCache scannedCache = scan(query.getIpStringMap());
        return initializePushPullCache(scannedCache, query);
    }

    public List<ComputeNodeCache> scanForPushPullByNode(QueryParse query) {
        return scanForPushPullByNode(query, 1);
    }

    public List<ComputeNodeCache> scanForPushPullCombined(QueryParse query, int threadNum) {
        if(cachedPagesByNode == null){
            List<ComputeNodeCache> single = new ArrayList<>(1);
            ComputeNodeCache scannedCache = scanInParallel(query.getIpStringMap(), threadNum);
            single.add(initializePushPullCache(scannedCache, query));
            return single;
        }

        int pageNum = 0;
        for(List<byte[]> nodePages : cachedPagesByNode){
            pageNum += nodePages.size();
        }

        List<byte[]> allPages = new ArrayList<>(pageNum);
        for(List<byte[]> nodePages : cachedPagesByNode){
            allPages.addAll(nodePages);
        }

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap =
                FullScan.parseIpString(query.getIpStringMap(), schema);
        int workerNum = Math.max(1, threadNum * cachedPagesByNode.size());
        ComputeNodeCache scannedCache = new ComputeNodeCache(scanPagesInParallel(allPages, ipMap, null, workerNum));

        List<ComputeNodeCache> single = new ArrayList<>(1);
        single.add(initializePushPullCache(scannedCache, query));
        return single;
    }

    public List<ComputeNodeCache> scanForPushPullByNode(QueryParse query, int threadNum) {
        if(cachedPagesByNode == null){
            List<ComputeNodeCache> single = new ArrayList<>(1);
            ComputeNodeCache scannedCache = scanInParallel(query.getIpStringMap(), threadNum);
            single.add(initializePushPullCache(scannedCache, query));
            return single;
        }

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap =
                FullScan.parseIpString(query.getIpStringMap(), schema);
        List<PushPullCacheScanThread> threads = new ArrayList<>(cachedPagesByNode.size());
        for(List<byte[]> nodePages : cachedPagesByNode){
            PushPullCacheScanThread thread = new PushPullCacheScanThread(nodePages, ipMap, query, threadNum);
            thread.start();
            threads.add(thread);
        }

        List<ComputeNodeCache> caches = new ArrayList<>(threads.size());
        for(PushPullCacheScanThread thread : threads){
            try{
                thread.join();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while scanning push-pull compute cache", e);
            }
            if(thread.getError() != null){
                throw thread.getError();
            }
            caches.add(thread.getComputeCache());
        }
        return caches;
    }

    private ComputeNodeCache initializePushPullCache(ComputeNodeCache computeCache, QueryParse query) {
        computeCache.cache.sortByTimestamp();
        computeCache.pushPullQuery = query;
        computeCache.pushPullProcessedVarNames = new HashSet<>(8);
        computeCache.pushPullAllEvents = new ArrayList<>(512);
        computeCache.pushPullFilteredRecordsMap = new HashMap<>(8);
        return computeCache;
    }

    class PushPullCacheScanThread extends Thread {
        private final List<byte[]> pages;
        private final Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap;
        private final QueryParse query;
        private final int threadNum;
        private ComputeNodeCache computeCache;
        private RuntimeException error;

        PushPullCacheScanThread(List<byte[]> pages,
                                Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap,
                                QueryParse query,
                                int threadNum) {
            this.pages = pages;
            this.ipMap = ipMap;
            this.query = query;
            this.threadNum = threadNum;
        }

        public ComputeNodeCache getComputeCache() {
            return computeCache;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public void run() {
            try{
                computeCache = new ComputeNodeCache(scanPagesInParallel(pages, ipMap, null, threadNum));
                initializePushPullCache(computeCache, query);
            }catch (RuntimeException e){
                error = e;
            }
        }
    }

    public Map<String, List<byte[]>> pullEventsForPushPull(List<String> requestedVarNames) {
        ensurePushPullInitialized();
        Map<String, List<byte[]>> pulledEvents = new HashMap<>(requestedVarNames.size() << 1);
        for(String varName : requestedVarNames){
            List<byte[]> records = cache.getVarByteRecords(varName, 0);
            if(!records.isEmpty()){
                pulledEvents.put(varName, records);
            }
        }
        return pulledEvents;
    }

    public Map<String, List<byte[]>> matchFilterForPushPull(List<String> requestedVarNames,
                                                            Map<String, List<byte[]>> eventListMap) {
        ensurePushPullInitialized();
        if(eventListMap == null){
            eventListMap = Collections.emptyMap();
        }

        for(Map.Entry<String, List<byte[]>> entry : eventListMap.entrySet()){
            pushPullProcessedVarNames.add(entry.getKey());
            pushPullAllEvents = mergeSortedEvents(pushPullAllEvents, entry.getValue());
        }

        pushPullFilteredRecordsMap = new HashMap<>(requestedVarNames.size() << 1);
        if(pushPullProcessedVarNames.isEmpty()){
            for(String requestedVarName : requestedVarNames){
                pushPullFilteredRecordsMap.put(requestedVarName, new ArrayList<byte[]>(0));
            }
            return pushPullFilteredRecordsMap;
        }

        Set<String> dpNames = pushPullQuery.getDpMap().keySet();
        boolean openOptimize = pushPullProcessedVarNames.size() == 1;
        String headVarName = pushPullProcessedVarNames.iterator().next();
        for(String requestedVarName : requestedVarNames){
            String key = headVarName.compareTo(requestedVarName) < 0 ?
                    (headVarName + "-" + requestedVarName) : (requestedVarName + "-" + headVarName);
            if(openOptimize && !dpNames.contains(key)){
                int headOrTail = pushPullQuery.headTailMarker(headVarName);
                long leftOffset;
                long rightOffset;
                switch (headOrTail){
                    case 0:
                        leftOffset = 0;
                        rightOffset = pushPullQuery.getWindow();
                        break;
                    case 1:
                        leftOffset = -pushPullQuery.getWindow();
                        rightOffset = 0;
                        break;
                    default:
                        leftOffset = -pushPullQuery.getWindow();
                        rightOffset = pushPullQuery.getWindow();
                }

                ReplayIntervals ri = new ReplayIntervals(pushPullAllEvents.size());
                for(byte[] e : pushPullAllEvents){
                    long ts = schema.getTimestamp(e);
                    ri.insert(ts + leftOffset, ts + rightOffset);
                }
                ri.sortAndReconstruct();

                List<byte[]> records = cache.getVarByteRecords(requestedVarName, 0);
                List<byte[]> filteredRecords = new ArrayList<>(Math.max(1, records.size() >> 3));

                for(byte[] e : records){
                    long ts = schema.getTimestamp(e);
                    if(ri.contains(ts)){
                        filteredRecords.add(e);
                    }
                }
                pushPullFilteredRecordsMap.put(requestedVarName, filteredRecords);
                pushPullProcessedVarNames.add(requestedVarName);

                if(!schema.getTableName().equals("SYNTHETIC")){
                    cache.cleanByName(requestedVarName);
                }
            }
            else{
                Set<String> projectedVarNames = new HashSet<>(pushPullProcessedVarNames);
                projectedVarNames.add(requestedVarName);
                List<byte[]> requestRecords = cache.getVarByteRecords(requestedVarName, 0);
                List<byte[]> records = cache.mergeEvents(pushPullAllEvents, requestRecords);
                NFA nfa = new NFA();
                nfa.constructNFA(pushPullQuery, projectedVarNames);
                for(byte[] record : records){
                    nfa.consume(record, SelectionStrategy.SKIP_TILL_ANY_MATCH, schema);
                }
                List<byte[]> projectedResults = nfa.getProjectedRecords(requestedVarName);
                pushPullFilteredRecordsMap.put(requestedVarName, projectedResults);

                if(!schema.getTableName().equals("SYNTHETIC")){
                    cache.cleanByName(requestedVarName);
                }

                if(pushPullProcessedVarNames.size() == pushPullQuery.getVariableNames().size() - 1){
                    pushPullAllEvents = new ArrayList<>(0);
                    System.gc();
                }
            }
        }
        return pushPullFilteredRecordsMap;
    }

    private void ensurePushPullInitialized() {
        if(cache == null || pushPullQuery == null || pushPullProcessedVarNames == null){
            throw new IllegalStateException("Push-pull compute cache must be initialized by scanForPushPull first.");
        }
    }

    private List<byte[]> mergeSortedEvents(List<byte[]> allEvents, List<byte[]> curEvents) {
        if(curEvents == null || curEvents.isEmpty()){
            return allEvents == null ? new ArrayList<byte[]>(0) : allEvents;
        }
        if(allEvents == null || allEvents.isEmpty()){
            return curEvents;
        }

        int size1 = allEvents.size();
        int size2 = curEvents.size();
        List<byte[]> mergedList = new ArrayList<>(size1 + size2);

        int i = 0;
        int j = 0;
        while(i < size1 && j < size2){
            byte[] record1 = allEvents.get(i);
            long ts1 = schema.getTimestamp(record1);

            byte[] record2 = curEvents.get(j);
            long ts2 = schema.getTimestamp(record2);
            if(ts1 <= ts2){
                mergedList.add(record1);
                i++;
            }else {
                mergedList.add(record2);
                j++;
            }
        }

        while(i < size1){
            mergedList.add(allEvents.get(i));
            i++;
        }

        while(j < size2){
            mergedList.add(curEvents.get(j));
            j++;
        }
        return mergedList;
    }

    public long getCachedBytes() {
        return cachedBytes;
    }

    public Map<String, Integer> getCardinality() {
        return cache.getCardinality();
    }

    public Map<String, ReplayIntervals> getTwoTripsReplayIntervals(QueryParse query) {
        if(cache == null){
            throw new IllegalStateException("Two-trips compute cache must be initialized by scan first.");
        }

        long window = query.getWindow();
        List<String> varNames = query.getVariableNames();
        Map<String, ReplayIntervals> replayIntervalMap = new HashMap<>(varNames.size() << 1);
        for(String varName : varNames){
            ByteBuffer buffer = cache.generateReplayIntervals(varName, window, query.headTailMarker(varName));
            replayIntervalMap.put(varName, ReplayIntervals.deserialize(buffer));
        }
        return replayIntervalMap;
    }

    public Map<String, Set<TsAttrPair>> getTwoTripsTsAttrPairs(QueryParse query) {
        if(cache == null){
            throw new IllegalStateException("Two-trips compute cache must be initialized by scan first.");
        }

        Set<String> varAttrNameSet = query.getVarAttrSetFromDpList();
        Map<String, Set<TsAttrPair>> tsAttrPairMap = new HashMap<>(varAttrNameSet.size() << 1);
        for(String varAttrName : varAttrNameSet){
            String[] splits = varAttrName.split("-");
            tsAttrPairMap.put(varAttrName, cache.getTsAttrPairSet(splits[0], splits[1]));
        }
        return tsAttrPairMap;
    }

    public List<byte[]> getRecords(ReplayIntervals replayIntervals) {
        if(cache == null){
            throw new IllegalStateException("Two-trips compute cache must be initialized by scan first.");
        }
        return cache.getRecords(replayIntervals);
    }

    public ByteBuffer getInitialIntervals(String varName, long window, int headTailMarker) {
        ByteBuffer intervalBuffer = cache.generateReplayIntervals(varName, window, headTailMarker);
        hasFilteredVarNames.add(varName);
        return intervalBuffer;
    }

    public Map<String, ByteBuffer> getBF4EQJoin(String varName, long window, Map<String, List<String>> dpStrMap,
                                                Map<String, Integer> keyNumMap, ByteBuffer buff) {
        if(Args.isOptimizedSwf){
            optimizedSWF = OptimizedSWF.deserialize(buff.duplicate());
            for(int i = 0; i < hasFilteredVarNames.size() - 1; i++){
                cache.simpleFilter(hasFilteredVarNames.get(i), window, optimizedSWF);
            }
        }else{
            swf = SWF.deserialize(buff.duplicate());
            for(int i = 0; i < hasFilteredVarNames.size() - 1; i++){
                cache.simpleFilter(hasFilteredVarNames.get(i), window, swf);
            }
        }

        Map<String, ByteBuffer> bfMap = new HashMap<>(dpStrMap.size() << 1);
        for(String preVarName : dpStrMap.keySet()) {
            List<String> dpStrList = dpStrMap.get(preVarName);
            List<EqualDependentPredicate> dpList = new ArrayList<>(dpStrList.size());
            for (String dpStr : dpStrList) {
                dpList.add(new EqualDependentPredicate(dpStr));
            }
            bfMap.put(preVarName, cache.generateBloomFilter(preVarName, keyNumMap.get(preVarName), window, dpList));
        }
        return bfMap;
    }

    public FilteredResult windowFilter(String varName, long window, int headTailMarker, ByteBuffer buff) {
        ByteBuffer ans;
        if(Args.isOptimizedSwf){
            optimizedSWF = OptimizedSWF.deserialize(buff.duplicate());
            ans = cache.updatePointers(varName, window, headTailMarker, optimizedSWF);
        }else{
            swf = SWF.deserialize(buff.duplicate());
            ans = cache.updatePointers(varName, window, headTailMarker, swf);
        }

        hasFilteredVarNames.add(varName);
        return new FilteredResult(cache.getEventNum(varName), ans);
    }

    public FilteredResult eqJoinFilter(String varName, long window, int headTailMarker,
                                       Map<String, Boolean> previousOrNext,
                                       Map<String, List<String>> dpStrMap,
                                       Map<String, ByteBuffer> bfBufferMap) {
        int previousVarNum = previousOrNext.size();
        Map<String, BasicBF> bfMap = new HashMap<>(previousVarNum << 1);
        Map<String, List<EqualDependentPredicate>> dpMap = new HashMap<>(previousVarNum << 1);

        for(String previousVariableName : bfBufferMap.keySet()){
            ByteBuffer buffer = bfBufferMap.get(previousVariableName);
            BasicBF bf = WindowWiseBF.deserialize(buffer.duplicate());
            bfMap.put(previousVariableName, bf);

            List<EqualDependentPredicate> dps = new ArrayList<>();
            List<String> dpStrList = dpStrMap.get(previousVariableName);
            for(String dpStr : dpStrList){
                dps.add(new EqualDependentPredicate(dpStr));
            }
            dpMap.put(previousVariableName, dps);
        }

        ByteBuffer buffer;
        if(Args.isOptimizedSwf){
            buffer = cache.updatePointers(varName, window, headTailMarker, optimizedSWF, previousOrNext, bfMap, dpMap);
        }else{
            buffer = cache.updatePointers(varName, window, headTailMarker, swf, previousOrNext, bfMap, dpMap);
        }

        hasFilteredVarNames.add(varName);
        return new FilteredResult(cache.getEventNum(varName), buffer);
    }

    public List<byte[]> getAllFilteredEvents(long window, ByteBuffer updatedSWF) {
        if(Args.isOptimizedSwf){
            OptimizedSWF finalSWF = OptimizedSWF.deserialize(updatedSWF.duplicate());
            return cache.getRecords(window, finalSWF);
        }else{
            SWF finalSWF = SWF.deserialize(updatedSWF.duplicate());
            return cache.getRecords(window, finalSWF);
        }
    }
}
