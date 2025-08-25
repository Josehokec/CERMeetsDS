package store;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Event Storage Class
 * Storing Byte Type Records
 * filename=schemaName.store
 * Caching function not implemented
 */
public class EventStore {
    public static int pageSize = 4 * 1024;          // page size is set to 4KB
    private int page;                               // Current buffered pages
    private short offset;                           // offset
    private final File file;                        // file
    private final ByteBuffer buf;                   // byte buffer for writing record
    private int cachePage;                          // cached page id
    private MappedByteBuffer readMappedBuffer;      // mapped buffer for reading, we only cache a page

    public EventStore(String tableName, boolean deletionFlag){
        String storePath = System.getProperty("user.dir") + File.separator + "event_store";
        File directory = new File(storePath);

        if (!directory.exists()) {
            boolean isCreated = directory.mkdirs();
            if (isCreated) {
                System.out.println("The directory " + storePath + " does not exist, the directory has been successfully created.");
            } else {
                throw new RuntimeException("Create the directory " + storePath + " failed.");
            }
        }
        String filename = tableName.toUpperCase() + ".store";
        String filePath = storePath + File.separator + filename;
        // System.out.println("store filePath: " + filePath);
        file = new File(filePath);

        if(deletionFlag && file.exists()){
            System.out.println("file: '"+ filename + "' exists in disk, we will delete this file, flag: " + file.delete());
        }

        buf = ByteBuffer.allocate(pageSize);
        cachePage = -1;
        page = 0;
        offset = 0;
        readMappedBuffer = null;
    }

    @SuppressWarnings("unused")
    public int getPage() {
        return page;
    }

    public File getFile(){
        return file;
    }

    public long getFileSize(){
        return file.length();
    }

    /**
     * this function aims to support indexes
     * please ensure file is cleared
     * @param record        single record
     * @param recordSize    record size
     * @return              storage position
     */
    public final RID insertSingleRecord(byte[] record, int recordSize){
        // here we will support insert new records
        RID rid;
        // If the cache can no longer hold data, it will be flushed to a file
        if(offset + recordSize > pageSize){
            // Lock the content and then flush the data into the file
            buf.flip();
            try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
                // note that this page may not be full, but it doesn't matter
                byte[] array = buf.array();
                out.write(array);
                out.flush();
            }catch (Exception e) {
                e.printStackTrace();
            }
            page++;
            offset = 0;
        }
        buf.put(record);
        rid = new RID(page, offset);
        offset += (short) recordSize;
        return rid;
    }

    /**
     * this function aims to append a batch of data
     * @param batchRecords      a batch record
     * @param singleRecordSize  fixed length for a record
     * @return                  success or fail
     */
    public final boolean appendBatchRecords(byte[] batchRecords, int singleRecordSize){
        long fileSize = getFileSize();
        int writeSize = batchRecords.length;

        // our code will ensure that either the file size is divisible by the page size,
        // or at least one record can be placed on the current page
        int lastPageRemaining = (int) (pageSize - fileSize % pageSize);
        if(lastPageRemaining < singleRecordSize){
            throw new RuntimeException("forget padding exception");
        }

        try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
            if(lastPageRemaining >= writeSize){
                // we can put all bytes
                if(lastPageRemaining < writeSize + singleRecordSize){
                    // we need padding zero
                    byte[] paddingContent = new byte[lastPageRemaining];
                    System.arraycopy(batchRecords, 0, paddingContent, 0, writeSize);
                    out.write(paddingContent);
                }else{
                    // write all data
                    out.write(batchRecords);
                }
            }else{
                // please note that we only process the data with a fixed length
                int pagMaxWrittenSize = pageSize / singleRecordSize * singleRecordSize;
                // we need to write full current page
                int firstLen = lastPageRemaining / singleRecordSize * singleRecordSize;
                int srcPos = 0;
                byte[] paddingContent = new byte[lastPageRemaining];
                System.arraycopy(batchRecords, 0, paddingContent, 0, firstLen);
                out.write(paddingContent);
                srcPos += firstLen;
                writeSize -= firstLen;

                while(writeSize >= pageSize){
                    byte[] writtenBytes = new byte[pageSize];
                    System.arraycopy(batchRecords, srcPos, writtenBytes, 0, pagMaxWrittenSize);
                    out.write(writtenBytes);

                    srcPos += pagMaxWrittenSize;
                    writeSize -= pagMaxWrittenSize;
                }

                if(writeSize + singleRecordSize > pageSize){
                    // Please note that we currently do not support recording cross page content
                    // thus, we need to perform a padding operation
                    byte[] writtenBytes = new byte[pageSize];
                    System.arraycopy(batchRecords, srcPos, writtenBytes, 0, writeSize);
                    out.write(writtenBytes);
                }else{
                    byte[] writtenBytes = new byte[writeSize];
                    System.arraycopy(batchRecords, srcPos, writtenBytes, 0, writeSize);
                    out.write(writtenBytes);
                }
            }
            out.flush();
        }catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    public void forceFlush(){
        buf.flip();
        int len = buf.limit();
        try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
            byte[] array = buf.array();
            byte[] content = new byte[len];
            System.arraycopy(array, 0, content, 0, len);
            out.write(content);
            out.flush();
            // clear the buffer
            buf.clear();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    // this function is too slow
    @Deprecated
    public final MappedByteBuffer getMappedBuffer(int queryPage, long fileSize){
        if(cachePage != queryPage){
            RandomAccessFile raf;
            try{
                raf = new RandomAccessFile(file, "rw");
                long startPos = (long) queryPage * pageSize;
                FileChannel fileChannel = raf.getChannel();
                int offset = (startPos + pageSize > fileSize) ? (int)(fileSize - startPos) : pageSize;
                readMappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startPos, offset);
                raf.close();
                cachePage = queryPage;
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        return readMappedBuffer;
    }
}

