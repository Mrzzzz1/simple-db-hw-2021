package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    File f;
    TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNumber = pid.getPageNumber();
        RandomAccessFile file = null;
        Page page = null;
        try {
            file = new RandomAccessFile(f,"r");
            if((pageNumber+1)*BufferPool.getPageSize()>f.length()) {
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pageNumber));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            file.seek(pageNumber*BufferPool.getPageSize());
            file.read(bytes,0,BufferPool.getPageSize());
            page = new HeapPage(new HeapPageId(tableId,pageNumber),bytes);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return page;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId id = page.getId();
        if(id.getPageNumber()>numPages()) throw new IllegalArgumentException();
        RandomAccessFile file = new RandomAccessFile(f,"rw");
        file.seek(id.getPageNumber()*BufferPool.getPageSize());
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     * 可以存储的总页数
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(f.length()*1.0/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> list = new ArrayList<>();
        for(int i=0;i<numPages();i++) {
            PageId pid = new HeapPageId(getId(),i);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(page.getNumEmptySlots()!=0) {
                page.insertTuple(t);
                page.markDirty(true,tid);
                list.add(page);
                return list;
            } else   {
                Database.getBufferPool().unsafeReleasePage(tid,pid);
            }

        }
        PageId pid = new HeapPageId(getId(),numPages());
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        RandomAccessFile file = new RandomAccessFile(f,"rw");
        file.seek(pid.getPageNumber()*BufferPool.getPageSize());
        file.write(emptyPageData);
        file.close();
//        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(f, true));
//        byte[] emptyPageData = HeapPage.createEmptyPageData();
//        outputStream.write(emptyPageData);
//        outputStream.close();
//        PageId pid = new HeapPageId(getId(),numPages()-1);
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        page.markDirty(true,tid);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        page.markDirty(true,tid);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }
    public static class HeapFileIterator implements DbFileIterator {
        HeapFile heapFile;
        int pageNum;
        int pageNo=0;
        TransactionId tid;
        Iterator<Tuple> tupleIterator=null;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
            pageNum = heapFile.numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            tupleIterator = getTupleIterator(pageNo++);

        }
        private Iterator<Tuple> getTupleIterator(int pageNo) throws TransactionAbortedException, DbException {

            if(pageNo<0||pageNo>=pageNum)return null;
            PageId pageId = new HeapPageId(heapFile.getId(),pageNo);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator==null)return false;
            if (tupleIterator.hasNext())return true;
            while(pageNo<pageNum) {
                open();
                if(tupleIterator == null) return false;
                if(tupleIterator.hasNext())return true;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator==null) throw new NoSuchElementException();
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageNo = 0;
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

}

