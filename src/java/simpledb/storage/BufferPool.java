package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    class PageLock {
        public static final int SHARE = 0;
        public static final int EXCLUSIVE = 1;
        private int type;
        private TransactionId tid;

        public PageLock(int type, TransactionId tid) {
            this.type = type;
            this.tid = tid;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }
    class LockManager {
        private Map<PageId,Map<TransactionId,PageLock>> lockMap = new ConcurrentHashMap<>();
        public synchronized boolean acquiredLock(PageId pageId, TransactionId tid, int requiredType) {
            if(!lockMap.containsKey(pageId)) {
                PageLock pageLock = new PageLock(requiredType,tid);
                Map<TransactionId,PageLock> map = new ConcurrentHashMap<>();
                map.put(tid,pageLock);
                lockMap.put(pageId,map);
                return true;
            } else if(lockMap.get(pageId).isEmpty()) {
                Map<TransactionId,PageLock> map = lockMap.get(pageId);
                PageLock pageLock = new PageLock(requiredType,tid);
                map.put(tid,pageLock);
                return true;
            }
            else if(lockMap.get(pageId).size()>1) {
                if(requiredType==PageLock.SHARE) {
                    PageLock pageLock = new PageLock(requiredType,tid);
                    if(!lockMap.containsKey(tid)) {
                        Map<TransactionId,PageLock> map = lockMap.get(pageId);
                        map.put(tid,pageLock);
                    }

                    return true;
                }else return false;
            }
            else {
                PageLock curLock = null;
                TransactionId curTid = null;
                for (Map.Entry<TransactionId, PageLock> lockEntry : lockMap.get(pageId).entrySet()) {
                    curLock = lockEntry.getValue();
                    curTid = lockEntry.getKey();
                }
                if(tid.equals(curTid)) {
                    if(curLock.type == PageLock.SHARE && requiredType == PageLock.EXCLUSIVE) {
                        curLock.setType(PageLock.EXCLUSIVE);
                    }
                    return true;
                } else if(curLock.type == PageLock.SHARE && requiredType == PageLock.SHARE) {
                    PageLock pageLock = new PageLock(requiredType,tid);
                    Map<TransactionId,PageLock> map = lockMap.get(pageId);
                    map.put(tid,pageLock);
                    return true;
                }

            }
            return false;
        }
        public synchronized boolean isHoldLock(TransactionId tid,PageId pageId) {
            Map<TransactionId, PageLock> pageLockMap = lockMap.get(pageId);
            if(pageLockMap == null) return false;
            if(pageLockMap.get(tid) == null) return false;
            return true;
        }
        public synchronized boolean releaseLock(TransactionId tid,PageId pageId) {
            if(isHoldLock(tid,pageId)) {
                Map<TransactionId, PageLock> pageLockMap = lockMap.get(pageId);
                pageLockMap.remove(tid);
                if(lockMap.size() == 0)lockMap.remove(pageId);
                return true;
            }
            return false;
        }
        public synchronized void completeTranslation(TransactionId tid) {
            for (PageId pageId : lockMap.keySet()) {
                releaseLock(tid,pageId);
            }
        }

    }
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    Map<PageId,LinkNode> pageMap;
    LockManager lockManager;
    int numPages;
    class LinkNode {
        LinkNode prev;
        LinkNode next;
        Page page;
        PageId pageId;

        public LinkNode(Page page, PageId pageId) {
            this.page = page;
            this.pageId = pageId;
        }

        public LinkNode(LinkNode prev, LinkNode next, Page page, PageId pageId) {
            this.prev = prev;
            this.next = next;
            this.page = page;
            this.pageId = pageId;
        }
    }
    LinkNode head;
    LinkNode tail;
    private void moveNodeToFirst(LinkNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }
    private void addNodeToFirst(LinkNode node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;

    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageMap = new ConcurrentHashMap<PageId, LinkNode>();
        head = new LinkNode(null,null);
        tail = new LinkNode(null,null);
        head.next = tail;
        tail.prev = head;
        lockManager = new LockManager();

    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */

    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if(pageMap.containsKey(pid)){
            LinkNode node = pageMap.get(pid);
            moveNodeToFirst(node);
            boolean isAcquired = false;
            long startTime = System.currentTimeMillis();
            while(!isAcquired) {
                long now = System.currentTimeMillis();
                if(now-startTime>500) throw new TransactionAbortedException();
                if(lockManager.acquiredLock(node.page.getId(), tid, perm == Permissions.READ_ONLY ? PageLock.SHARE : PageLock.EXCLUSIVE)){
                    isAcquired = true;
                }
            }

            return node.page;

        }
        else {
            if(pageMap.size() == numPages) evictPage();
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            LinkNode node = new LinkNode(page,pid);
            addNodeToFirst(node);
            pageMap.put(pid,node);
            boolean isAcquired = false;
            long startTime = System.currentTimeMillis();
            while(!isAcquired) {
                long now = System.currentTimeMillis();
                if(now-startTime>500) throw new TransactionAbortedException();
                if(lockManager.acquiredLock(node.page.getId(), tid, perm == Permissions.READ_ONLY ? PageLock.SHARE : PageLock.EXCLUSIVE)){
                    isAcquired = true;
                }
            }
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit) {
            try {
                for (LinkNode node : pageMap.values()) {
                    Page page = node.page;
                    if(Objects.equals(page.isDirty(),tid)) {
                        flushPage(page.getId());
                        // use current page contents as the before-image
// for the next transaction that modifies this page.
                        page.setBeforeImage();

                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            for (LinkNode node : pageMap.values()) {
                Page page = node.page;
                if(Objects.equals(page.isDirty(),tid)) page.setBeforeImage();
            }
            lockManager.completeTranslation(tid);
        } else {
            try {
                readPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            lockManager.completeTranslation(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pageList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pageList) {
            page.markDirty(true,tid);
            if(pageMap.containsKey(page.getId())) {
                LinkNode node = pageMap.get(page.getId());
                node.page = page;
                moveNodeToFirst(node);
            }else {
                LinkNode node = new LinkNode(page, page.getId());
                addNodeToFirst(node);
                pageMap.put(page.getId(),node);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        List<Page> pageList = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for (Page page : pageList) {
            page.markDirty(true,tid);
            if(pageMap.containsKey(page.getId())) {
                LinkNode node = pageMap.get(page.getId());
                node.page = page;
                moveNodeToFirst(node);
            }else {
                LinkNode node = new LinkNode(page, page.getId());
                addNodeToFirst(node);
                pageMap.put(page.getId(),node);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (LinkNode node : pageMap.values()) {
            Page page = node.page;
            if(page.isDirty()!=null) flushPage(page.getId());
        }


    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageMap.remove(pid);

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(!pageMap.containsKey(pid)) return;
        int tableId = pid.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        Page p = pageMap.get(pid).page;
        // append an update record to the log, with
        // a before-image and after-image.
        TransactionId dirtier = p.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
        }
        dbFile.writePage(p);
        pageMap.get(pid).page.markDirty(false,null);

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (LinkNode node : pageMap.values()) {
            Page page = node.page;
            if(Objects.equals(page.isDirty(),tid)) flushPage(page.getId());
        }
    }
    public synchronized  void readPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (LinkNode node : pageMap.values()) {
            Page page = node.page;
            if(Objects.equals(page.isDirty(),tid)) {

                int tableId = page.getId().getTableId();
                Page oldPage = Database.getCatalog().getDatabaseFile(tableId).readPage(page.getId());
                node.page = oldPage;
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        LinkNode evictNode = tail.prev;
        while (evictNode.page.isDirty()!=null) {
            evictNode = evictNode.prev;
            if(evictNode == head)throw new DbException("EvictPageError: All pages are dirty");
        }
        evictNode.prev.next = evictNode.next;
        evictNode.next.prev = evictNode.prev;
        pageMap.remove(evictNode.pageId);

    }

}
