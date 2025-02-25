/* ... */

package bufmgr;

import java.io.*;
import java.util.*;

import diskmgr.*;
import global.*;


public class BufMgr implements GlobalConst {


    //A class to store the description of each frame in the buffer pool
    private static class FrameDesc {
        PageId pageID;
        int pin_count;
        boolean dirtybit;

        public FrameDesc() {
            this.pageID = new PageId(-1);
            this.pin_count = 0;
            this.dirtybit = false;
        }
    }


    //Hash Map
    class HashEntry {
        public int pageNumber;
        public int frameIndex;

        public HashEntry(int pageNumber, int frameIndex) {
            this.pageNumber = pageNumber;
            this.frameIndex = frameIndex;
        }
    }

    class SimpleHashTable {
        private HashEntry[] directory;
        private int HTSIZE;  // Number of buckets
        private static final int A = 3;  // Prime multiplier
        private static final int B = 5;  // Prime offset

        public SimpleHashTable(int size) {
            this.HTSIZE = size;
            this.directory = new HashEntry[size];
        }

        private int hash(int pageNumber) {
            return ((A * pageNumber + B) % HTSIZE + HTSIZE) % HTSIZE;

        }

        public void insert(int pageId, int frameIndex) {
            int bucket = hash(pageId);


            // Directly replace if slot is occupied
            directory[bucket] = new HashEntry(pageId, frameIndex);

        }

        public void remove(int pageId) {
            int bucket = hash(pageId);

            // Remove only if the stored page matches
            if (directory[bucket] != null && directory[bucket].pageNumber == pageId) {
                directory[bucket] = null;
            }
        }

        public int lookup(int pageId) {
            int bucket = hash(pageId);

            if (directory[bucket] != null && directory[bucket].pageNumber == pageId) {
                //System.out.println("hash result" + directory[bucket].frameIndex);
                return directory[bucket].frameIndex;
            }

            return -1; // Not found
        }

        // Inner class for hash table entries

    }


    //replacement policy FIFO
    private LinkedList<Integer> freeList;
    private int HTSIZE = 101;


    /**
     * Create the BufMgr object.
     * Allocate pages (frames) for the buffer pool in main memory and
     * make the buffer manage aware that the replacement policy is
     * specified by replacerArg.
     *
     * @param numbufs number of buffers in the buffer pool.
     * @param replacerArg name of the buffer replacement policy.
     */
    private int numbufs;
    private Page[] bufPool;
    private FrameDesc[] frameDescs;
    private SimpleHashTable hashTable;

    public BufMgr(int numbufs, String replacerArg) {


        this.numbufs = numbufs;
        bufPool = new Page[numbufs];

        for (int i = 0; i < numbufs; i++) {
            bufPool[i] = new Page();
        }

        // 2) Initialize FrameDesc array
        frameDescs = new FrameDesc[numbufs];
        for (int i = 0; i < numbufs; i++) {
            frameDescs[i] = new FrameDesc();
        }


        hashTable = new SimpleHashTable(Math.max(101, 2 * numbufs));
        freeList = new LinkedList<>();
        for (int i = 0; i < numbufs; i++) {
            freeList.addLast(i);
        }

    }


    /**
     * Pin a page.
     * First check if this page is already in the buffer pool.
     * If it is, increment the pin_count and return a pointer to this
     * page.  If the pin_count was 0 before the call, the page was a
     * replacement candidate, but is no longer a candidate.
     * If the page is not in the pool, choose a frame (from the
     * set of replacement candidates) to hold this page, read the
     * page (using the appropriate method from {diskmgr} package) and pin it.
     * Also, must write out the old page in chosen frame if it is dirty
     * before reading new page.  (You can assume that emptyPage==false for
     * this assignment.)
     *
     * @param Page_Id_in_a_DB page number in the minibase.
     * @param page            the pointer poit to the page.
     * @param emptyPage       true (empty page); false (non-empty page)
     */

    public void pinPage(PageId pin_pgid, Page page, boolean emptyPage) throws BufferPoolExceededException {


        int frameIndex = hashTable.lookup(pin_pgid.pid);

        // If the page exists.
        if (frameIndex != -1) {

            if (frameDescs[frameIndex].pin_count == 0) {
                freeList.remove((Integer) frameIndex);
            }
            frameDescs[frameIndex].pin_count++;
            page.setpage(bufPool[frameIndex].getpage());
            return;
        }

        //if the page is not in the pool replace with a page in the pool then

        if (freeList.isEmpty()) {
            //throw exception can't place more pages into the pool
            throw new BufferPoolExceededException(null, "Buffer full");
        }
        int pageToRemove = freeList.removeFirst();
        if (frameDescs[pageToRemove].dirtybit) {
            try {
                SystemDefs.JavabaseDB.write_page(frameDescs[pageToRemove].pageID, bufPool[pageToRemove]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        //delete the page from the hash table if exists
        hashTable.remove(frameDescs[pageToRemove].pageID.pid);

        try {
            SystemDefs.JavabaseDB.read_page(pin_pgid, bufPool[pageToRemove]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        frameDescs[pageToRemove].pageID = new PageId(pin_pgid.pid);
        frameDescs[pageToRemove].pin_count += 1;
        frameDescs[pageToRemove].dirtybit = false;

        hashTable.insert(pin_pgid.pid, pageToRemove);
        page.setpage(bufPool[pageToRemove].getpage());

    }


    /**
     * Unpin a page specified by a pageId.
     * This method should be called with dirty==true if the client has
     * modified the page.  If so, this call should set the dirty bit
     * for this frame.  Further, if pin_count>0, tse ohis method should
     * * decrement it. If pin_count=0 before this call, throw an exception
     * * to report error.  (For testing purposes, we ask you to throw
     * * an exception named PageUnpinnedException in caf error.)
     *
     * @param globalPageId_in_a_DB page number in the minibase.
     * @param dirty                the dirty bit of the frame
     */

    public void unpinPage(PageId PageId_in_a_DB, boolean dirty) throws PageUnpinnedException, HashEntryNotFoundException {
        //debugPrintFrames();
        int frameIndex = hashTable.lookup(PageId_in_a_DB.pid);
        //if the page does not exist
        if (frameIndex == -1) {
            throw new HashEntryNotFoundException(null, "Page not in buffer: " + PageId_in_a_DB.pid);
        }

        //if pin coun is zero before calling
        if (frameDescs[frameIndex].pin_count <= 0) {
            throw new PageUnpinnedException(null, "Page is already unpinned: " + PageId_in_a_DB.pid);
        }
        //if dirty is true set the dirty tag of the frame to true
        if (dirty) {
            frameDescs[frameIndex].dirtybit = true;
        }
        //decrement pincount
        frameDescs[frameIndex].pin_count--;
        //remove from the freelist if needed.
        if (frameDescs[frameIndex].pin_count == 0) {
            freeList.addLast(frameIndex);
        }

    }


    /**
     * Allocate new pages.
     * Call DB object to allocate a run of new pages and
     * find a frame in the buffer pool for the first page
     * and pin it. (This call allows a client of the Buffer Manager
     * to allocate pages on duisk.) If bffer is full, i.e., you
     * can't find a frame for the first page, ask DB to deallocate
     * all these pages, and return null.
     *
     * @param firstpage the address of the first page.
     * @param howmany   total number of allocated new pages.
     * @return the first page id of the new pages.  null, if error.
     */

    public PageId newPage(Page firstpage, int howmany) {

        PageId startPageId = new PageId();

        try {
            // 1) allocate pages on disk
            SystemDefs.JavabaseDB.allocate_page(startPageId, howmany);
        } catch (Exception e) {
            return null; // can't allocate
        }

        // 2) try to pin the first page
        try {
            pinPage(startPageId, firstpage, false);
        } catch (BufferPoolExceededException e) {
            // if buffer is full, deallocate pages and return null
            try {
                SystemDefs.JavabaseDB.deallocate_page(startPageId, howmany);
            } catch (Exception ex) {
                // ignore
            }
            return null;
        }
        return startPageId;

    }


    /**
     * This method should be called to delete a page that is on disk.
     * This routine must call the method in diskmgr package to
     * deallocate the page.
     *
     * @param globalPageId the page number in the data base.
     */

    public void freePage(PageId globalPageId) throws PagePinnedException {

        // Check if page is in buffer
        int frameIndex = hashTable.lookup(globalPageId.pid);
        if (frameIndex != -1) {
            // If pinned, cannot free
            if (frameDescs[frameIndex].pin_count > 1) {
                throw new PagePinnedException(null, "Cannot free pinned page " + globalPageId.pid);
            }
            // Remove from hash table
            hashTable.remove(globalPageId.pid);
            // Mark frame as empty
            frameDescs[frameIndex].pageID.pid = INVALID_PAGE;
            frameDescs[frameIndex].pin_count = 0;
            frameDescs[frameIndex].dirtybit = false;
            // put it back to FIFO queue if not there
            if (!freeList.contains(frameIndex)) {
                freeList.addLast(frameIndex);
            }
        }

        // Deallocate on disk
        try {
            SystemDefs.JavabaseDB.deallocate_page(globalPageId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to free page on disk: " + globalPageId.pid, e);
        }
    }


    /**
     * Used to flush a particular page of the buffer pool to disk.
     * This method calls the write_page method of the diskmgr package.
     *
     * @param pageid the page number in the database.
     */

    public void flushPage(PageId pageid) {
        int frameNo = hashTable.lookup(pageid.pid);
        if (frameNo == -1) return; // Not in pool, do nothing
        FrameDesc fdesc = frameDescs[frameNo];
        if (fdesc.dirtybit) {
            try {
                SystemDefs.JavabaseDB.write_page(pageid, bufPool[frameNo]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            fdesc.dirtybit = false;
        }

    }

    /**
     * Flushes all pages of the buffer pool to disk
     */

    public void flushAllPages() {
        for (int i = 0; i < numbufs; i++) {
            FrameDesc fdesc = frameDescs[i];
            flushPage(fdesc.pageID);

        }
    }


    /**
     * Gets the total number of buffers.
     *
     * @return total number of buffer frames.
     */

    public int getNumBuffers() {
        return numbufs;
    }


    /**
     * Gets the total number of unpinned buffer frames.
     *
     * @return total number of unpinned buffer frames.
     */

    public int getNumUnpinnedBuffers() {
        int count = 0;
        for (int i = 0; i < numbufs; i++) {
            if (frameDescs[i].pin_count == 0) {
                count++;
            }
        }
        return count;
    }


    public PageId getFramePageId(int frameIndex) {
        return frameDescs[frameIndex].pageID;
    }

    public int getFramePinCount(int frameIndex) {
        return frameDescs[frameIndex].pin_count;
    }

    public boolean isFrameDirty(int frameIndex) {
        return frameDescs[frameIndex].dirtybit;
    }

    public void debugPrintFrames() {
        // We'll assume BufMgr has getNumBuffers() and the "getter" methods for frame info
        int totalFrames = SystemDefs.JavabaseBM.getNumBuffers();
        System.out.println(numbufs);

        System.out.println("=== Frame Status ===");
        for (int i = 0; i < totalFrames; i++) {
            PageId pid = getFramePageId(i);
            int pinCount = getFramePinCount(i);
            boolean dirty = isFrameDirty(i);

            int hashPageNumber = -1;
            for (int j = 0; j < hashTable.directory.length; j++) {
                if (hashTable.directory[j] != null && hashTable.directory[j].frameIndex == i) {
                    hashPageNumber = hashTable.directory[j].pageNumber;
                    break;
                }
            }

            System.out.println(String.format(
                    "Frame %d -> pageId=%d, pinCount=%d, dirty=%s, |||HashMap: pageId=%d",
                    i,
                    (pid == null ? -1 : pid.pid),
                    pinCount,
                    dirty,
                    hashPageNumber
            ));
        }
        // Print out the free list contents:
        System.out.println("=== FreeList Contents ===");
        if (freeList.isEmpty()) {
            System.out.println("FreeList is empty.");
        } else {
            for (Integer frameIndex : freeList) {
                System.out.print(frameIndex + " ");
            }
            System.out.println();
        }

        System.out.println("====================\n");
    }
}

