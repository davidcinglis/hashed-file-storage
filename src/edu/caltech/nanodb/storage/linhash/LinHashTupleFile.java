package edu.caltech.nanodb.storage.linhash;

import java.io.EOFException;
import java.io.IOException;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.UnsupportedOperationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.ColumnStatsCollector;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.expressions.TupleLiteral;


import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFileManager;
import edu.caltech.nanodb.storage.BucketPage;
import edu.caltech.nanodb.storage.HashedTupleFile;

import edu.caltech.nanodb.expressions.TupleHasher;

public class LinHashTupleFile implements HashedTupleFile
{

    private static Logger logger = Logger.getLogger(LinHashTupleFile.class);

    public static final int N_BUCKETS = 3;

    private StorageManager storageManager;

    private LinHashTupleFileManager linHashFileManager;

    private TableSchema schema;

    private TableStats stats;

    private DBFile dbFile;

    private List<Integer> hashColumns;

    private DBFile overflowFile;

    private boolean midSplit = false;

    public LinHashTupleFile(StorageManager storageManager,
                              LinHashTupleFileManager linHashFileManager,
                              DBFile dbFile,
                              TableSchema schema,
                              TableStats stats,
                              List<Integer> hashColumns,
                              DBFile overflowFile) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        if (linHashFileManager == null)
            throw new IllegalArgumentException("heapFileManager cannot be null");

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (stats == null)
            throw new IllegalArgumentException("stats cannot be null");

        if(hashColumns == null)
            throw new IllegalArgumentException("hashColumns cannot be null");

        if(overflowFile == null)
            throw new IllegalArgumentException("overflowFile cannot be null");

        this.storageManager = storageManager;
        this.linHashFileManager = linHashFileManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
        this.hashColumns = hashColumns;
        this.overflowFile = overflowFile;
    }

    @Override
    public LinHashTupleFileManager getManager() { return linHashFileManager; }

    @Override
    public TableSchema getSchema() { return schema; }

    @Override
    public TableStats getStats() { return stats; }

    public DBFile getDBFile() { return dbFile; }

    public List<Integer> getHashColumns() { return hashColumns; }


    public void initialize() throws IOException {

        // Create the initial bucket pages
        for(int i = 0; i < N_BUCKETS; i++)
        {
            DBPage newBucket = storageManager.loadDBPage(dbFile, i + 1, true);
            BucketPage.initNewPage(newBucket);
        }
    }

    @Override
    public Tuple getFirstTuple() throws IOException {
        try {
            // Scan through the data pages until we hit the end of the table
            // file.  It may be that the first run of data pages is empty,
            // so just keep looking until we hit the end of the file.

            // Header page is page 0, so first data page is page 1.

            for (int iPage = 1; /* nothing */ ; iPage++) {
                // Look for data on this page...

                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                int numSlots = BucketPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = BucketPage.getSlotValue(dbPage, iSlot);
                    if (offset == BucketPage.EMPTY_SLOT)
                        continue;

                    // This is the first tuple in the file.  Build up the
                    // HeapFilePageTuple object and return it.
                    return new HashFilePageTuple(schema, dbPage, iSlot, offset);
                }

                // If we got here, the page has no tuples.  Unpin the page.
                dbPage.unpin();
            }
        }
        catch (EOFException e) {
            // We ran out of pages.  No tuples in the file!
            logger.debug("No tuples in table-file " + dbFile +
                    ".  Returning null.");
        }

        return null;
    }

    @Override
    public Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        DBPage dbPage;
        try {
            // This could throw EOFException if the page doesn't actually exist.
            dbPage = storageManager.loadDBPage(dbFile, fptr.getPageNo());
        }
        catch (EOFException eofe) {
            throw new InvalidFilePointerException("Specified page " +
                    fptr.getPageNo() + " doesn't exist in file " +
                    dbFile.getDataFile().getName(), eofe);
        }

        // The file-pointer points to the slot for the tuple, not the tuple itself.
        // So, we need to look up that slot's value to get to the tuple data.

        int slot;
        try {
            slot = BucketPage.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        }
        catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // Pull the tuple's offset from the specified slot, and make sure
        // there is actually a tuple there!

        int offset = BucketPage.getSlotValue(dbPage, slot);
        if (offset == BucketPage.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot +
                    " on page " + fptr.getPageNo() + " is empty.");
        }

        return new HashFilePageTuple(schema, dbPage, slot, offset);
    }

    @Override
    public Tuple getNextTuple(Tuple tup) throws IOException {
        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HashFilePageTuple)) {
            throw new IllegalArgumentException(
                    "Tuple must be of type HashFilePageTuple; got " + tup.getClass());
        }
        HashFilePageTuple ptup = (HashFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        DBFile currFile = dbPage.getDBFile();

        int nextSlot = ptup.getSlot() + 1;
        while (true) {
            int numSlots = BucketPage.getNumSlots(dbPage);

            while (nextSlot < numSlots) {
                int nextOffset = BucketPage.getSlotValue(dbPage, nextSlot);
                if (nextOffset != BucketPage.EMPTY_SLOT) {
                    return new HashFilePageTuple(schema, dbPage, nextSlot,
                            nextOffset);
                }

                nextSlot++;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            try {
                DBPage nextDBPage =
                        storageManager.loadDBPage(currFile, dbPage.getPageNo() + 1);
                dbPage.unpin();
                dbPage = nextDBPage;

                nextSlot = 0;
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                break;
            }
        }
        /* Check the overflow file too if it is nonempty. */
        if (currFile.toString().equals(dbFile.toString()) && overflowFile.getNumPages() > 1)
        {
            currFile = overflowFile;
            dbPage = storageManager.loadDBPage(currFile, 1);
            while (true) {
                int numSlots = BucketPage.getNumSlots(dbPage);

                while (nextSlot < numSlots) {
                    int nextOffset = BucketPage.getSlotValue(dbPage, nextSlot);
                    if (nextOffset != BucketPage.EMPTY_SLOT) {
                        return new HashFilePageTuple(schema, dbPage, nextSlot,
                                nextOffset);
                    }

                    nextSlot++;
                }

                // If we got here then we reached the end of this page with no
                // tuples.  Go on to the next data-page, and start with the first
                // tuple in that page.

                try {
                    DBPage nextDBPage =
                            storageManager.loadDBPage(currFile, dbPage.getPageNo() + 1);
                    dbPage.unpin();
                    dbPage = nextDBPage;

                    nextSlot = 0;
                }
                catch (EOFException e) {
                    // Hit the end of the file with no more tuples.  We are done
                    // scanning.
                    return null;
                }
            }
        }
        else
            return null;

        // It's pretty gross to have no return statement here, but there's
        // no way to reach this point.
    }

    @Override
    public Tuple addTuple(Tuple tup) throws IOException {

        int tupSize = PageTuple.getTupleStorageSize(schema, tup);
        logger.debug("Adding new tuple of size " + tupSize + " bytes.");

        // Sanity check:  Make sure that the tuple would actually fit in a page
        // in the first place!
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize +
                    " is larger than page size " + dbFile.getPageSize() + ".");
        }

        // Hash the tuple to get its page number
        int pageNo = 1 + hashTuple(tup);
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        DBPage old = dbPage;

       /*
        * Keep iterating through the page and all its overflow pages until
        * space for the tuple is found. If all pages are full, create a new
        * one and link it to the previous overflow page.
        */
        while (true)
        {
            int freeSpace = BucketPage.getFreeSpaceInPage(dbPage);

            logger.trace(String.format("Page %d has %d bytes of free space.",
                    pageNo, freeSpace));

            // If this page has enough free space to add a new tuple, break
            // out of the loop.  (The "+ 2" is for the new slot entry we will
            // also need.)
            if (freeSpace >= tupSize + 2) {
                logger.debug("Found space for new tuple in page " + pageNo + ".");
                break;
            }

            // If we reached this point then the page doesn't have enough
            // space, so go on to the next data page.
            int nextPageNo = BucketPage.getNextBucket(dbPage);
            old = dbPage;

            // If the next page number is 0, we need to create a new overflow page
            if (nextPageNo == 0) {
                dbPage = null;
                break;
            }

            // Otherwise, we go to the next overflow page and loop back
            else {
                dbPage = storageManager.loadDBPage(overflowFile, nextPageNo);
            }

        }

        // If the dbPage is null, we must create a new overflow page.
        if (dbPage == null) {
            int numPages = overflowFile.getNumPages();
            dbPage = storageManager.loadDBPage(overflowFile, numPages, true);
            BucketPage.initNewPage(dbPage);
            BucketPage.setNextBucket(old, numPages);
        }

        // Finally, we add the tuple to the page
        int slot = BucketPage.allocNewTuple(dbPage, tupSize);
        int tupOffset = BucketPage.getSlotValue(dbPage, slot);
        HashFilePageTuple pageTup =
                HashFilePageTuple.storeNewTuple(schema, dbPage, slot, tupOffset, tup);

        storageManager.logDBPageWrite(dbPage);

        BucketPage.sanityCheck(dbPage);

        splitCheck();

        return pageTup;
    }

    @Override
    public void updateTuple(Tuple tup, Map<String, Object> newValues)
        throws IOException {

        throw new UnsupportedOperationException("Not yet implemented!");

    }

    @Override
    public void deleteTuple(Tuple tup) throws IOException {
        if (!(tup instanceof HashFilePageTuple)) {
            throw new IllegalArgumentException(
                    "Tuple must be of type HashFilePageTuple; got " + tup.getClass());
        }
        HashFilePageTuple ptup = (HashFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        BucketPage.deleteTuple(dbPage, ptup.getSlot());
        storageManager.logDBPageWrite(dbPage);

        BucketPage.sanityCheck(dbPage);

    }

    public Tuple findFirstTupleEquals(Tuple hashKey) throws IOException {

        int pageNo = hashTuple(hashKey);
        DBPage curr = storageManager.loadDBPage(dbFile, pageNo + 1);
        // Keep looping until we hit then end of the bucket
        while (true) {
            // Look for data on this page...

            int numSlots = BucketPage.getNumSlots(curr);
            for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                // Get the offset of the tuple in the page.  If it's 0 then
                // the slot is empty, and we skip to the next slot.
                int offset = BucketPage.getSlotValue(curr, iSlot);
                if (offset == BucketPage.EMPTY_SLOT)
                    continue;

                // This is the first tuple in the file.  Build up the
                // HashFilePageTuple object and return it.
                Tuple tup = new HashFilePageTuple(schema, curr, iSlot, offset);
                return new HashFilePageTuple(schema, curr, iSlot, offset);
            }


            // Move to the next bucket page
            pageNo = BucketPage.getNextBucket(curr);
            if(pageNo == 0)
                break;
            else
                curr = storageManager.loadDBPage(overflowFile, pageNo);
        }
        // No tuples with that key exist
        logger.debug("No tuples with hash in table-file " +
                dbFile + ".  Returning null.");
        return null;
    }


    public Tuple findNextTupleEquals(Tuple tup) throws IOException {
                /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HashFilePageTuple)) {
            throw new IllegalArgumentException(
                    "Tuple must be of type HashFilePageTuple; got " + tup.getClass());
        }
        HashFilePageTuple ptup = (HashFilePageTuple) tup;

        DBPage curr = ptup.getDBPage();
        DBPage test = storageManager.loadDBPage(dbFile, 1);

        int nextSlot = ptup.getSlot() + 1;
        int nextPage;
        while (true) {
            int numSlots = BucketPage.getNumSlots(curr);

            while (nextSlot < numSlots) {
                int nextOffset = BucketPage.getSlotValue(curr, nextSlot);

                if (nextOffset != BucketPage.EMPTY_SLOT) {
                    return new HashFilePageTuple(schema, curr, nextSlot,
                            nextOffset);
                }

                nextSlot++;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            nextPage = BucketPage.getNextBucket(curr);
            if (nextPage != 0) {
                curr = storageManager.loadDBPage(overflowFile, nextPage);
                nextSlot = 0;
            }
            else
                break;
        }

        // No more equivalent tuples exist
        return null;
    }

    /**
     * Returns the column(s) that comprise the hash key in this tuple file.
     *
     * @return the column(s) that comprise the hash key in this tuple file.
     */
    public List<Integer> getKeySpec() {
        return hashColumns;
    }

    @Override
    public void analyze() throws IOException {

        throw new UnsupportedOperationException("Not yet implemented!");
    }

    @Override
    public List<String> verify() throws IOException {
        // TODO!
        // Right now we will just report that everything is fine.
        return new ArrayList<String>();
    }


    @Override
    public void optimize() throws IOException {

        throw new UnsupportedOperationException("Not yet implemented!");
    }


    public int hashTuple(Tuple tup) throws IOException {
        // load the header page
        DBPage dbPage = storageManager.loadDBPage(dbFile, 0);
        int n = HeaderPage.getnBuckets(dbPage);
        int hash = Math.abs(TupleHasher.hashTuple(tup, hashColumns));
        int hash0 = hash % (n * (1 << HeaderPage.getLevel(dbPage)));

        // check if the bucket has already been split
        if (hash0 < HeaderPage.getNext(dbPage))
        {
            // if the bucket has been split, we hash on the next level
            hash %= (n * (1 << (1 + HeaderPage.getLevel(dbPage))));
            return hash;
        }

        // otherwise we return the value hashed on the initial level
        return hash0;
    }

    public void splitCheck() throws IOException {
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        int numPages = overflowFile.getNumPages();

        // Our "capacity" sets an upper bound of 1 overflow page per bucket
        // To calculate this we multiply the number of buckets/level by the current level
        int capacity = HeaderPage.getnBuckets(headerPage) * (1 << HeaderPage.getLevel(headerPage));

        if (numPages > capacity && !midSplit)
        {
            // a split is necessary
            splitBucket();
        }
    }

    public void splitBucket() throws IOException {
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        int n = HeaderPage.getnBuckets(headerPage);
        int level = HeaderPage.getLevel(headerPage);
        int next = HeaderPage.getNext(headerPage);

        // Calculate the number of the new bucket
        int newBucketNum = n * (1 << level) + next;

        // Create that bucket
        DBPage newBucket = storageManager.loadDBPage(dbFile, newBucketNum + 1, true);
        BucketPage.initNewPage(newBucket);

        /*
        System.out.println("Bucket actually has this number of pages: " + dbFile.getNumPages());
        System.out.println("Bucket should have this number of pages: " + newBucketNum);
        System.out.println("level is: " + HeaderPage.getLevel(headerPage));
        System.out.println("next is: " + HeaderPage.getNext(headerPage));

        System.out.println("val is: " + (n * (1 << level) - 1));
        */
        // Here, we need to increment the next variable, so that
        // when we re-add tuples they go into the new bucket
        if (next == n * (1 << level) - 1)
        {
            HeaderPage.setNext(headerPage, (short) 0);
            HeaderPage.incLevel(headerPage);
        }
        else
        {
            HeaderPage.setNext(headerPage, ((short) (next + 1)));
        }

        // Rehash all the tuples in the old bucket
        DBPage currPage = storageManager.loadDBPage(dbFile, next + 1);
        while (true)
        {
            int numSlots = BucketPage.getNumSlots(currPage);
            for (int i = 0; i < numSlots; i++)
            {
                int offset = BucketPage.getSlotValue(currPage, i);

                if (offset == BucketPage.EMPTY_SLOT)
                    continue;

                Tuple tup = new HashFilePageTuple(schema, currPage, i, offset);
                int hash = TupleHasher.hashTuple(tup, hashColumns);

                hash %= (1 << level + 1);

                if (hash >= n)
                {
                    // we need to move the tuple to the new bucket
                    Tuple add = new TupleLiteral(tup);
                    deleteTuple(tup);
                    midSplit = true;
                    addTuple(add);
                    midSplit = false;
                }
            }
            int nextBucket = BucketPage.getNextBucket(currPage);

            if (nextBucket == 0)
                break;
            else
                currPage = storageManager.loadDBPage(overflowFile, nextBucket);
        }
    }
}