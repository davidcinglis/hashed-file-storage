package edu.caltech.nanodb.storage.overflowfile;

import edu.caltech.nanodb.expressions.TupleHasher;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.*;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.storage.linhash.HashFilePageTuple;

/**
 * This class implements the TupleFile interface for overflow files.
 */
public class OverflowTupleFile implements HashedTupleFile{
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(OverflowTupleFile.class);

    /**
     * The storage manager to use for reading and writing file pages, pinning
     * and unpinning pages, write-ahead logging, and so forth.
     */
    private StorageManager storageManager;


    /**
     * The manager for hash tuple files provides some higher-level operations
     * such as saving the metadata of a hash tuple file, so it's useful to
     * have a reference to it.
     */
    private OverflowTupleFileManager hashFileManager;


    /** The schema of tuples in this tuple file. */
    private TableSchema schema;

    /** Statistics for this tuple file. */
    private TableStats stats;

    /** Column indices to hash */
    private List<Integer> hashedColumns;

    /** The file that stores the tuples. */
    private DBFile dbFile;

    public OverflowTupleFile(StorageManager storageManager,
                                   OverflowTupleFileManager hashFileManager, DBFile dbFile,
                                   TableSchema schema, TableStats stats,
                                   List<Integer> hashedColumns) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        if (hashFileManager == null)
            throw new IllegalArgumentException("hashFileManager cannot be null");

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (stats == null)
            throw new IllegalArgumentException("stats cannot be null");

        if (hashedColumns == null)
            throw new IllegalArgumentException("hashedColumns cannot be null");

        this.storageManager = storageManager;
        this.hashFileManager = hashFileManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
        this.hashedColumns = hashedColumns;
    }

    @Override
    public TupleFileManager getManager() {
        return hashFileManager;
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public TableStats getStats() {
        return stats;
    }

    public List<Integer> getHashedColumns() { return hashedColumns; }

    public DBFile getDBFile() {
        return dbFile;
    }

    /**
     * Initializes the hash file to have two buckets and initializes the
     * Bucket Address Table to indicate this.
     * @throws IOException if BAT_PAGE cannot be loaded or buckets can't be
     * created
     */
    public void initialize() throws IOException {
        // Empty for now, may need to add things though
    }

    //TupleFile functions
    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if there
     * are no tuples in the file. For extendable hashing, returns the first
     * tuple of the "first" bucket (with ordering of buckets specified by
     * their page numbers).
     *
     * @return the first tuple, or <tt>null</tt> if the table is empty
     *
     * @throws IOException if an IO error occurs while trying to read out the
     *         first tuple
     */
    public Tuple getFirstTuple() throws IOException {
        throw new UnsupportedOperationException("NYI");
    }


    /**
     * Returns the tuple that follows the specified tuple, or {@code null} if
     * there are no more tuples in the file. For extendable hashing, returns
     * the next tuple in the bucket of the input, or the first tuple of the
     * next bucket.
     *
     * @param tup the "previous" tuple in the table
     *
     * @return the tuple following the previous tuple, or {@code null} if the
     *         previous tuple is the last one in the table
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         next tuple.
     */
    public Tuple getNextTuple(Tuple tup) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by other features in the database, such as indexes.
     *
     * @param fptr a file-pointer specifying the tuple to retrieve
     *
     * @return the tuple referenced by <tt>fptr</tt>
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         specified tuple.
     */
    public Tuple getTuple(FilePointer fptr)
            throws InvalidFilePointerException, IOException{
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


    /**
     * Adds the specified tuple into the table file, returning a new object
     * corresponding to the actual tuple added to the table.
     *
     * @param tup a tuple object containing the values to add to the table
     *
     * @return a tuple object actually backed by this table
     *
     * @throws IOException if an IO error occurs while trying to add the new
     *         tuple to the table.
     */
    public Tuple addTuple(Tuple tup) throws IOException{

        throw new UnsupportedOperationException("NYI");
    }



    /**
     * Modifies the values in the specified tuple.
     *
     * @param tuple the tuple to modify in the table
     *
     * @param newValues a map containing the name/value pairs to use to update
     *        the tuple.  Values in this map will be coerced to the
     *        column-type of the specified columns.  Only the columns being
     *        modified need to be specified in this collection.
     *
     * @throws IOException if an IO error occurs while trying to modify the
     *         tuple's values.
     */
    public void updateTuple(Tuple tuple, Map<String, Object> newValues)
            throws IOException{
        // Update currently fails for heap files, so we're not touching this.
        throw new UnsupportedOperationException("NYI");
    }


    /**
     * Deletes the specified tuple from the table.
     *
     * @param tup the tuple to delete from the table
     *
     * @throws IOException if an IO error occurs while trying to delete the
     *         tuple.
     */
    public void deleteTuple(Tuple tup) throws IOException{

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


    /**
     * Analyzes the tuple data in the file, updating the file's statistics.
     *
     * @throws IOException if an IO error occurs while analyzing the file or
     *         updating the statistics.
     */
    public void analyze() throws IOException{
        throw new UnsupportedOperationException("NYI");
    }


    /**
     * Verifies the tuple file's internal storage format, identifying any
     * potential structural errors in the file.  Errors are returned as a list
     * of error-message strings, each one reporting some error that was found.
     *
     * @return a list of error-message strings describing issues identified
     *         during verification.  The list will be empty if the file has no
     *         identifiable errors.
     *
     * @throws IOException if an IO error occurs during verification.
     */
    public List<String> verify() throws IOException{
        throw new UnsupportedOperationException("NYI");
    }


    /**
     * Optimizes the tuple file's layout or other characteristics to ensure
     * optimal performance and space usage.  Tuple file formats that don't
     * provide any optimization capabilities can simply return when this is
     * called.
     *
     * @throws IOException if an IO error occurs during optimization.
     */
    public void optimize() throws IOException{
        throw new UnsupportedOperationException("NYI");
    }

    //HashedTupleFile functions

    /**
     * Returns the column(s) that comprise the hash key in this tuple file.
     *
     * @return the column(s) that comprise the hash key in this tuple file.
     */
    public List<Integer> getKeySpec() {
        return hashedColumns;
    }


    /**
     * Returns the first tuple in the file that has the same hash-key values,
     * or {@code null} if there are no tuples with this hash-key value in
     * the tuple file.
     *
     * @param hashKey the tuple to search for
     *
     * @return The first tuple in the file with the same hash-key values, or
     *         {@code null} if the file contains no files with the specified
     *         search key value.  This tuple will actually be backed by the
     *         tuple file, so typically it will be a subclass of
     *         {@link PageTuple}.
     *
     * @throws IOException if an IO error occurs during the operation
     */
    public Tuple findFirstTupleEquals(Tuple hashKey) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    public Tuple findFirstTupleEquals(int hashValue) throws IOException{
        throw new UnsupportedOperationException("NYI");
    }


    /**
     * Returns the next entry in the index that has the same hash-key value,
     * or {@code null} if there are no more entries with this hash-key value
     * in the tuple file.
     *
     * @param tup The tuple from which to resume the search for the next
     *        tuple with the same hash-key values.  This should be a tuple
     *        returned by a previous call to {@link #findFirstTupleEquals} or
     *        {@link #findNextTupleEquals}; using any other tuple would be an
     *        error.
     *
     * @return The next tuple in the file with the same hash-key values, or
     *         {@code null} if there are no more entries with this hash-key
     *         value in the file.
     *
     * @throws IOException if an IO error occurs during the operation
     */
    public Tuple findNextTupleEquals(Tuple tup) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }
}