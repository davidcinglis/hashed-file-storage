package edu.caltech.nanodb.storage.linhash;

import java.io.EOFException;
import java.io.IOException;

import java.lang.Override;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.ColumnStatsCollector;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFileManager;

public class LinHashTupleFile implements TupleFile
{

    // private static Logger logger = Logger.getLogger(LinHashTupleFile.class);

    private StorageManager storageManager;

    private LinHashTupleFileManager linHashFileManager;

    private TableSchema schema;

    private TableStats stats;

    private DBFile dbFile;

    public LinHashTupleFile(StorageManager storageManager,
                              LinHashTupleFileManager linHashFileManager,
                              DBFile dbFile,
                              TableSchema schema,
                              TableStats stats) {
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

        this.storageManager = storageManager;
        this.linHashFileManager = linHashFileManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
    }

    @Override
    public LinHashTupleFileManager getManager() { return linHashFileManager; }

    @Override
    public TableSchema getSchema() { return schema; }

    @Override
    public TableStats getStats() { return stats; }

    public DBFile getDBFile() { return dbFile; }


    @Override
    public Tuple getFirstTuple() throws IOException {
        /* Identical to HeapTupleFile, find the first
         * data page with something on it and return
         * the first tuple on that page.
         */

        // placeholder
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    @Override
    public Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        /* This should be identical to the HeapTupleFile
         * implementation.
         */


        // placeholder
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    @Override
    public Tuple getNextTuple(Tuple tup) throws IOException {
        /* I want a function that takes a tuple as input
         * and uses the hashing functions to calculate
         * the page number and slot of that tuple.
         * Here you would just call that function and
         * return the next tuple after that location.
         */

        // placeholder
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    @Override
    public Tuple addTuple(Tuple tup) throws IOException {
        /**
         * Again we would call the indexing function here
         * that hashes the tuple based on the search key and
         * returns the resulting page number and slot.
         * Then we simply add the tuple at that location.
         */

        // placeholder
        throw new UnsupportedOperationException("Not yet implemented!");

    }

    @Override
    public void updateTuple(Tuple tup, Map<String, Object> newValues)
        throws IOException {
        /**
         * I think you would just delete the tuple and call
         * addTuple on the new set of values, since updating
         * the tuple could change the search key. Maybe you would
         * check to see if the set of newValues changes the primary
         * key.
         */

        // placeholder
        throw new UnsupportedOperationException("Not yet implemented!");

    }

    @Override
    public void deleteTuple(Tuple tup) throws IOException {
        /**
         * Identical to HeapTupleFile implementation.
         */

        // placeholder
        throw new UnsupportedOperationException("Not yet implemented!");

    }

    @Override
    public void analyze() throws IOException {
        /**
         * Placeholder.
         */

        // placeholder
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
        // TODO!
        throw new UnsupportedOperationException("Not yet implemented!");
    }

}