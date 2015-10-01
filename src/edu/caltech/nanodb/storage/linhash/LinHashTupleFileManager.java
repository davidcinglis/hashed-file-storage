package edu.caltech.nanodb.storage.linhash;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.util.*;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.SchemaWriter;
import edu.caltech.nanodb.storage.StatsWriter;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFileManager;
import edu.caltech.nanodb.storage.HashTupleFileManager;
import edu.caltech.nanodb.storage.FileManagerImpl;

public class LinHashTupleFileManager implements HashTupleFileManager {
    private static Logger logger = Logger.getLogger(LinHashTupleFileManager.class);

    private StorageManager storageManager;

    public LinHashTupleFileManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }

    /** Because this class implements the HashTupleFileManager interface we must declare these methods
     * even though we don't use them. */
    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema)
            throws IOException{
        throw new IllegalArgumentException("Must specify hash key columns.");
    }

    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema, List<Integer> hashColumns)
        throws IOException{
        throw new IllegalArgumentException("Must specify overflow file.");
    }

    /**
     * Creates a linear hashing tuple file based on the input data.
     * @param dbFile The input file.
     * @param schema The input schema for the table.
     * @param hashColumns The list of column indices to hash on.
     * @param overflowFile THe overflow file.
     * @return The newly constructed LinHashTupleFile
     * @throws IOException
     */
    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema, List<Integer> hashColumns, DBFile overflowFile)
        throws IOException {

        logger.info(String.format(
                "Initializing new linhash tuple file %s with %d columns",
                dbFile, schema.numColumns()));

        TableStats stats = new TableStats(schema.numColumns());
        LinHashTupleFile tupleFile = new LinHashTupleFile(storageManager, this,
                dbFile, schema, stats, hashColumns, overflowFile);
        saveMetadata(tupleFile);

        // Initialization for main file.
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);

        // Initialize the next bucket to split to the first bucket.
        headerPage.writeShort(HeaderPage.getNextOffset(headerPage), 0);

        // Initialize the level to 0.
        headerPage.writeShort(HeaderPage.getLevelOffset(headerPage), 0);

        // Initialize the buckets in the tuple file
        tupleFile.initialize();

        /*
        //Initialization for overflow file.
        headerPage = storageManager.loadDBPage(overflowFile, 0);

        // Initialize the next bucket to split to the first bucket.
        headerPage.writeShort(HeaderPage.getNextOffset(headerPage), 1);

        // Initialize the level to 0.
        headerPage.writeShort(HeaderPage.getLevelOffset(headerPage), 0);
        */

        return tupleFile;
    }

    @Override
    public TupleFile openTupleFile(DBFile dbFile) throws IOException {
        logger.info("Opening existing linhash tuple file " + dbFile);

        // Table schema is stored into the header page, so get it and prepare
        // to write out the schema information.
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        PageReader hpReader = new PageReader(headerPage);
        // Skip past the page-size value.
        hpReader.setPosition(HeaderPage.OFFSET_SCHEMA_START);

        // Read in the schema details.
        SchemaWriter schemaWriter = new SchemaWriter();
        TableSchema schema = schemaWriter.readTableSchema(hpReader);

        // Read in hash column spec
        ArrayList<Integer> hashColumns = new ArrayList<Integer>();
        for (int i = 0; i < HeaderPage.getHashColumnsSize(headerPage); i+=2) {
            hashColumns.add((int) hpReader.readShort());
        }

        // Read in the statistics.
        StatsWriter statsWriter = new StatsWriter();
        TableStats stats = statsWriter.readTableStats(hpReader, schema);

        // Open the overflow file
        DBFile overflow = storageManager.openDBFile("ovflw_" + dbFile.toString());

        return new LinHashTupleFile(storageManager, this, dbFile, schema, stats, hashColumns, overflow);
    }

    @Override
    public void saveMetadata(TupleFile tupleFile) throws IOException {

        if (tupleFile == null)
            throw new IllegalArgumentException("tupleFile cannot be null");

        // Curiously, we never cast the tupleFile reference to HeapTupleFile,
        // but still, it would be very awkward if we tried to update the
        // metadata of some different kind of tuple file...
        if (!(tupleFile instanceof LinHashTupleFile)) {
            throw new IllegalArgumentException(
                    "tupleFile must be an instance of LinHashTupleFile");
        }

        LinHashTupleFile lhTupleFile = (LinHashTupleFile) tupleFile;

        DBFile dbFile = tupleFile.getDBFile();

        TableSchema schema = tupleFile.getSchema();
        TableStats stats = tupleFile.getStats();
        List<Integer> hashColumns = lhTupleFile.getHashColumns();


        // Grab header page for initialization
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);

        // Initialize level and next values to 0
        headerPage.writeShort(HeaderPage.getLevelOffset(headerPage), 0);
        headerPage.writeShort(HeaderPage.getNextOffset(headerPage), 0);


        // Table schema is stored into the header page, so get it and prepare
        // to write out the schema information.
        PageWriter hpWriter = new PageWriter(headerPage);
        // Skip past the page-size value.
        hpWriter.setPosition(HeaderPage.OFFSET_SCHEMA_START);

        // Write out the schema details now.
        SchemaWriter schemaWriter = new SchemaWriter();
        schemaWriter.writeTableSchema(schema, hpWriter);

        // Compute and store the schema's size.
        int schemaEndPos = hpWriter.getPosition();
        int schemaSize = schemaEndPos - HeaderPage.OFFSET_SCHEMA_START;
        HeaderPage.setSchemaSize(headerPage, schemaSize);

        // Store hashedColumns spec
        for (int col : hashColumns) {
            hpWriter.writeShort(col);
        }
        int hashColumnsEndPos = hpWriter.getPosition();
        HeaderPage.setHashColumnsSize(headerPage, hashColumnsEndPos - schemaEndPos);

        // Write in empty statistics, so that the values are at least
        // initialized to something.
        StatsWriter statsWriter = new StatsWriter();
        statsWriter.writeTableStats(schema, stats, hpWriter);
        int statsSize = hpWriter.getPosition() - schemaEndPos;
        HeaderPage.setStatsSize(headerPage, statsSize);

        storageManager.logDBPageWrite(headerPage);
    }

    @Override
    public void deleteTupleFile(TupleFile tupleFile) throws IOException {
        // TODO
        throw new UnsupportedOperationException("NYI:  deleteTupleFile()");
    }
}