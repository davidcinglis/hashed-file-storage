package edu.caltech.nanodb.storage.overflowfile;

import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OverflowTupleFileManager implements HashTupleFileManager {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(OverflowTupleFileManager.class);


    /** A reference to the storage manager. */
    private StorageManager storageManager;


    public OverflowTupleFileManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }

    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema)
            throws IOException{
        throw new IllegalArgumentException("Must specify hash key columns.");
    }

    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema,
                                     List<Integer> hashColumns, DBFile overflowFile) throws IOException {
        throw new IllegalArgumentException("Shouldn't have an overflow file.");
    }

    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema, List<Integer> hashColumns)
            throws IOException {

        logger.info(String.format(
                "Initializing new overflow tuple file %s with %d columns",
                dbFile, schema.numColumns()));

        TableStats stats = new TableStats(schema.numColumns());
        OverflowTupleFile tupleFile = new OverflowTupleFile(
                storageManager, this, dbFile, schema, stats, hashColumns);
        saveMetadata(tupleFile);
        tupleFile.initialize();
        logger.warn("initialized the thing");
        return tupleFile;
    }

    @Override
    public TupleFile openTupleFile(DBFile dbFile) throws IOException {

        logger.info("Opening existing overflow tuple file " + dbFile);

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
        ArrayList<Integer> hashCols = new ArrayList<Integer>();
        for (int i = 0; i < HeaderPage.getHashColumnsSize(headerPage); i+=2) {
            hashCols.add((int) hpReader.readShort());
        }

        // Read in the statistics.
        StatsWriter statsWriter = new StatsWriter();
        TableStats stats = statsWriter.readTableStats(hpReader, schema);

        return new OverflowTupleFile(storageManager, this, dbFile,
                schema, stats, hashCols);
    }


    @Override
    public void saveMetadata(TupleFile tupleFile) throws IOException {

        if (tupleFile == null)
            throw new IllegalArgumentException("tupleFile cannot be null");

        if (!(tupleFile instanceof OverflowTupleFile)) {
            throw new IllegalArgumentException(
                    "tupleFile must be an instance of OverflowHashTupleFile");
        }
        OverflowTupleFile hashFile = (OverflowTupleFile) tupleFile;
        DBFile dbFile = tupleFile.getDBFile();

        TableSchema schema = tupleFile.getSchema();
        TableStats stats = tupleFile.getStats();
        List<Integer> hashedColumns = hashFile.getHashedColumns();

        // Table schema is stored into the header page, so get it and prepare
        // to write out the schema information.
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
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
        for (int col : hashedColumns) {
            hpWriter.writeShort(col);
        }
        int hashColumnsEndPos = hpWriter.getPosition();
        HeaderPage.setHashColumnsSize(headerPage, hashColumnsEndPos - schemaEndPos);

        // Write in empty statistics, so that the values are at least
        // initialized to something.
        StatsWriter statsWriter = new StatsWriter();
        statsWriter.writeTableStats(schema, stats, hpWriter);
        int statsSize = hpWriter.getPosition() - hashColumnsEndPos;
        HeaderPage.setStatsSize(headerPage, statsSize);

        storageManager.logDBPageWrite(headerPage);
    }


    @Override
    public void deleteTupleFile(TupleFile tupleFile) throws IOException {
        // TODO
        throw new UnsupportedOperationException("NYI:  deleteTupleFile()");
    }

}