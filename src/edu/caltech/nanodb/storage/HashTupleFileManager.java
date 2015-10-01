package edu.caltech.nanodb.storage;

import edu.caltech.nanodb.relations.TableSchema;

import java.io.IOException;
import java.util.List;

/**
 * Created by daniel on 5/8/15.
 */
public interface HashTupleFileManager extends TupleFileManager {
    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema,
                                     List<Integer> hashColumns, DBFile overflowFile)
            throws IOException;

    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema,
                                     List<Integer> hashColumns) throws IOException;
}