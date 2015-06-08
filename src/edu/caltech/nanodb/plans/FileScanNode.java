package edu.caltech.nanodb.plans;


import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.*;
import edu.caltech.nanodb.storage.linhash.LinHashTupleFile;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.qeval.PlanCost;
import edu.caltech.nanodb.qeval.SelectivityEstimator;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableInfo;


/**
 * <p>
 * A select plan-node that scans a tuple file, checking the optional predicate
 * against each tuple in the file.  Note that there are no optimizations used
 * if the tuple file is a sequential tuple file. Hashed tuple files have some
 * optimization, provided that the predicate applied to the file-scan contains
 * equality comparisons on the hash key.
 * </p>
 * <p>
 * This plan node can also be used with indexes, when a "file-scan" is to be
 * performed over all of the index's tuples, in whatever order the index will
 * produce the tuples.  If the planner wishes to take advantage of an index's
 * ability to look up tuples based on various values, the {@link IndexScanNode}
 * should be used instead.
 * </p>
 */
public class FileScanNode extends SelectNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FileScanNode.class);


    /**
     * The table-info for the table being scanned, or {@code null} if the node
     * is performing a scan over an index.
     */
    private TableInfo tableInfo;


    /**
     * The index-info for the index being scanned, or {@code null} if the node
     * is performing a scan over a table.
     */
    private IndexInfo indexInfo;


    /** The table to select from if this node is a leaf. */
    private TupleFile tupleFile;


    /**
     * This field allows the file-scan node to mark a particular tuple in the
     * tuple-stream and then rewind to that point in the tuple-stream.
     */
    private FilePointer markedTuple;

    /**
     * The search key to use for scanning hash files, if applicable. This
     * field will be null if no hash key can be used, otherwise it will be a
     * TupleLiteral of the key value.
     */
    private Tuple key;


    private boolean jumpToMarkedTuple;


    /**
     * Construct a file scan node that traverses a table file.
     *
     * @param tableInfo the information about the table being scanned
     * @param predicate an optional predicate for selection, or {@code null}
     *        if all rows in the table should be included in the output
     */
    public FileScanNode(TableInfo tableInfo, Expression predicate) {
        super(predicate);

        if (tableInfo == null)
            throw new IllegalArgumentException("tableInfo cannot be null");

        this.tableInfo = tableInfo;
        tupleFile = tableInfo.getTupleFile();
        key = null;
    }


    /**
     * Construct a file scan node that traverses an index file.
     *
     * @param indexInfo the information about the index being scanned
     * @param predicate an optional predicate for selection, or {@code null}
     *        if all rows in the index should be included in the output
     */
    public FileScanNode(IndexInfo indexInfo, Expression predicate) {
        super(predicate);

        if (indexInfo == null)
            throw new IllegalArgumentException("indexInfo cannot be null");

        this.indexInfo = indexInfo;
        tupleFile = indexInfo.getTupleFile();
    }


    /**
     * Returns true if the passed-in object is a <tt>FileScanNode</tt> with
     * the same predicate and table.
     *
     * @param obj the object to check for equality
     *
     * @return true if the passed-in object is equal to this object; false
     *         otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FileScanNode) {
            FileScanNode other = (FileScanNode) obj;
            // We don't include the table-info or the index-info since each
            // table or index is in its own tuple file.
            return tupleFile.equals(other.tupleFile) &&
                    predicate.equals(other.predicate);
        }

        return false;
    }


    /**
     * Computes the hashcode of a PlanNode.  This method is used to see if two
     * plan nodes CAN be equal.
     **/
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        // We don't include the table-info or the index-info since each table
        // or index is in its own tuple file.
        hash = 31 * hash + tupleFile.hashCode();
        return hash;
    }


    /**
     * Creates a copy of this simple filter node node and its subtree.  This
     * method is used by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        FileScanNode node = (FileScanNode) super.clone();

        // TODO:  Should we clone these?
        node.tableInfo = tableInfo;
        node.indexInfo = indexInfo;

        // The tuple file doesn't need to be copied since it's immutable.
        node.tupleFile = tupleFile;

        return node;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("FileScan[");
        if (tableInfo != null) {
            buf.append("table:  ").append(tableInfo.getTableName());
        }
        else if (indexInfo != null) {
            buf.append("index:  ").append(indexInfo.getTableName());
            buf.append('.').append(indexInfo.getIndexName());
        }
        else {
            throw new IllegalStateException("Both tableInfo and indexInfo " +
                    "are null!");
        }

        if (predicate != null)
            buf.append(", pred:  ").append(predicate.toString());

        buf.append("]");

        return buf.toString();
    }


    /**
     * Currently we will always say that the file-scan node produces unsorted
     * results.  In actuality, a file scan's results will be sorted if the table
     * file uses a sequential format, but currently we don't have any sequential
     * file formats.
     */
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** This node supports marking. */
    public boolean supportsMarking() {
        return true;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    protected void prepareSchema() {
        // Grab the schema from the table.
        schema = tupleFile.getSchema();
    }


    // Inherit javadocs from base class.
    public void prepare() {
        // Grab the schema and DBFile
        schema = tupleFile.getSchema();
        DBFile dbFile = tupleFile.getDBFile();

        // If we have an extendable hash file, check if we can optimize
        if (dbFile.getType() == DBFileType.LINEAR_HASH_FILE &&
                predicate != null) {
            LinHashTupleFile hashFile = (LinHashTupleFile) tupleFile;

            // Get the hash key columns
            ArrayList<ColumnName> keyColNames = new ArrayList<>();
            for (int i : hashFile.getHashColumns()) {
                keyColNames.add(schema.getColumnInfo(i).getColumnName());
            }

            // Get predicate conjuncts and check if the predicate is on the
            // hash key
            ArrayList<Expression> conjuncts = new ArrayList<>();
            PredicateUtils.collectConjuncts(predicate, conjuncts);
            HashMap<ColumnName, Object> predicateValues = new HashMap<>();
            for (Expression c : conjuncts) {
                // We only want to look at equality operators
                if (c instanceof CompareOperator &&
                        ((CompareOperator) c).getType() ==
                                CompareOperator.Type.EQUALS) {
                    // Get column on left and literal on right
                    ((CompareOperator) c).normalize();
                    Expression left = ((CompareOperator) c).getLeftExpression();
                    Expression right = ((CompareOperator) c).getRightExpression();
                    if (left instanceof ColumnValue &&
                            right instanceof LiteralValue) {
                        // If the predicate conjunct in the right format, record it
                        predicateValues.put(((ColumnValue) left).getColumnName(),
                                right.evaluate(null));
                    }
                }
            }

            // If our predicate uses the whole hash key, construct a search key
            // to use
            if (predicateValues.keySet().containsAll(keyColNames)) {
                TupleLiteral newKey = new TupleLiteral();
                for (ColumnName name : keyColNames) {
                    newKey.addValue(predicateValues.get(name));
                }
                key = newKey;
            }
        }

        // Get table stats
        TableStats tableStats = tupleFile.getStats();
        stats = tableStats.getAllColumnStats();

        // If we don't have a predicate, selectivity is 100%, otherwise call the
        // helper function.
        float selectivity = 1.0f;
        if (predicate != null) {
            selectivity = SelectivityEstimator.estimateSelectivity(predicate,
                    schema, stats);
        }

        // Grab the left child's cost, then update the cost based on the
        // selectivity of our predicate.

        float numTuples = tableStats.numTuples;
        numTuples *= selectivity;

        // The CPU cost will be proportional to the total number of tuples, not
        // the number of tuples we expect to output.
        cost = new PlanCost(numTuples, tableStats.avgTupleSize,
                tableStats.numTuples, tableStats.numDataPages);

        // TODO:  We should also update the table statistics based on the
        //        predicate, but that's too complicated, so we'll leave them
        //        unchanged for now.
    }


    public void initialize() {
        super.initialize();

        // Reset our marking state.
        markedTuple = null;
        jumpToMarkedTuple = false;
    }


    public void cleanUp() {
        // Nothing to do!
    }


    /**
     * Advances the current tuple forward for a file scan. Grabs the first tuple
     * if current is null. Otherwise gets the next tuple.
     *
     * @throws java.io.IOException if the TableManager failed to open the table.
     */
    protected void advanceCurrentTuple() throws IOException {

        if (jumpToMarkedTuple) {
            logger.debug("Resuming at previously marked tuple.");
            try {
                currentTuple = tupleFile.getTuple(markedTuple);
            }
            catch (InvalidFilePointerException e) {
                throw new IOException(
                        "Couldn't resume at previously marked tuple!", e);
            }
            jumpToMarkedTuple = false;

            return;
        }

        if (currentTuple == null)
            // Use optimized scan if possible
            if (key == null) {
                currentTuple = tupleFile.getFirstTuple();
            } else {
                currentTuple = ((LinHashTupleFile)
                        tupleFile).findFirstTupleEquals(key);
            }
        else
            // Use optimized scan if possible
            if (key == null) {
                currentTuple = tupleFile.getNextTuple(currentTuple);
            } else {
                currentTuple = ((LinHashTupleFile)
                        tupleFile).findNextTupleEquals(currentTuple);
            }
    }


    public void markCurrentPosition() {
        if (currentTuple == null)
            throw new IllegalStateException("There is no current tuple!");

        logger.debug("Marking current position in tuple-stream.");
        markedTuple = currentTuple.getExternalReference();
    }


    public void resetToLastMark() {
        if (markedTuple == null)
            throw new IllegalStateException("There is no last-marked tuple!");

        logger.debug("Resetting to previously marked position in tuple-stream.");
        jumpToMarkedTuple = true;
    }
}