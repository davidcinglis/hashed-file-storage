package edu.caltech.test.nanodb.storage.linhash;

import org.testng.annotations.Test;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.test.nanodb.storage.TableFormatTestCase;


@Test
public class TestLinearHashFormat extends TableFormatTestCase {

    /**
     * Inserts into a table file, where everything should stay within a single
     * data page.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHashTableOnePageInsert() throws Exception {
        tryDoCommand("CREATE TABLE hash_1p_ins (a INTEGER, b VARCHAR(20)) " +
                "PROPERTIES (storage = 'lin-hash', pagesize = 4096, hashkey = '0');", false);

        insertRows("hash_1p_ins", 150, 200, 3, 20, /* ordered */ false,
                   /* delete */ false);
    }


    /**
     * Inserts and deletes from a table file, where everything should stay
     * within a single data page.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHashTableOnePageInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE hash_1p_insdel (a INTEGER, b VARCHAR(20)) " +
                "PROPERTIES (storage = 'lin-hash', pagesize = 4096, hashkey = '0');", false);

        insertRows("hash_1p_insdel", 150, 200, 3, 20, /* ordered */ false,
                   /* delete */ true);
    }


    /**
     * Inserts into a table file, where everything should stay within about
     * fifteen data pages and land in the same bucket. This forces the
     * bucket table to expand to its maximum size, then continue to extend
     * the bucket, since splitting is no longer possible.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHashTableMultiPageInsert() throws Exception {
        tryDoCommand("CREATE TABLE hash_mp_ins (a INTEGER, b VARCHAR(50)) " +
                "PROPERTIES (storage = 'lin-hash', pagesize = 4096, hashkey = '0');", false);

        // This should require around 15 pages.
        insertRows("hash_mp_ins", 1500, 200, 20, 50, /* ordered */ false,
                   /* delete */ false);
    }


    /**
     * Inserts and deletes from a table file, where everything should stay
     * within about ten data pages.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHashTableMultiPageInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE hash_mp_insdel (a INTEGER, b VARCHAR(50)) " +
                "PROPERTIES (storage = 'lin-hash', pagesize = 4096, hashkey = '0');", false);

        // Not sure how many pages this will require, since tuples will be
        // deleted along the way.
        insertRows("hash_mp_insdel", 3000, 200, 20, 50, /* ordered */ false,
                   /* delete */ true);
    }


    /**
     * Inserts and then deletes a sequence of 10000 rows, so that we can
     * detect if header entries are leaked, or tuple data ranges are leaked.
     * At the end of the test, the table file should have nothing.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testInsertDeleteManyTimes() throws Exception {
        tryDoCommand("CREATE TABLE hash_insdel (a INTEGER, b VARCHAR(20)) " +
                "PROPERTIES (storage = 'lin-hash', pagesize = 4096, hashkey = '0');");

        // Insert a row, then delete it immediately.
        // If this loop starts failing after a hundred or more iterations
        // then it could be that tuple data is not being reclaimed.  If it
        // starts failing after around a thousand iterations then it could be
        // that header data is not being reclaimed.
        for (int i = 0; i < 10000; i++) {
            tryDoCommand(String.format("INSERT INTO %s VALUES (%d, '%s');",
                    "hash_insdel", i, makeRandomString(3, 20)));
            tryDoCommand(String.format("DELETE FROM %s WHERE a = %d;",
                    "hash_insdel", i));
        }

        // Should have deleted everything.
        CommandResult result = tryDoCommand("SELECT * FROM hash_insdel;", true);
        assert result.getTuples().size() == 0;
    }
    /**
     * Inserts into a table file, where everything should stay within a single
     * data page. Uses a multiple-column hash key.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHashTable2ColKey() throws Exception {
        tryDoCommand("CREATE TABLE hash_2col (a INTEGER, b VARCHAR(20)) " +
                "PROPERTIES (storage = 'lin-hash', pagesize = 4096, hashkey = '0, 1');", false);

        insertRows("hash_2col", 150, 200, 3, 20, /* ordered */ false,
                   /* delete */ false);
    }

    /**
     * Inserts into a table file, where tuples should be roughly uniformly
     * distributed amongst buckets.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHashTableUniformInsert() throws Exception {
        tryDoCommand("CREATE TABLE hash_ins (a INTEGER, b VARCHAR(20)) " +
                "PROPERTIES (storage = 'lin-hash', pagesize = 4096, hashkey = '1');", false);

        insertRows("hash_ins", 15000, Integer.MAX_VALUE, 3, 20, /* ordered */ false,
                   /* delete */ false);
    }

}