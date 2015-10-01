package edu.caltech.test.nanodb.storage;

import edu.caltech.nanodb.storage.*;

import edu.caltech.nanodb.storage.BucketPage;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.io.IOException;

/**
 * This test checks basic read/write operations on the bucket address
 * table (BAT) page.
 * TODO Consider adding tests for error values/filling an entire BAT
 * Created by daniel on 4/17/15.
 */
@Test
public class TestBucketPage extends StorageTestCase {

    /** This is the filename used for the tests in this class. */
    private final String TEST_FILE_NAME = "TestBucketPage_TestFile";


    /** This is the file-manager instance used for the tests in this class. */
    private FileManager fileMgr;


    /** This is the buffer-manager instance used for tests in this class. */
    private BufferManager bufMgr;


    /**
     * Instances of <tt>DBPage</tt> must be associated with a <tt>DBFile</tt>,
     * so this is the file used for testing.  It is created and cleaned up by
     * this class.
     */
    private DBFile dbFile;


    /**
     * This is the <tt>DBPage</tt> object that all tests run against.  Since we
     * are simply writing various values to the data page, we can use a single
     * object for all the different tests.
     */
    private DBPage dbPage;


    /**
     * This set-up method initializes the file manager, data-file, and page that
     * all tests will run against.
     */
    @BeforeClass
    public void beforeClass() throws IOException {

        fileMgr = new FileManagerImpl(testBaseDir);
        bufMgr = new BufferManager(fileMgr);

        // Get DBFile
        DBFileType type = DBFileType.LINEAR_HASH_FILE;
        int pageSize = DBFile.DEFAULT_PAGESIZE; // 8k

        try {
            dbFile = fileMgr.createDBFile(TEST_FILE_NAME, type, pageSize);
        }
        catch (IOException e) {
            // The file is already created
        }

        dbPage = new DBPage(bufMgr, dbFile, 0);
    }

    /**
     * Remove the dbFile created in beforeClass().
     *
     * @throws IOException
     */
    @AfterClass
    public void afterClass() throws IOException {
        fileMgr.deleteDBFile(dbFile);
    }

    public void testBucketPage() {

        BucketPage.setNextBucket(dbPage, 2);
        assert BucketPage.getNextBucket(dbPage) == 2;

        BucketPage.setNextBucket(dbPage, 0);
        assert BucketPage.getNextBucket(dbPage) == 0;

        BucketPage.setNextBucket(dbPage, 1);
        assert BucketPage.getNextBucket(dbPage) == 1;

        BucketPage.setNextBucket(dbPage, 5000);
        assert BucketPage.getNextBucket(dbPage) == 5000;

        BucketPage.setNumSlots(dbPage, 100);
        assert BucketPage.getNumSlots(dbPage) == 100;

        BucketPage.setSlotValue(dbPage, 1, 5000);
        assert BucketPage.getSlotValue(dbPage, 1) == 5000;

    }
}
