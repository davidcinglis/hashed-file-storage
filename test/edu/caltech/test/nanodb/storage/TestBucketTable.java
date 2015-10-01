package edu.caltech.test.nanodb.storage;

import edu.caltech.nanodb.storage.*;

import edu.caltech.nanodb.storage.exthashfile.BucketAddressTablePage;
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
public class TestBucketTable extends StorageTestCase {

    /** This is the filename used for the tests in this class. */
    private final String TEST_FILE_NAME = "TestBucketTable_TestFile";


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
        DBFileType type = DBFileType.EXTENDABLE_HASH_FILE;
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

    public void testReadWriteBucket() {
        int prefix0 = 0;
        int prefix1 = 1;
        int prefix2 = 2;
        int prefix3 = 3;
        // Set prefix length to 2
        BucketAddressTablePage.setPrefixLength(dbPage, 2);
        int prefixLength = BucketAddressTablePage.getPrefixLength(dbPage);

        // Set buckets to certain values
        BucketAddressTablePage.setBucket(dbPage, prefix0, 1);
        // Check 0 and 63335 cases
        BucketAddressTablePage.setBucket(dbPage, prefix1, 65535);
        BucketAddressTablePage.setBucket(dbPage, prefix2, 0);
        BucketAddressTablePage.setBucket(dbPage, prefix3, 512);

        // Get buckets for hash values with the proper prefixes
        int bucket0 = BucketAddressTablePage.getBucket(dbPage, prefix0 << 30);
        int bucket1 = BucketAddressTablePage.getBucket(dbPage, prefix1 << 30);
        int bucket2 = BucketAddressTablePage.getBucket(dbPage, prefix2 << 30);
        int bucket3 = BucketAddressTablePage.getBucket(dbPage, prefix3 << 30);

        // Assert correct values
        assert prefixLength == 2;
        assert bucket0 == 1;
        assert bucket1 == 65535;
        assert bucket2 == 0;
        assert bucket3 == 512;
    }
}
