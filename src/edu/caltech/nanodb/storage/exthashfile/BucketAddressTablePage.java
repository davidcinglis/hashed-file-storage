package edu.caltech.nanodb.storage.exthashfile;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.DBFileType;
import org.apache.log4j.Logger;

/**
 * This class manipulates the BucketAddressTablePage
 *
 * <ul>
 *   <li><u>Byte 0:</u>  {@link DBFileType#EXTENDABLE_HASH_FILE} (unsigned byte)</li>
 *   <li><u>Byte 1:</u>  page size  <i>p</i> (unsigned byte) - file's page
 *       size is <i>P</i> = 2<sup>p</sup></li>
 *
 *   <li>Byte 2-M:  shorts indicating page numbers mapped to each prefix.
 *      Each prefix represents an index into the table, and the mapped
 *      bucket page is present at OFFSET_TABLE_START + ADDR_SIZE + prefix.
 * </ul>
 */
public class BucketAddressTablePage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BucketAddressTablePage.class);
    /**
     * The offset in the bucket address page where the current hash prefix
     * length is stored.  This value is an unsigned short.
     */
    public static final int OFFSET_PREFIX_LEN = 2;

    /**
     * The offset in the bucket address page where the table begins.
     */
    public static final int OFFSET_TABLE_START = 4;

    /**
     * The size of each bucket address (page number) associated with each
     * hash prefix.
     */
    public static final int ADDR_SIZE = 2;

    public static int getBucket(DBPage dbPage, int hash) {
        // Read the BAT's prefix length
        PageReader bucketReader = new PageReader(dbPage);
        int prefixLength = dbPage.readUnsignedShort(OFFSET_PREFIX_LEN);

        // Shift the hash to obtain the prefix/index
        int index = hash >>> (Integer.SIZE - prefixLength);

        // Index into the BAT by the prefix and read an unsigned short
        // to obtain the bucket
        bucketReader.setPosition(OFFSET_TABLE_START + index * ADDR_SIZE);
        return bucketReader.readUnsignedShort();
    }

    /**
     * Writes a bucket page number for the given prefix
     * @param dbPage The page to be written to
     * @param prefix The prefix associated with the bucket
     * @param bucket The bucket page number to write
     */
    public static void setBucket(DBPage dbPage, int prefix, int bucket) {
        // Check if this is a valid bucket
        int maxPrefix = (int) Math.pow(2, getPrefixLength(dbPage));
        if (prefix > maxPrefix) {
            throw new IllegalArgumentException(
                    "Prefix is too big for current prefix size; was given " +
                            "value " + prefix);
        }
        // Write the bucket value at the correct offset
        PageWriter bucketWriter = new PageWriter(dbPage);
        bucketWriter.setPosition(OFFSET_TABLE_START + prefix * ADDR_SIZE);
        bucketWriter.writeShort(bucket);
    }

    /**
     * Updates the bucket address table to have prefix length equal to the
     * passed argument
     * @param length the new prefix length of the BAT
     */
    public static void setPrefixLength(DBPage dbPage, int length) {
        //
        int maxLength = (dbPage.getPageSize() - 4) / ADDR_SIZE;
        if (length > maxLength) {
            throw new IllegalArgumentException(
                    "Prefix length is too big for the page size; was given " +
                            "length " + length);
        }
        dbPage.writeShort(OFFSET_PREFIX_LEN, length);
    }

    /**
     * Reads the prefix length for the BAT
     * @param dbPage The BAT page
     * @return the BAT prefix length
     */
    public static int getPrefixLength(DBPage dbPage) {
        return dbPage.readUnsignedShort(OFFSET_PREFIX_LEN);
    }
}