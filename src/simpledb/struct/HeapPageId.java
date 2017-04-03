package simpledb.struct;

import simpledb.BufferPool;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private int tableId_;
    private int pgNo_;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        tableId_ = tableId;
        pgNo_ = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return tableId_;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        return pgNo_;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        final int PRIME = 31;
        int result = 17;
        result = PRIME * result + tableId_;
        result = PRIME * result + pgNo_;
        return result;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if(o == null) return false;
        if(this == o) return true;
        if(getClass() != o.getClass()) return false;
        HeapPageId hpid = (HeapPageId)o;
        return tableId_ == hpid.getTableId() &&
                pgNo_ == hpid.pageNumber();
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

    @Override
    public String toString() {
        return "tableId: " + tableId_ + ", pageNo: " + pageNumber();
    }

}
