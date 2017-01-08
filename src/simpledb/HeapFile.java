package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    TupleDesc td;
    HeapPage page;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
       return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId hpid = (HeapPageId)pid;
        int pgNo = hpid.pageNumber();
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            raf.seek(hpid.pageNumber() * BufferPool.PAGE_SIZE);
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            raf.read(data);
            return new HeapPage(hpid, data);
        } catch (IOException e) {
           throw new IllegalArgumentException("page does not exist in this file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        final TransactionId transactionId = tid;
        return new DbFileIterator() {
            private boolean opened;
            private int pgNo = 0;
            private Iterator<Tuple> iterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
                rewind();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!opened)
                    return false;

                return pgNo < numPages() - 1 || iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!opened)
                    throw new NoSuchElementException();

                if(iterator.hasNext())
                    return iterator.next();
                else if(pgNo >= numPages()-1)
                    throw new NoSuchElementException();

                setTupleIterator(++pgNo);
                return next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pgNo = 0;
                setTupleIterator(pgNo);
            }

            @Override
            public void close() {
                opened = false;

            }

            private void setTupleIterator(int num) throws TransactionAbortedException, DbException {
                HeapPageId pid = new HeapPageId(getId(), num);
                HeapPage page  = (HeapPage)Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_ONLY);
                iterator = page.iterator();
            }
        };
    }

}

