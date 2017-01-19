package simpledb.struct;

import simpledb.*;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    TupleDesc td;


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
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "r");
            raf.seek(hpid.pageNumber() * BufferPool.PAGE_SIZE);
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            raf.read(data);
            return new HeapPage(hpid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("page does not exist in this file");
        }finally {
            if(raf !=null)
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE);
        raf.write(page.getPageData());
        raf.close();
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
        ArrayList<Page> returnPages = new ArrayList<>();
        int pageNo = 0;
        BufferPool bufferPool = Database.getBufferPool();
        for(;pageNo<numPages();++pageNo) {
            HeapPage page = (HeapPage) bufferPool.getPage(
                    tid, new HeapPageId(getId(), pageNo), Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                returnPages.add(page);
                return returnPages;
            }
        }

        HeapPage page = new HeapPage(new HeapPageId(getId(), pageNo), new byte[BufferPool.PAGE_SIZE]);
        page.insertTuple(t);
        writePage(page);
        returnPages.add(page);
        return returnPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return page;
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
                if(iterator.hasNext()) return true;
                for(int i=pgNo+1;i<numPages();++i) {
                    HeapPageId pid = new HeapPageId(getId(), i);
                    HeapPage page  = (HeapPage)Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_ONLY);
                    if(page.getNumEmptySlots() != page.numSlots)
                        return true;
                }
                return false;
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

