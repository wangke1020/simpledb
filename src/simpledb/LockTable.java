package simpledb;

import org.apache.mina.util.ConcurrentHashSet;
import simpledb.struct.PageId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangke on 17-3-15.
 */

class LockTable {

    enum LockType {
        ShareLock,
        ExclusiveLock;

        public static LockType fromPerms(Permissions perms) {
            return perms.getPermLevel() == 0 ? ShareLock : ExclusiveLock;
        }
    }

    class LockUnit {

        LockUnit(TransactionId tid, LockType lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }
        private TransactionId tid;
        private LockType lockType;

        TransactionId getTid(){
            return tid;
        }
        LockType getLockType() {
            return lockType;
        }


        @Override
        public boolean equals(Object o) {
            if(o == null) return false;
            if(!(o instanceof LockUnit)) return false;
            LockUnit tldObj =  (LockUnit)o;
            return getTid().equals(tldObj.getTid()) &&
                    getLockType().equals(tldObj.getLockType());
        }

        @Override
        public int hashCode() {
            return 31 * tid.hashCode() + lockType.hashCode();
        }

    }

    private ConcurrentHashMap<PageId, ConcurrentHashSet<LockUnit>> bucket = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, ConcurrentHashSet<PageId>> holds = new ConcurrentHashMap<>();
    private WaitForGraph waitForGraph = new WaitForGraph();

    private void addToBucket(TransactionId tid, PageId pid, LockType type) {
        if(!bucket.containsKey(pid))
            bucket.put(pid, new ConcurrentHashSet<>());
        if(!holds.containsKey(tid)) {
            holds.put(tid, new ConcurrentHashSet<>());
        }

        ConcurrentHashSet<LockUnit> set = bucket.get(pid);
        LockUnit SLockUnit = new LockUnit(tid, LockType.ShareLock);

        // upgrade shared lock to X lock
        if(set.contains(SLockUnit) && type.equals(LockType.ExclusiveLock))
            set.remove(SLockUnit);
        set.add(new LockUnit(tid,type));
        holds.get(tid).add(pid);
    }

    private boolean isExclusiveLocked(PageId pid) {
        ConcurrentHashSet<LockUnit> set = bucket.get(pid);
        if(set == null) return false;

        for(LockUnit entity : set) {
            if(entity.getLockType().equals(LockType.ExclusiveLock))
                return true;
        }
        return false;
    }

    private Set<LockUnit> locksOnPage(PageId pid) {
        ConcurrentHashSet<LockUnit> set = bucket.get(pid);
        if(set == null) return new HashSet<>();
        return set;
    }

    private boolean locked(PageId pid) {
        return locksOnPage(pid).size() > 0;
    }

    private LockType holdLock(TransactionId tid, PageId pid) {
        ConcurrentHashSet<LockUnit> set = bucket.get(pid);
        if(set == null || set.isEmpty()) return null;
        if(set.contains(new LockUnit(tid,LockType.ExclusiveLock)))
            return LockType.ExclusiveLock;
        if(set.contains(new LockUnit(tid, LockType.ShareLock)))
            return LockType.ShareLock;
        return null;
    }

    private void releaseLock(TransactionId tid, PageId pid, LockType type) {
        ConcurrentHashSet<LockUnit> set = bucket.get(pid);
        set.remove(new LockUnit(tid, type));

        ConcurrentHashSet<PageId> pageIds = holds.get(tid);
        pageIds.remove(pid);
        if(pageIds.size() == 0)
            holds.remove(tid);
    }

    public ConcurrentHashSet<PageId> getHolds(TransactionId tid) {
        return holds.get(tid);
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perms) throws TransactionAbortedException {

         LockItem lockRes = new LockItem(pid.hashCode(), LockItemType.RESOURCE, pid.toString());
        LockItem lockRequester = new LockItem(tid.getId(), LockItemType.TRANSACTION, tid.toString());

        while(true) {
            boolean isExclusiveLocked = isExclusiveLocked(pid);
            LockType holdLock = holdLock(tid, pid);
            if (isExclusiveLocked) {

                if (holdLock != null && holdLock.equals(LockType.ExclusiveLock)) {
                    // tid already hold X lock.
                    return;
                }
                else {
                    // Check deadlock and wait
                    waitForGraph.tryAddEdge(lockRequester, lockRes);

                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            }

            // No X lock on page
            LockType lockType = LockType.fromPerms(perms);
            if (locked(pid)) {
                // S lock existed.
                Set<LockUnit> locks = locksOnPage(pid);
                boolean alreadyholdSLock = locks.contains(new LockUnit(tid, LockType.ShareLock));

                if(! alreadyholdSLock) {
                    // No S lock hold
                    if(lockType.equals(LockType.ShareLock)) {
                        // Request for S lock
                        addToBucket(tid, pid, lockType);
                        waitForGraph.addEdge(lockRes, lockRequester);
                        return;
                    }else {
                        // request for X lock but S lock holds by other transaction.
                        waitForGraph.tryAddEdge(lockRequester, lockRes);
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    }
                }

                if(lockType.equals(LockType.ShareLock)) {
                    // Already hold S lock and request for S lock
                    return;
                }
                if(locks.size() == 1) {
                    // only the transaction hold S lock and can upgrade X lock.
                    addToBucket(tid, pid, lockType);
                    waitForGraph.addEdge(lockRes, lockRequester);
                    return;
                }
                else {
                    // other transaction alse hold S lock, can not upgrade to X lock, wait
                    waitForGraph.tryAddEdge(lockRequester, lockRes);
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            }

            addToBucket(tid, pid, lockType);
            waitForGraph.addEdge(lockRes, lockRequester);
            return;
        }
    }

    public  void releaseLock(TransactionId tid, PageId pid) {
        LockType lockType = holdLock(tid, pid);
        if(lockType != null) {
            releaseLock(tid, pid, lockType);
            LockItem lockRes = new LockItem(pid.hashCode(), LockItemType.RESOURCE, pid.toString());
            LockItem lockRequster = new LockItem(tid.getId(), LockItemType.TRANSACTION, tid.toString());
            waitForGraph.removeEdge(lockRes, lockRequster);
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid){
        return getHolds(tid).contains(pid);
    }

    public void releaseAllLocks(TransactionId tid) {
        ConcurrentHashSet<PageId> holds = getHolds(tid);
        if(holds == null) {
            return;
        }
        HashSet<PageId> copys = new HashSet<>();
        copys.addAll(holds);

        for(PageId pid : copys) {
            releaseLock(tid, pid);
        }
    }
}