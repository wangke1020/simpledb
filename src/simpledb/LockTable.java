package simpledb;

import simpledb.struct.PageId;

import java.util.HashSet;
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

    private ConcurrentHashMap<PageId, HashSet<LockUnit>> bucket = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> holds = new ConcurrentHashMap<>();
    private WaitForGraph waitForGraph = new WaitForGraph();

    private void addToBucket(TransactionId tid, PageId pid, LockType type) {
        if(!bucket.containsKey(pid))
            bucket.put(pid, new HashSet<>());
        if(!holds.containsKey(tid)) {
            holds.put(tid, new HashSet<>());
        }

        HashSet<LockUnit> set = bucket.get(pid);
        LockUnit SLockUnit = new LockUnit(tid, LockType.ShareLock);

        // upgrade shared lock to X lock
        if(set.contains(SLockUnit) && type.equals(LockType.ExclusiveLock))
            set.remove(SLockUnit);
        set.add(new LockUnit(tid,type));
        holds.get(tid).add(pid);
    }

    private boolean isExclusiveLocked(PageId pid) {
        HashSet<LockUnit> set = bucket.get(pid);
        if(set == null) return false;

        for(LockUnit entity : set) {
            if(entity.getLockType().equals(LockType.ExclusiveLock))
                return true;
        }
        return false;
    }

    private boolean locked(PageId pid) {
        HashSet<LockUnit> set = bucket.get(pid);
        return set != null && (!set.isEmpty());
    }

    private LockType holdLock(TransactionId tid, PageId pid) {
        HashSet<LockUnit> set = bucket.get(pid);
        if(set == null || set.isEmpty()) return null;
        if(set.contains(new LockUnit(tid,LockType.ExclusiveLock)))
            return LockType.ExclusiveLock;
        if(set.contains(new LockUnit(tid, LockType.ShareLock)))
            return LockType.ShareLock;
        return null;
    }

    private TransactionId getOwner(PageId pid) {
        HashSet<LockUnit> set = bucket.get(pid);
        return set.iterator().next().getTid();
    }

    private void releaseLock(TransactionId tid, PageId pid, LockType type) {
        HashSet<LockUnit> set = bucket.get(pid);
        set.remove(new LockUnit(tid, type));

        HashSet<PageId> pageIds = holds.get(tid);
        pageIds.remove(pid);
        if(pageIds.size() == 0)
            holds.remove(tid);
    }

    public HashSet<PageId> getHolds(TransactionId tid) {
        return holds.get(tid);
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perms) throws TransactionAbortedException {

        while(true) {
            boolean isExclusiveLocked = isExclusiveLocked(pid);
            LockType holdLock = holdLock(tid, pid);
            if (isExclusiveLocked) {
                if (holdLock != null && holdLock.equals(LockType.ExclusiveLock))
                    return;
                else {
                    TransactionId toTid = getOwner(pid);

                    LockItem from = new LockItem(tid.getId(), LockItemType.TRANSACTION);
                    LockItem to = new LockItem(toTid.getId(), LockItemType.TRANSACTION);
                    waitForGraph.addEdge(from, to);
                    if(waitForGraph.haveCircle()) {
                        waitForGraph.removeEdge(from, to);
                        throw new TransactionAbortedException();
                    }else {
                        waitForGraph.removeEdge(from, to);
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            }

            LockType lockType = LockType.fromPerms(perms);
            if (locked(pid)) {
                if(holdLock == null && lockType.equals(LockType.ExclusiveLock)) {

                    TransactionId toTid = getOwner(pid);

                    LockItem from = new LockItem(tid.getId(), LockItemType.TRANSACTION);
                    LockItem to = new LockItem(toTid.getId(), LockItemType.TRANSACTION);
                    waitForGraph.addEdge(from, to);
                    if(waitForGraph.haveCircle()) {
                        waitForGraph.removeEdge(from, to);
                        throw new TransactionAbortedException();
                    }else {
                        waitForGraph.removeEdge(from, to);
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

            }
            addToBucket(tid, pid, lockType);
            LockItem from = new LockItem(pid.hashCode(), LockItemType.RESOURCE);
            LockItem to = new LockItem(tid.getId(), LockItemType.TRANSACTION);
            waitForGraph.addEdge(from, to);
            break;
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        LockType lockType = holdLock(tid, pid);
        if(lockType != null) {
            releaseLock(tid, pid, lockType);
            LockItem from = new LockItem(pid.hashCode(), LockItemType.RESOURCE);
            LockItem to = new LockItem(tid.getId(), LockItemType.TRANSACTION);
            waitForGraph.removeEdge(from, to);
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid){
        return getHolds(tid).contains(pid);
    }

    public void releaseAllLocks(TransactionId tid) {
        HashSet<PageId> holds = getHolds(tid);
        HashSet<PageId> copys = new HashSet<>();
        copys.addAll(holds);

        for(PageId pid : copys) {
            releaseLock(tid, pid);
        }
    }
}