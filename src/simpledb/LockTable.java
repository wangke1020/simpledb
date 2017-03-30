package simpledb;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangke on 17-3-15.
 */

class LockTable<T> {

    enum LockType {
        ShareLock,
        ExclusiveLock;

        public static LockType fromPerms(Permissions perms) {
            return perms.getPermLevel() == 0 ? ShareLock : ExclusiveLock;
        }
    }

    class LockUnit<T> {

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

    private ConcurrentHashMap<T, HashSet<LockUnit>> bucket = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, HashSet<T>> holds = new ConcurrentHashMap<>();
    private WaitForGraph waitForGraph = new WaitForGraph();

    private void addToBucket(TransactionId tid, T obj, LockType type) {
        if(!bucket.containsKey(obj))
            bucket.put(obj, new HashSet<>());
        if(!holds.containsKey(tid)) {
            holds.put(tid, new HashSet<>());
        }

        HashSet<LockUnit> set = bucket.get(obj);
        LockUnit SLockUnit = new LockUnit(tid, LockType.ShareLock);

        // upgrade shared lock to X lock
        if(set.contains(SLockUnit) && type.equals(LockType.ExclusiveLock))
            set.remove(SLockUnit);
        set.add(new LockUnit(tid,type));
        holds.get(tid).add(obj);
    }

    private boolean isExclusiveLocked(T obj) {
        HashSet<LockUnit> set = bucket.get(obj);
        if(set == null) return false;

        for(LockUnit entity : set) {
            if(entity.getLockType().equals(LockType.ExclusiveLock))
                return true;
        }
        return false;
    }

    private boolean locked(T obj) {
        HashSet<LockUnit> set = bucket.get(obj);
        return set != null && (!set.isEmpty());
    }

    private LockType holdLock(TransactionId tid, T obj) {
        HashSet<LockUnit> set = bucket.get(obj);
        if(set == null || set.isEmpty()) return null;
        if(set.contains(new LockUnit(tid,LockType.ExclusiveLock)))
            return LockType.ExclusiveLock;
        if(set.contains(new LockUnit(tid, LockType.ShareLock)))
            return LockType.ShareLock;
        return null;
    }

    private void releaseLock(TransactionId tid, T obj, LockType type) {
        HashSet<LockUnit> set = bucket.get(obj);
        set.remove(new LockUnit(tid, type));

        HashSet<T> tsets = holds.get(tid);
        tsets.remove(obj);
        if(tsets.size() == 0)
            holds.remove(tid);
    }

    public HashSet<T> getHolds(TransactionId tid) {
        return holds.get(tid);
    }

    public void acquireLock(TransactionId tid, T obj, Permissions perms) throws TransactionAbortedException {

        while(true) {
            boolean isExclusiveLocked = isExclusiveLocked(obj);
            LockType holdLock = holdLock(tid, obj);
            if (isExclusiveLocked) {
                if (holdLock != null && holdLock.equals(LockType.ExclusiveLock))
                    return;
                else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            }

            LockType lockType = LockType.fromPerms(perms);
            if (locked(obj)) {
                if(holdLock == null && lockType.equals(LockType.ExclusiveLock)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

            }
            addToBucket(tid, obj, lockType);
            break;
        }
    }

    public void releaseLock(TransactionId tid, T obj) {
        LockType lockType = holdLock(tid, obj);
        if(lockType != null)
            releaseLock(tid, obj, lockType);

    }

    public boolean holdsLock(TransactionId tid, T obj){
        return getHolds(tid).contains(obj);
    }

    public void releaseAllLocks(TransactionId tid) {
        HashSet<T> holds = getHolds(tid);
        HashSet<T> copys = new HashSet<>();
        copys.addAll(holds);

        for(T t : copys) {
            releaseLock(tid, t);
        }
    }
}