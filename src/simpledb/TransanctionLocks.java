package simpledb;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangke on 17-3-15.
 */
enum LockType {
    ShareLock,
    ExclusiveLock;

    public static LockType fromPerms(Permissions perms) {
        return perms.getPermLevel() == 0 ? ShareLock : ExclusiveLock;
    }
}

class LockUnit<T>{

    public LockUnit(TransactionId tid, T obj, LockType lockType) {
        this.obj = obj;
        this.tid = tid;
        this.lockType = lockType;
    }
    private T obj;
    private TransactionId tid;
    private LockType lockType;

    public T getObj() {
        return obj;
    }
    public TransactionId getTid(){
        return tid;
    }
    public LockType getLockType() {
        return lockType;
    }


    @Override
    public boolean equals(Object o) {
        if(o == null) return false;
        if(!(o instanceof LockUnit)) return false;
        LockUnit tldObj =  (LockUnit)o;
        return getTid().equals(tldObj.getTid()) &&
                getObj().equals(tldObj.getObj()) &&
                getLockType().equals(tldObj.getLockType());
    }

    @Override
    public int hashCode() {
        return 31 * (31 * tid.hashCode() + obj.hashCode()) + lockType.hashCode();
    }

}

class LockBuckets<T> {
    ConcurrentHashMap<T, HashSet<LockUnit<T>>> buckets = new ConcurrentHashMap<>();
    ConcurrentHashMap<TransactionId, HashSet<T>> holds = new ConcurrentHashMap<>();
    public void addToBucket(TransactionId tid, T obj, LockType type) {
        if(!buckets.containsKey(obj))
            buckets.put(obj, new HashSet<>());
        if(!holds.containsKey(tid)) {
            holds.put(tid, new HashSet<T>());
        }

        HashSet<LockUnit<T>> set = buckets.get(obj);
        LockUnit sharelockUnit = new LockUnit<>(tid, obj, LockType.ShareLock);

        // upgreade shared lock to excluiveLock
        if(set.contains(sharelockUnit) && type.equals(LockType.ExclusiveLock))
            set.remove(sharelockUnit);
        set.add(new LockUnit<>(tid, obj,type));
        holds.get(tid).add(obj);
    }

    public boolean isExclusiveLocked(T obj) {
        HashSet<LockUnit<T>> set = buckets.get(obj);
        if(set == null) return false;

        for(LockUnit entity : set) {
            if(entity.getLockType().equals(LockType.ExclusiveLock))
                return true;
        }
        return false;
    }

    public boolean locked(T obj) {
        HashSet<LockUnit<T>> set = buckets.get(obj);
        return set != null && (!set.isEmpty());
    }

    public LockType holdLock(TransactionId tid, T obj) {
        HashSet<LockUnit<T>> set = buckets.get(obj);
        if(set == null || set.isEmpty()) return null;
        if(set.contains(new LockUnit<T>(tid, obj, LockType.ExclusiveLock)))
            return LockType.ExclusiveLock;
        if(set.contains(new LockUnit<T>(tid, obj, LockType.ShareLock)))
            return LockType.ShareLock;
        return null;
    }

    public void releaseLock(TransactionId tid, T obj, LockType type) {
        HashSet<LockUnit<T>> set = buckets.get(obj);
        set.remove(new LockUnit<>(tid, obj, type));

        HashSet<T> tsets = holds.get(tid);
        tsets.remove(obj);
        if(tsets.size() == 0)
            holds.remove(tid);
    }
    public HashSet<T> getHolds(TransactionId tid) {
        return holds.get(tid);
    }
}

class TransanctionLocks<T> {

    private LockBuckets<T> buckets = new LockBuckets<>();

    public void acquireLock(TransactionId tid, T obj, Permissions perms) throws TransactionAbortedException {

        while(true) {
            boolean isExclusiveLocked = buckets.isExclusiveLocked(obj);
            LockType holdLock = buckets.holdLock(tid, obj);
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
            if (buckets.locked(obj)) {
                if(holdLock == null && lockType.equals(LockType.ExclusiveLock)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

            }
            buckets.addToBucket(tid, obj, lockType);
            break;
        }

    }

    public void releaseLock(TransactionId tid, T obj) {
        LockType lockType = buckets.holdLock(tid, obj);
        if(lockType != null)
            buckets.releaseLock(tid, obj, lockType);

    }

    public boolean holdsLock(TransactionId tid, T obj){
        return getHolds(tid).contains(obj);
    }
    public HashSet<T> getHolds(TransactionId tid) {
        return buckets.getHolds(tid);
    }

    public void releaseAllLocks(TransactionId tid) {
        HashSet<T> holds = getHolds(tid);
        HashSet<T> copys = new HashSet<T>();
        copys.addAll(holds);

        for(T t : copys) {
            releaseLock(tid, t);
        }
    }
}