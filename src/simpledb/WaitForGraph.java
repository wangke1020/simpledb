package simpledb;

import simpledb.utils.Digraph;


enum LockItemType {
    RESOURCE,
    TRANSACTION
}
class LockItem {
    long id;
    LockItemType type;
    String desc;

    LockItem(long id, LockItemType type, String desc) {
        this.id = id;
        this.type = type;
        this.desc = desc;
    }

    long getId() {
        return id;
    }

    LockItemType getType() {
        return type;
    }

    String getDesc() {
        return desc;
    }

    @Override
    public int hashCode() {
        return (int)(id * 31 +  type.hashCode());
    }

    public boolean equals(Object o) {
        if(o == null) return false;
        if(this == o) return true;
        if(getClass() != o.getClass()) return false;
        LockItem item = (LockItem)o;
        return item.id == this.id &&
                item.type.equals(this.type);
    }

    @Override
    public String toString() {
        return desc;
    }
}

public class WaitForGraph {
    private Digraph<LockItem> digraph;

    public WaitForGraph() {
        digraph = new Digraph<>();
    }

    public void addEdge(LockItem from, LockItem to) {
        digraph.addEdge(from ,to);
    }

    public boolean haveEdge(LockItem from, LockItem to) {
        return digraph.haveEdge(from, to);
    }

    public synchronized void tryAddEdge(LockItem from, LockItem to) throws TransactionAbortedException {
        if(haveEdge(from, to))
            return;

        addEdge(from, to);
        if(haveCircle()) {
            removeEdge(from, to);
            throw new TransactionAbortedException();
        }
    }

    public synchronized void removeEdge(LockItem from, LockItem to) {
        digraph.removeEdge(from, to);
    }

    public boolean haveCircle() {
        return digraph.hasCircle();
    }

    @Override
    public String toString() {
        return digraph.toString();
    }
}
