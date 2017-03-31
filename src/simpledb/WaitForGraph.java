package simpledb;

import simpledb.utils.Digraph;


enum LockItemType {
    RESOURCE,
    TRANSACTION
}
class LockItem {
    long id;
    LockItemType type;

    LockItem(long id, LockItemType type) {
        this.id = id;
        this.type = type;
    }

    long getId() {
        return id;
    }

    LockItemType getType() {
        return type;
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
}

public class WaitForGraph {



    private Digraph<LockItem> digraph;

    public WaitForGraph() {
        digraph = new Digraph<>();
    }

    public void addEdge(LockItem from, LockItem to) {
        digraph.addEdge(from ,to);
    }

    public void removeEdge(LockItem from, LockItem to) {
        digraph.removeEdge(from, to);
    }

    public boolean haveCircle() {
        return digraph.hasCircle();
    }
}
