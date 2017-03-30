package simpledb;

import simpledb.utils.Digraph;

public class WaitForGraph {

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

        @Override
        public int hashCode() {
            return (int)(id * 31 +  type.hashCode());
        }
    }

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
