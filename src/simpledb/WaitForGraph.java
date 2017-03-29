package simpledb;

import java.util.Set;

import org.apache.mina.util.ConcurrentHashSet;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangke on 17-3-29.
 */

public class WaitForGraph<Item> {
    private int v;
    private int e;
    private ConcurrentHashMap<Item, ConcurrentHashSet<Item>> adj;

    public WaitForGraph() {
        this.e = 0;

        adj = new ConcurrentHashMap<>();
    }

    public int getVertexNum() {
        return adj.size();
    }

    public int getEdgeNum() {
        return e;
    }

    public WaitForGraph<Item> reverse() {
        WaitForGraph<Item> r = new WaitForGraph<>();
        for (Item from : adj.keySet()) {
            Set<Item> tos = adj.get(from);
            for (Item to : tos) {
                r.addEdge(to, from);
            }
        }
        return r;
    }

    public void addEdge(Item from, Item to) {
        if (!adj.containsKey(from)) {
            adj.put(from, new ConcurrentHashSet<>());
        }
        if (!adj.containsKey(to))
            adj.put(to, new ConcurrentHashSet<>());

        adj.get(from).add(to);
        ++e;
    }

    public boolean haveVetex(Item item) {
        return adj.containsKey(item);
    }

    private boolean haveTo(Item from) {
        return haveVetex(from) && adj.get(from).size() > 0;
    }

    private boolean haveFrom(Item to) {
        return reverse().haveTo(to);
    }

    private boolean haveEdge(Item from, Item to) {
        return haveTo(from) && haveVetex(to) &&
                adj.get(from).contains(to);
    }

    public void removeEdge(Item from, Item to) throws Exception {
        if (!haveEdge(from, to))
            throw new Exception("no edge exist");

        Set<Item> tos = adj.get(from);
        tos.remove(to);
        --e;

        if (tos.size() == 0 && !haveFrom(from)) {
            adj.remove(from);
        }

        if (!haveFrom(to) && !haveTo(to)) {
            adj.remove(to);
        }
    }

    public boolean hasCircle() {
        return false;
    }
}
