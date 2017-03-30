package simpledb.utils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangke on 17-3-29.
 */

public class Digraph<Item> {
    private int e;
    private Map<Item, Set<Item>> adj;

    public Digraph() {
        this.e = 0;

        adj = new ConcurrentHashMap<>();
    }

    public int getVertexNum() {
        return adj.size();
    }

    public int getEdgeNum() {
        return e;
    }

    public Digraph<Item> reverse() {
        Digraph<Item> r = new Digraph<>();
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
            adj.put(from, new HashSet<Item>());
        }
        if (!adj.containsKey(to))
            adj.put(to, new HashSet<Item>());

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

    public void removeEdge(Item from, Item to) {
//        if (!haveEdge(from, to))
//            throw new Exception("no edge exist");

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
        Set<Item> visited = new HashSet<>();
        Set<Item> stack = new HashSet<>();
        for(Item key : adj.keySet()) {
            if(hasCircle(key, visited, stack)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasCircle(Item item, Set<Item> visited, Set<Item> stack) {
        for (Item i : stack) {
            System.out.printf(i + " ");
        }
        System.out.println();
        if(!visited.contains(item)) {
            visited.add(item);
            stack.add(item);

            Set<Item> tos = adj.get(item);
            for(Item to : tos) {
                if (!visited.contains(to) && hasCircle(to, visited, stack))
                    return true;
                else if (stack.contains(to))
                    return true;
            }
        }
        stack.remove(item);
        return false;
    }

    public static void main(String[] args) {
        Digraph<Integer> g = new Digraph<>();
        g.addEdge(0, 1);
        g.addEdge(0, 2);
        g.addEdge(1, 2);
        g.addEdge(2, 3);
        g.addEdge(2, 4);
        g.addEdge(4, 0);

        System.out.println("E: " + g.getEdgeNum());
        System.out.println("V: " + g.getVertexNum());
        System.out.println(g.hasCircle());
    }
}
