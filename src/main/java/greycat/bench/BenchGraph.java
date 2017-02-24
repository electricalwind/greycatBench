package greycat.bench;

import greycat.Callback;

public interface BenchGraph {

    //insert
    void constructGraph(Callback<Boolean> callback);

    //read
    void sumOfChildren(int id, int time, Callback<Integer> callback);
    void buildStringOfNChildren(int id, int n, int time, Callback<String> callback);
}
