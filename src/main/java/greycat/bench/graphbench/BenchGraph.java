package greycat.bench.graphbench;

import greycat.Callback;

public interface BenchGraph {

    //insert
    void constructGraph(Callback<Boolean> callback);

    //read
    void sumOfChildren(int[] idsTolook, int time, Callback<int[]> callback);
    void buildStringOfNChildren(int[] idsTolook, int n, int time, Callback<String[]> callback);
}
