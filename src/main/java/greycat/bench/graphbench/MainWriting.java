package greycat.bench.graphbench;

import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;

import java.util.concurrent.CountDownLatch;

public class MainWriting {

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws InterruptedException {
        int memorySize;
        int[] percentOfModification;
        int[] nbSplit;
        int[] nbModification;
        int[] nbNodes;


        memorySize = 50000000;
        nbNodes = new int[]{100000, 1000000};
        percentOfModification = new int[]{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        nbSplit = new int[]{1, 2, 3, 4};
        nbModification = new int[]{5000, 5000, 500};

        for (int k = 0; k < nbSplit.length; k++) {
            for (int i = 0; i < nbNodes.length; i++) {
                for (int j = 0; j < percentOfModification.length; j++) {
                    final long startIteration = System.currentTimeMillis();
                    CountDownLatch countDownLatch = new CountDownLatch(1);
                    int startPosition = (100 - percentOfModification[j]) * nbNodes[i] / 100;

                    int saveEvery = (memorySize * 10 * nbSplit[k]) / (nbNodes[i] * percentOfModification[j] + 1) - 1;
                    GraphGenerator graphGenerator = new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[0], startPosition, 3);

                    RocksDBGraph rocksDBGraph = new RocksDBGraph("grey/grey_", memorySize, saveEvery, graphGenerator, "snap/");
                    rocksDBGraph.constructGraph(on -> countDownLatch.countDown());

                    countDownLatch.await();
                    final long time = System.currentTimeMillis() - startIteration;
                    System.out.println(graphGenerator.toString() + " : " + time + " ms");
                }

                for (int j = 0; j < percentOfModification.length; j++) {
                    final long startIteration = System.currentTimeMillis();
                    int startPosition = (100 - percentOfModification[j]) * nbNodes[i] / 100;
                    GraphGenerator graphGenerator = new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[0], startPosition, 3);

                    CountDownLatch countDownLatchInflux = new CountDownLatch(1);
                    InfluxGraph influx = new InfluxGraph("grey/grey_", memorySize, graphGenerator);
                    influx.constructGraph(result -> countDownLatchInflux.countDown());
                    countDownLatchInflux.await();

                    final long time = System.currentTimeMillis() - startIteration;
                    System.out.println(graphGenerator.toString() + " : " + time + " ms");
                }


            }
        }
    }

}
