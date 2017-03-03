package greycat.bench.graphbench;

import greycat.bench.graphbench.GreycatGraph;
import greycat.bench.graphbench.InfluxGraph;
import greycat.bench.graphbench.RocksDBGraph;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;

import java.util.Random;

public class MainBenching {


    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        int memorySize;
        int[] percentOfModification;
        int[] nbSplit;
        int[] nbModification;
        int[] nbNodes;


        memorySize = 100000000;
        nbNodes = new int[]{10000, 100000, 1000000};
        percentOfModification = new int[]{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        nbSplit = new int[]{1, 2, 3, 4};
        nbModification = new int[]{10000, 10000, 1000};

        for (int k = 0; k < nbSplit.length; k++) {
            for (int i = 0; i < nbNodes.length; i++) {
                for (int j = 0; j < percentOfModification.length; j++) {
                    int startPosition = (100 - percentOfModification[j]) * nbNodes[i] / 100;

                    int saveEvery = (memorySize * 10 * nbSplit[k]) / (nbNodes[i] * percentOfModification[j] + 1) - 1;
                    GraphGenerator graphGenerator = new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[0], startPosition, 3);


                    int[] nodeIds = new Random().ints(graphGenerator.getOffset(), nbNodes[i] - 1 + graphGenerator.getOffset()).limit(1000).toArray();
                    int[] numberOfLoadS = new int[1000];
                    for (int index = 0; index < 1000; index++) {
                        numberOfLoadS[index] = numberOfNodeLoaded(nodeIds[index], nbNodes[i]);
                    }
                    int nbInsertMax = nbNodes[i] * 6 / 10;
                    int[] time = new Random().ints(nbInsertMax, nbInsertMax + (nbModification[0] * nbSplit[k] * 6)).limit(1000).toArray();

                    int[] numberOfLoadC = new int[1000];
                    int[] nchild = new Random().ints(0, 9).limit(1000).toArray();
                    for (int index = 0; index < 1000; index++) {
                        numberOfLoadC[index] = numberOfNodeString(nodeIds[index], nchild[index], nbNodes[i]);
                    }

                    RocksDBGraph rocksDBGraph = new RocksDBGraph("grey/grey_", memorySize, saveEvery, graphGenerator, "snap/");
                    GreycatGraph greycatGraph = new GreycatGraph("grey/grey_", memorySize, saveEvery, graphGenerator);
                    InfluxGraph influxGraph = new InfluxGraph("grey/grey_", memorySize, graphGenerator);

                    long[] influxC, greycatC, rocksC = new long[1000];
                    long[] influxS, greycatS, rocksS = new long[1000];

                    for (int timeId = 0; timeId < 1000; timeId++) {

                     /**   long timeBeforeGS = System.currentTimeMillis();
                        long timeAfterGS;
                        greycatGraph.buildStringOfNChildren(nodeIds, nchild[timeId], time[timeId], callback ->);


                        long timeBeforeGC = System.currentTimeMillis();
                        long timeAfterGC;
                        greycatGraph.sumOfChildren(nodeIds, time[timeId], callback ->);


                        long timeBeforeIS = System.currentTimeMillis();
                        long timeAfterIS;
                        influxGraph.buildStringOfNChildren(nodeIds, nchild[timeId], time[timeId], callback ->);


                        long timeBeforeIC = System.currentTimeMillis();
                        long timeAfterIC;
                        influxGraph.sumOfChildren(nodeIds, time[timeId], callback ->);


                        long timeBeforeRS = System.currentTimeMillis();
                        long timeAfterRS;
                        rocksDBGraph.buildStringOfNChildren(nodeIds, nchild[timeId], time[timeId], callback ->);


                        long timeBeforeRC = System.currentTimeMillis();
                        long timeAfterRC;
                        rocksDBGraph.sumOfChildren(nodeIds, time[timeId], callback ->);*/

                    }


                }
            }
        }
    }

    private static int numberOfNodeString(int nodeStart, int childNumber, int numberOfNode) {
        int current = nodeStart;
        int result = 0;
        while (current < numberOfNode) {
            result++;
            current = (current + 1) * 10 + childNumber;
        }
        return result;
    }

    private static int numberOfNodeLoaded(int nodeStart, int numberOfNode) {
        int current = nodeStart;
        int result = 1;
        int power = 0;
        while (current < numberOfNode) {
            int supposedResult = power * 10;
            if (current + supposedResult >= numberOfNode) {
                result += (numberOfNode - current);
            } else {
                result += supposedResult;
            }
            current = (current + 1) * 10;
            power++;
        }
        return result;
    }

}
