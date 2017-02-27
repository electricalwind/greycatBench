package greycat.bench.graphbench;

import greycat.Callback;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.concurrent.CountDownLatch;

public class RocksDBGraph extends GreycatGraph {


    private final String _path;

    /**
     * @param pathToSave     where should the database be saved
     * @param memorySize     how many nodes should the cache contains
     * @param saveEvery
     * @param graphGenerator generator of graph
     */
    public RocksDBGraph(String pathToSave, int memorySize, int saveEvery, GraphGenerator graphGenerator, String pathToSnapshot) {
        super(pathToSave, memorySize, saveEvery, graphGenerator);
        RocksDB.loadLibrary();
        this._path = pathToSnapshot + graphGenerator.toString();

        File location = new File(_path+"/current");
        if (!location.exists()) {
            location.mkdirs();
        }
        File targetDB = new File(location, "data");
        targetDB.mkdirs();
    }

    public String get_path() {
        return _path;
    }

    @Override
    public void constructGraph(Callback<Boolean> callback) {
        try {
            Options options = new Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION);
            RocksDB db = RocksDB.open(options, _path + "/current");

        internal_ConstructGraph(db, new Callback<Boolean>() {
            @Override
            public void on(Boolean result) {
                db.close();
                callback.on(result);
            }
        });
        }catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void connectToClosestTime(int time){

    }

    @Override
    public void sumOfChildren(int id, int time, Callback<Integer> callback) {

    }

    @Override
    public void buildStringOfNChildren(int id, int n, int time, Callback<String> callback) {

    }

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws InterruptedException {

        //Parameters
        int memorySize;
        int[] percentOfModification;
        int[] nbSplit;
        int[] nbModification;
        int[] nbNodes;

        boolean test = true;
        if (test) { // laptop
            memorySize = 1000000;
            nbNodes = new int[]{1000};
            percentOfModification = new int[]{10};
            nbSplit = new int[]{1};
            nbModification = new int[]{1000};
        } else { //server
            memorySize = 100000000;
            nbNodes = new int[]{10000, 100000, 1000000};
            percentOfModification = new int[]{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
            nbSplit = new int[]{1, 2, 3, 4};
            nbModification = new int[]{10000, 10000, 1000};
        }

        for (int k = 0; k < nbSplit.length; k++) {
            for (int i = 0; i < nbNodes.length; i++) {
                for (int j = 0; j < percentOfModification.length; j++) {

                    int startPosition = (100 - percentOfModification[j]) * nbNodes[i] / 100;

                    int saveEvery = (memorySize * 10 * nbSplit[k]) / (nbNodes[i] * percentOfModification[j] + 1) - 1;

                    if (percentOfModification[j] == 0 && k != 0) break;
                    CountDownLatch countDownLatch = new CountDownLatch(1);
                    RocksDBGraph grey = new RocksDBGraph(
                            "grey/grey_", memorySize, saveEvery,
                            new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[i], startPosition, 3),"snap/");

                    grey.constructGraph(result -> countDownLatch.countDown());
                    /**grey.sumOfChildren(11, 1500, new Callback<Integer>() {
                        @Override
                        public void on(Integer integer) {

                            countDownLatch.countDown();
                            System.out.println(integer);
                        }
                    });
                    grey.buildStringOfNChildren(11, 5, 1500, new Callback<String>() {
                        @Override
                        public void on(String s) {

                            countDownLatch.countDown();
                            System.out.println(s);
                        }
                    });*/
                    countDownLatch.await();
                }
            }
        }
    }


}
