package greycat.bench.graphbench;

import greycat.Callback;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;
import greycat.internal.heap.HeapBuffer;
import greycat.struct.Buffer;
import greycat.utility.Base64;
import greycat.utility.HashHelper;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static greycat.bench.BenchConstants.*;

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

        File location = new File(_path + "/current");
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

            internal_ConstructGraph(db, result -> {
                db.close();
                callback.on(result);
            });
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public int findClosestTime(int time) {
        File[] directories = new File(_path).listFiles(File::isDirectory);
        if (directories.length > 0) {
            int timeNearest = -1;
            for (int i = 0; i < directories.length; i++) {
                if (directories[i].getName().compareTo("current") != 0) {
                    int newtime = Integer.parseInt(directories[i].getName());
                    if (newtime == time)
                        return newtime;
                    if (newtime < time && newtime - timeNearest > 0) {
                        timeNearest = newtime;
                    }
                }
            }
            return timeNearest;
        } else return -1;
    }


    @SuppressWarnings("Duplicates")
    @Override
    public void sumOfChildren(int[] idsToLook, int time, Callback<int[]> callback) {
        RocksDB db = null;
        int children = HashHelper.hash(NODE_CHILDREN);
        int value = HashHelper.hash(NODE_VALUE);
        int[] sums = new int[idsToLook.length];
        try {
            db = RocksDB.openReadOnly(_path + "/" + findClosestTime(time));

            for (int k = 0; k < idsToLook.length; k++) {
                int sum = 0;
                List<Integer> ids = new ArrayList<>();
                ids.add(idsToLook[k]);
                while (ids.size() != 0) {
                    StateChunckWrapper[] stw = loadNodes(db, ids);
                    ids.clear();
                    for (int i = 0; i < stw.length; i++) {
                        StateChunckWrapper node = stw[i];
                        int indexOfValue = getArrayIndex(node.getKeys(), value);
                        int indexOfChildren = getArrayIndex(node.getKeys(), children);
                        if (indexOfChildren != -1) {
                            long[] childrenArray = (long[]) node.getValues()[indexOfChildren];
                            for (int j = 0; j < childrenArray.length; j++) {
                                ids.add((int) childrenArray[j]);
                            }
                        }
                        sum += (int) node.getValues()[indexOfValue];
                    }
                }
                sums[k] = sum;
            }
            db.close();
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            callback.on(sums);
        }
    }


    private StateChunckWrapper[] loadNodes(RocksDB rocksDB, List<Integer> keys) {
        List<byte[]> listKeys = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            Buffer buffer = new HeapBuffer();
            Base64.encodeIntToBuffer(keys.get(i), buffer);

            listKeys.add(buffer.data());
        }

        StateChunckWrapper[] nodes = new StateChunckWrapper[keys.size()];

        try {
            final int[] i = {0};
            Map<byte[], byte[]> mapOfResul = rocksDB.multiGet(listKeys);
            mapOfResul.forEach((k, v) -> {
                        Buffer buffer = new HeapBuffer();
                        buffer.writeAll(k);
                        int id = Base64.decodeToIntWithBounds(buffer, 0, k.length);
                        buffer.free();
                        buffer.writeAll(v);
                        nodes[i[0]] = new StateChunckWrapper(id, buffer);
                        i[0]++;
                    }
            );
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {

            return nodes;
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void buildStringOfNChildren(int[] idsToLook, int n, int time, Callback<String[]> callback) {
        RocksDB db = null;
        int children = HashHelper.hash(NODE_CHILDREN);
        int rc = HashHelper.hash(NODE_RANDOM_CHAR);
        String[] sequences = new String[idsToLook.length];
        try {
            db = RocksDB.openReadOnly(_path + "/" + findClosestTime(time));
            for (int k = 0; k < idsToLook.length; k++) {
                String sequence = "";
                List<Integer> ids = new ArrayList<>();
                ids.add(idsToLook[k]);
                while (ids.size() != 0) {
                    StateChunckWrapper[] stw = loadNodes(db, ids);
                    ids.clear();
                    for (int i = 0; i < stw.length; i++) {
                        StateChunckWrapper node = stw[i];
                        int indexOfRC = getArrayIndex(node.getKeys(), rc);
                        int indexOfChildren = getArrayIndex(node.getKeys(), children);
                        if (indexOfChildren != -1) {
                            long[] childrenArray = (long[]) node.getValues()[indexOfChildren];
                            if (childrenArray.length > n)
                                ids.add((int) childrenArray[n]);
                        }
                        sequence += (String) node.getValues()[indexOfRC];
                    }
                }
                sequences[k] = sequence;
            }
            db.close();
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            callback.on(sequences);
        }
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
                    CountDownLatch countDownLatch = new CountDownLatch(2);
                    RocksDBGraph grey = new RocksDBGraph(
                            "grey/grey_", memorySize, saveEvery,
                            new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[i], startPosition, 3), "snap/");

                    // grey.constructGraph(result -> countDownLatch.countDown());
                    //grey.findClosestTime(1500);
                    //countDownLatch.countDown();
                    grey.sumOfChildren(new int[]{11,10}, 1500, integer -> {

                        countDownLatch.countDown();
                        System.out.println(Arrays.toString(integer));
                    });
                    grey.buildStringOfNChildren(new int[]{11,10}, 5, 1500, s -> {

                        countDownLatch.countDown();
                        System.out.println(Arrays.toString(s));
                    });
                    countDownLatch.await();
                }
            }
        }
    }

    public int getArrayIndex(int[] arr, int value) {

        int k = -1;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == value) {
                k = i;
                break;
            }
        }
        return k;
    }

}


/**
 * Buffer buffer = new HeapBuffer();
 * Base64.encodeIntToBuffer(id, buffer);
 * byte[] result = db.get(buffer.data());
 * buffer.free();
 * buffer.writeAll(result);
 * StateChunckWrapper stateChunckWrapper = new StateChunckWrapper(id, buffer);
 * int i = 0;
 */
