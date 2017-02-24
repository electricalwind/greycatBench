package greycat.bench.influx;

import greycat.*;
import greycat.bench.graphgen.GraphGenerator;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.rocksdb.RocksDBStorage;
import greycat.scheduler.HybridScheduler;
import greycat.struct.Relation;
import greycat.struct.RelationIndexed;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static greycat.Constants.END_OF_TIME;
import static greycat.Tasks.newTask;

public class InfluxGraph {

    protected final Graph _graph;
    private final GraphGenerator _gGen;

    public InfluxGraph(String pathToLoad, int memorySize, GraphGenerator graphGenerator) {
        this._graph = new GraphBuilder()
                .withStorage(new RocksDBStorage(pathToLoad + graphGenerator.toString()))
                .withMemorySize(memorySize)
                .withScheduler(new HybridScheduler())
                .build();
        this._gGen = graphGenerator;
    }

    public void creatingGraph(Callback<Boolean> callback) {
        final long timeStart = System.currentTimeMillis();
        int start = _gGen.getOffset();
        int end = _gGen.getOffset() + _gGen.get_nbNodes() - 1;
        InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
        influxDB.deleteDatabase(_gGen.toString());
        influxDB.createDatabase(_gGen.toString());
        _graph.connect(
                new Callback<Boolean>() {
                    @Override
                    public void on(Boolean result) {
                        newTask()
                                .travelInTime(String.valueOf(_gGen.get_nbNodes() / 10))
                                .loopPar(
                                        String.valueOf(start), String.valueOf(end),
                                        newTask()
                                                .lookup("{{i}}")
                                                .thenDo(ctx -> {
                                                    Node node = ctx.resultAsNodes().get(0);
                                                    int i = ctx.intVar("i");
                                                    node.timepoints(0, END_OF_TIME, new Callback<long[]>() {
                                                        @Override
                                                        public void on(long[] result) {
                                                            int size = result.length;
                                                            long[] ids = new long[size];
                                                            Arrays.fill(ids, i);
                                                            long[] world = new long[size];
                                                            Arrays.fill(world, 0);
                                                            node.free();
                                                            ctx.graph().lookupBatch(world, result, ids, new Callback<Node[]>() {
                                                                @Override
                                                                public void on(Node[] result) {
                                                                    BatchPoints batchPoints = BatchPoints
                                                                            .database(_gGen.toString())
                                                                            .tag("async", "true")
                                                                            .retentionPolicy("autogen")
                                                                            .consistency(InfluxDB.ConsistencyLevel.ALL)
                                                                            .build();

                                                                    for (int j = result.length - 1; j >= 0; j--) {
                                                                        Node node1 = result[j];
                                                                        Object father = node1.get("father");
                                                                        Object children = node1.get("children");
                                                                        Point point;
                                                                        if (father != null) {
                                                                            if (children != null) {
                                                                                Relation relationFather = (Relation) father;
                                                                                RelationIndexed relationChildren = (RelationIndexed) children;
                                                                                point = Point.measurement(String.valueOf(i))
                                                                                        .time(node1.time(), TimeUnit.SECONDS)
                                                                                        .addField("value", (int) node1.get("value"))
                                                                                        .addField("rc",(String) node.get("rc"))
                                                                                        .addField("children", Arrays.toString(relationChildren.all()))
                                                                                        .addField("father", relationFather.get(0))
                                                                                        .build();
                                                                            } else {
                                                                                Relation relationFather = (Relation) father;
                                                                                point = Point.measurement(String.valueOf(i))
                                                                                        .time(node1.time(), TimeUnit.SECONDS)
                                                                                        .addField("value", (int) node1.get("value"))
                                                                                        .addField("rc",(String) node.get("rc"))
                                                                                        .addField("children", "[]")
                                                                                        .addField("father", relationFather.get(0))
                                                                                        .build();
                                                                            }
                                                                        } else {
                                                                            if (children != null) {
                                                                                RelationIndexed relationChildren = (RelationIndexed) children;
                                                                                point = Point.measurement(String.valueOf(i))
                                                                                        .time(node1.time(), TimeUnit.SECONDS)
                                                                                        .addField("value", (int) node1.get("value"))
                                                                                        .addField("rc",(String) node.get("rc"))
                                                                                        .addField("children", Arrays.toString(relationChildren.all()))
                                                                                        .addField("father", -1L)
                                                                                        .build();
                                                                            } else {
                                                                                point = Point.measurement(String.valueOf(i))
                                                                                        .time(node1.time(), TimeUnit.SECONDS)
                                                                                        .addField("value", (int) node1.get("value"))
                                                                                        .addField("rc",(String) node.get("rc"))
                                                                                        .addField("children", "[]")
                                                                                        .addField("father", -1L)
                                                                                        .build();
                                                                            }
                                                                        }
                                                                        batchPoints.point(point);
                                                                        node1.free();
                                                                    }
                                                                    influxDB.write(batchPoints);
                                                                    ctx.continueTask();
                                                                }
                                                            });
                                                        }
                                                    });
                                                })
                                )
                                .execute(_graph, new Callback<TaskResult>() {
                                    @Override
                                    public void on(TaskResult result) {
                                        influxDB.close();
                                        if (result.exception() != null)
                                            result.exception().printStackTrace();
                                        final long timeEnd = System.currentTimeMillis();
                                        final long timetoProcess = timeEnd - timeStart;
                                        System.out.println(_gGen.toString() + " " + timetoProcess + " ms");

                                        _graph.disconnect(new Callback<Boolean>() {
                                            @Override
                                            public void on(Boolean result) {
                                                callback.on(result);
                                            }
                                        });
                                    }
                                });

                    }
                }
        );
    }


    public static void main(String[] args) throws InterruptedException {
        InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
        int memorySize = 1000000;//00
        int[] nbNodes = {10000};//0, 100000, 1000000};
        int[] percentOfModification = {10};//{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        int[] nbSplit = {1};//{1, 2, 3, 4};
        int[] nbModification = {10000};//{10000,10000,1000};
        for (int k = 0; k < nbSplit.length; k++) {
            for (int i = 0; i < nbNodes.length; i++) {
                for (int j = 0; j < percentOfModification.length; j++) {
                    CountDownLatch loginLatch = new CountDownLatch(1);
                    InfluxGraph influx = new InfluxGraph("grey/grey_", memorySize, new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[0], 0, 3));
                    influx.creatingGraph(new Callback<Boolean>() {
                        @Override
                        public void on(Boolean result) {
                            loginLatch.countDown();
                        }
                    });
                    loginLatch.await();
                }
            }
        }

    }
}


/**
 * DeferCounter dc = ctx.graph().newCounter(size);
 * // ((HeapChunkSpace) ctx.graph().space()).printMarked();
 * //System.out.println("\n");
 * for (int j = result.length-1; j >=0; j--) {
 * ctx.graph().lookup(world[j], result[j], ids[j],
 * new Callback<Node>() {
 *
 * @Override public void on(Node result) {
 * //((HeapChunkSpace) ctx.graph().space()).printMarked();
 * //System.out.println("\n");
 * BatchPoints batchPoints = BatchPoints
 * .database(_gGen.toString())
 * .tag("async", "true")
 * .retentionPolicy("autogen")
 * .consistency(InfluxDB.ConsistencyLevel.ALL)
 * .build();
 * Object father = result.get("father");
 * Object children = result.get("children");
 * Point point;
 * if (father != null) {
 * if (children != null) {
 * Relation relationFather = (Relation) father;
 * RelationIndexed relationChildren = (RelationIndexed) children;
 * point = Point.measurement(String.valueOf(i))
 * .time(result.time(), TimeUnit.SECONDS)
 * .addField("value", (int) result.get("value"))
 * .addField("children", Arrays.toString(relationChildren.all()))
 * .addField("father", relationFather.get(0))
 * .build();
 * } else {
 * Relation relationFather = (Relation) father;
 * point = Point.measurement(String.valueOf(i))
 * .time(result.time(), TimeUnit.SECONDS)
 * .addField("value", (int) result.get("value"))
 * .addField("father", relationFather.get(0))
 * .build();
 * }
 * } else {
 * if (children != null) {
 * RelationIndexed relationChildren = (RelationIndexed) children;
 * point = Point.measurement(String.valueOf(i))
 * .time(result.time(), TimeUnit.SECONDS)
 * .addField("value", (int) result.get("value"))
 * .addField("children", Arrays.toString(relationChildren.all()))
 * .addField("father", -1L)
 * .build();
 * } else {
 * point = Point.measurement(String.valueOf(i))
 * .time(result.time(), TimeUnit.SECONDS)
 * .addField("value", (int) result.get("value"))
 * .addField("father", -1L)
 * .build();
 * }
 * }
 * batchPoints.point(point);
 * result.free();
 *
 * influxDB.write(batchPoints);
 * dc.count();
 * }
 * });
 * }
 * dc.then(new Job() {
 * @Override public void run() {
 * ctx.continueTask();
 * }
 * });
 * }
 * });
 * })
 */