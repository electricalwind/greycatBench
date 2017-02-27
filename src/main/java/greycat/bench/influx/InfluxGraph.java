package greycat.bench.influx;

import greycat.*;
import greycat.bench.BenchGraph;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;
import greycat.rocksdb.RocksDBStorage;
import greycat.scheduler.TrampolineScheduler;
import greycat.struct.Relation;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static greycat.Constants.END_OF_TIME;
import static greycat.Tasks.newTask;

public class InfluxGraph implements BenchGraph {

    protected final Graph _graph;
    private final GraphGenerator _gGen;

    public InfluxGraph(String pathToLoad, int memorySize, GraphGenerator graphGenerator) {
        this._graph = new GraphBuilder()
                .withStorage(new RocksDBStorage(pathToLoad + graphGenerator.toString()))
                .withMemorySize(memorySize)
                .withScheduler(new TrampolineScheduler())
                .build();
        this._gGen = graphGenerator;
    }

    public void constructGraph(Callback<Boolean> callback) {

        InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
        influxDB.deleteDatabase(_gGen.toString());

        final long timeStart = System.currentTimeMillis();

        int start = _gGen.getOffset();
        int end = _gGen.getOffset() + _gGen.get_nbNodes() - 1;

        influxDB.createDatabase(_gGen.toString());

        _graph.connect(connected ->
                newTask()
                        .travelInTime(String.valueOf(_gGen.get_nbNodes() * 6 / 10))
                        .loopPar(
                                String.valueOf(start), String.valueOf(end),
                                newTask()
                                        .lookup("{{i}}")
                                        .thenDo(ctx ->
                                        {
                                            Node node = ctx.resultAsNodes().get(0);
                                            int nodeId = ctx.intVar("i");

                                            node.timepoints(0, END_OF_TIME,
                                                    timepoints -> {
                                                        int size = timepoints.length;

                                                        long[] ids = new long[size];
                                                        Arrays.fill(ids, nodeId);

                                                        long[] world = new long[size];
                                                        Arrays.fill(world, 0);

                                                        node.free();

                                                        ctx.graph().lookupBatch(world, timepoints, ids, nodes -> {
                                                            BatchPoints batchPoints = BatchPoints
                                                                    .database(_gGen.toString())
                                                                    .retentionPolicy("autogen")
                                                                    .consistency(InfluxDB.ConsistencyLevel.ALL)
                                                                    .build();

                                                            for (int j = nodes.length - 1; j >= 0; j--) {
                                                                Node node1 = nodes[j];
                                                                Object father = node1.get("father");
                                                                Object children = node1.get("children");
                                                                Point point;
                                                                if (father != null) {
                                                                    if (children != null) {
                                                                        Relation relationFather = (Relation) father;
                                                                        Relation relationChildren = (Relation) children;
                                                                        point = Point.measurement(String.valueOf(nodeId))
                                                                                .time(node1.time(), TimeUnit.SECONDS)
                                                                                .addField("value", (int) node1.get("value"))
                                                                                .addField("rc", (String) node1.get("rc"))
                                                                                .addField("children", Arrays.toString(relationChildren.all()))
                                                                                .addField("father", relationFather.get(0))
                                                                                .build();
                                                                    } else {
                                                                        Relation relationFather = (Relation) father;
                                                                        point = Point.measurement(String.valueOf(nodeId))
                                                                                .time(node1.time(), TimeUnit.SECONDS)
                                                                                .addField("value", (int) node1.get("value"))
                                                                                .addField("rc", (String) node1.get("rc"))
                                                                                .addField("children", "[]")
                                                                                .addField("father", relationFather.get(0))
                                                                                .build();
                                                                    }
                                                                } else {
                                                                    if (children != null) {
                                                                        Relation relationChildren = (Relation) children;
                                                                        point = Point.measurement(String.valueOf(nodeId))
                                                                                .time(node1.time(), TimeUnit.SECONDS)
                                                                                .addField("value", (int) node1.get("value"))
                                                                                .addField("rc", (String) node1.get("rc"))
                                                                                .addField("children", Arrays.toString(relationChildren.all()))
                                                                                .addField("father", -1L)
                                                                                .build();
                                                                    } else {
                                                                        point = Point.measurement(String.valueOf(nodeId))
                                                                                .time(node1.time(), TimeUnit.SECONDS)
                                                                                .addField("value", (int) node1.get("value"))
                                                                                .addField("rc", (String) node1.get("rc"))
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
                                                        });
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
                        })
        );
    }

    @Override
    public void sumOfChildren(int id, int time, Callback<Integer> callback) {
        InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
        String db = _gGen.toString();
        List<Integer> listOfId = new ArrayList<>();
        listOfId.add(id);
        double sum = 0;
        while (listOfId.size() != 0) {
            String ids = listOfId.stream().map(Object::toString).collect(Collectors.joining("\",\"", "\"", "\""));
            String squery = "SELECT value,children FROM " + ids + " WHERE time<=" + time + "s ORDER BY time DESC LIMIT 1";
            Query query = new Query(squery, db);
            QueryResult result = influxDB.query(query);
            List<QueryResult.Result> resultList = result.getResults();
            listOfId.clear();
            if (resultList.size() != 0) {
                List<QueryResult.Series> series = resultList.get(0).getSeries();
                for (int i = 0; i < series.size(); i++) {
                    List<Object> lo = series.get(i).getValues().get(0);
                    sum += (double) lo.get(1);
                    String childrens = (String) lo.get(2);
                    if (childrens.compareTo("[]") != 0) {
                        String[] childs = childrens.substring(1, childrens.length() - 1).split(",");
                        for (int child = 0; child < childs.length; child++) {
                            listOfId.add(Integer.valueOf(childs[child].trim()));
                        }
                    }
                }
            }

        }
        callback.on((int) sum);
    }

    @Override
    public void buildStringOfNChildren(int id, int n, int time, Callback<String> callback) {
        if (n < 0 || n > 9) throw new RuntimeException("n must be between 0 and 9");
        InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
        String db = _gGen.toString();
        int toLook = id;
        String sequence = "";
        while (toLook != -1) {
            String squery = "SELECT rc,children FROM \"" + toLook + "\" WHERE time<=" + time + "s ORDER BY time DESC LIMIT 1";
            Query query = new Query(squery, db);
            QueryResult result = influxDB.query(query);
            List<QueryResult.Result> resultList = result.getResults();
            toLook = -1;
            if (resultList.size() != 0) {
                QueryResult.Series serie = resultList.get(0).getSeries().get(0);
                List<Object> lo = serie.getValues().get(0);
                sequence += (String) lo.get(1);
                String childrens = (String) lo.get(2);
                if (childrens.compareTo("[]") != 0) {
                    String[] childs = childrens.substring(1, childrens.length() - 1).split(",");
                    if (childs.length > n) {
                        toLook = Integer.valueOf(childs[n].trim());
                    }
                }

            }

        }
        callback.on(sequence);


    }

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
                    CountDownLatch loginLatch = new CountDownLatch(1);
                    InfluxGraph influx = new InfluxGraph("grey/grey_", memorySize, new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[0], 0, 3));
                    influx.constructGraph(new Callback<Boolean>() {
                    @Override public void on(Boolean result) {
                    loginLatch.countDown();
                    }
                    });
                    influx.sumOfChildren(11, 1500, new Callback<Integer>() {
                        @Override
                        public void on(Integer integer) {
                            loginLatch.countDown();
                            System.out.println(integer);
                        }
                    });
                    influx.buildStringOfNChildren(11, 5, 1500, new Callback<String>() {
                        @Override
                        public void on(String s) {
                            loginLatch.countDown();
                            System.out.println(s);
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