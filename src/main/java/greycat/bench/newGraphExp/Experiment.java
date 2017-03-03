package greycat.bench.newGraphExp;

import greycat.*;
import greycat.bench.graphbench.StateChunckWrapper;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;
import greycat.bench.graphgen.Operations;
import greycat.chunk.StateChunk;
import greycat.internal.heap.HeapBuffer;
import greycat.plugin.SchedulerAffinity;
import greycat.rocksdb.RocksDBStorage;
import greycat.scheduler.TrampolineScheduler;
import greycat.struct.Buffer;
import greycat.struct.BufferIterator;
import greycat.struct.Relation;
import greycat.utility.Base64;
import greycat.utility.HashHelper;
import org.apache.commons.io.FileUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;
import org.rocksdb.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static greycat.Tasks.newTask;
import static greycat.bench.BenchConstants.*;
import static mylittleplugin.MyLittleActions.ifEmptyThen;
import static mylittleplugin.MyLittleActions.ifNotEmptyThen;

public class Experiment {


    private final double[] greyCatWrite;
    private final long[] greyCatStorage;

    private final long[] InfluxStorage;
    private final double[] InfluxWrite;
    private final Graph _graph;
    private final long[] RocksStorage;
    private final double[] RocksWrite;
    private final GraphGenerator _gGen;

    public final static String adaptedTimeVar = "realTime";
    public final static String fatherVar = "father";
    private static final String ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private final String _pathToRocks;
    private final String _influxAddress;
    private final String _pathToGrey;
    private final double[] RocksReadS;
    private final double[] greyCatReadS;
    private final double[] greyCatReadC;
    private final double[] RocksReadC;
    private final double[] InfluxReadC;
    private final double[] InfluxReadS;
    //private final int _saveEvery;

    public Experiment(int history, String pathToGrey, String pathToRocks, String influxAddress, int memorySize, GraphGenerator graphGenerator) {

        int fullHistory = history + graphGenerator.get_nbInsert() + 1;

        this.greyCatReadC = new double[fullHistory];
        this.greyCatReadS = new double[fullHistory];
        this.greyCatWrite = new double[fullHistory];
        this.greyCatStorage = new long[fullHistory];

        this.InfluxReadC = new double[fullHistory];
        this.InfluxReadS = new double[fullHistory];
        this.InfluxWrite = new double[fullHistory];
        this.InfluxStorage = new long[fullHistory];

        this.RocksReadC = new double[fullHistory];
        this.RocksReadS = new double[fullHistory];
        this.RocksWrite = new double[fullHistory];
        this.RocksStorage = new long[fullHistory];

        this._graph = new GraphBuilder()
                .withStorage(new RocksDBStorage(pathToGrey + graphGenerator.toString()))
                .withMemorySize(memorySize)
                .withScheduler(new TrampolineScheduler())
                .build();

        this._gGen = graphGenerator;
        this._influxAddress = influxAddress;

        this._pathToGrey = pathToGrey;
        this._pathToRocks = pathToRocks + graphGenerator.toString();

        File location = new File(_pathToRocks + "/current");
        if (!location.exists()) {
            location.mkdirs();
        }
        File targetDB = new File(location, "data");
        targetDB.mkdirs();
        //this._saveEvery = (memorySize * 10 *_gGen.get_nbSplit() ) / (_gGen.get_nbNodes() * _gGen.get_percentageOfModification() + 1) - 1;

    }


    @SuppressWarnings("Duplicates")
    public void constructGraph(Callback<Boolean> on) {
        final String graphGenTimeVar = "time";
        final String operationVar = "operation";
        final String valueVar = "value";
        final String nodesIdVar = "nodesId";
        final String nodeIdVar = "nodeId";
        final String fatherIDVar = "fatherId";
        final String nodeVar = "node";
        final String charVar = "character";

        try {
            Options options = new Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION);
            RocksDB db = RocksDB.open(options, _pathToRocks + "/current");
            InfluxDB influxDB = InfluxDBFactory.connect(_influxAddress);
            influxDB.deleteDatabase(_gGen.toString());
            influxDB.createDatabase(_gGen.toString());
            influxDB.close();
            _graph.connect(
                    result -> newTask()
                            .thenDo(ctx -> {
                                ctx.setVariable(graphGenTimeVar, 0);
                                ctx.setVariable(adaptedTimeVar, 0);
                                ctx.setVariable(valueVar, 0);
                                ctx.setVariable(operationVar, _gGen.nextTimeStamp());
                                ctx.continueTask();
                            })

                            .readGlobalIndex(ENTRY_POINT_INDEX)

                            .then(ifEmptyThen(
                                    newTask()
                                            //Dealing with Greycat initialization
                                            .createNode()
                                            .setAttribute(NODE_ID, Type.INT, "-1")
                                            .addToGlobalIndex(ENTRY_POINT_INDEX, NODE_ID)

                                            //Starting Graph Insert
                                            .whileDo(ctx -> ctx.variable(operationVar).get(0) != null,
                                                    newTask()
                                                            //Operation Object handling
                                                            .thenDo(ctx -> {
                                                                ctx.setTime(ctx.intVar(adaptedTimeVar));
                                                                Operations op = (Operations) ctx.variable(operationVar).get(0);
                                                                ctx.setVariable(nodesIdVar, op.get_arrayOfNodes());
                                                                ctx.continueWith(ctx.wrap(op.is_insert()));
                                                            })

                                                            //snapshot
                                                            .declareVar(fatherVar)
                                                            .ifThenElse(ctx -> (boolean) ctx.result().get(0),
                                                                    //If Insert
                                                                    newTask()
                                                                            .readVar(nodesIdVar)
                                                                            .map(newTask()
                                                                                    .ifThen(ctx -> (int) ctx.result().get(0) != -1,
                                                                                            newTask()
                                                                                                    .setAsVar(nodeIdVar)
                                                                                                    .thenDo(ctx -> ctx.continueWith(ctx.wrap(((ctx.intVar(nodeIdVar) - _gGen.getOffset()) / 10) - 1 + _gGen.getOffset())))
                                                                                                    .setAsVar(fatherIDVar)
                                                                                                    .createNode()
                                                                                                    .setAttribute(NODE_ID, Type.INT, "{{" + nodeIdVar + "}}")
                                                                                                    .setAttribute(NODE_VALUE, Type.INT, "{{" + valueVar + "}}")
                                                                                                    .setAttribute(NODE_RANDOM_CHAR, Type.STRING, "A")
                                                                                                    .ifThenElse(ctx -> ctx.intVar(fatherIDVar) == -1 + _gGen.getOffset(),
                                                                                                            //rootNode
                                                                                                            newTask()
                                                                                                                    .addToGlobalIndex(ENTRY_POINT_INDEX, NODE_ID)
                                                                                                            ,
                                                                                                            //children
                                                                                                            newTask()
                                                                                                                    .setAsVar(nodeVar)
                                                                                                                    .lookup("{{" + fatherIDVar + "}}")
                                                                                                                    .addVarToRelation(NODE_CHILDREN, nodeVar)
                                                                                                                    .setAsVar(fatherVar)
                                                                                                                    .readVar(nodeVar)
                                                                                                                    .addVarToRelation(NODE_FATHER, fatherVar)
                                                                                                    )
                                                                                    ))
                                                                    ,
                                                                    //else modify
                                                                    newTask()
                                                                            .thenDo(ctx -> {
                                                                                int value = ThreadLocalRandom.current().nextInt(-10, 25);
                                                                                String charac = "" + ALPHANUM.charAt(ThreadLocalRandom.current().nextInt(0, 35));
                                                                                ctx.setVariable(valueVar, value);

                                                                                ctx.setVariable(charVar, charac);
                                                                                ctx.continueTask();
                                                                            })
                                                                            .lookupAll("{{" + nodesIdVar + "}}")
                                                                            .setAttribute(NODE_VALUE, Type.INT, "{{" + valueVar + "}}")
                                                                            .setAttribute(NODE_RANDOM_CHAR, Type.STRING, "{{" + charVar + "}}")

                                                            )
                                                            //SAVE
                                                            .thenDo(ctx -> {
                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                final long startingTime = System.nanoTime();
                                                                snapshot(db).executeFrom(ctx, ctx.result(), SchedulerAffinity.SAME_THREAD,
                                                                        result1 -> {
                                                                            if (result1.exception() != null)
                                                                                result1.exception().printStackTrace();
                                                                            final long endTime = System.nanoTime();
                                                                            double numberOfNodes;
                                                                            if (realTime < _gGen.get_nbInsert())
                                                                                if (realTime == 0)
                                                                                    numberOfNodes = 10;
                                                                                else numberOfNodes = 11;
                                                                            else
                                                                                numberOfNodes = (_gGen.get_percentageOfModification() * _gGen.get_nbNodes()) / 100;
                                                                            RocksWrite[realTime] = numberOfNodes / (double) (endTime - startingTime) * 1000000000;
                                                                            RocksStorage[realTime] = FileUtils.sizeOfDirectory(new File(_pathToRocks));
                                                                            ctx.continueTask();
                                                                        });
                                                            })

                                                            .thenDo(ctx -> {
                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                final long startingTime = System.nanoTime();
                                                                saveInfluxDB().executeFrom(ctx, ctx.result(), SchedulerAffinity.SAME_THREAD,
                                                                        result12 -> {
                                                                            if (result12.exception() != null)
                                                                                result12.exception().printStackTrace();
                                                                            final long endTime = System.nanoTime();
                                                                            double numberOfNodes;
                                                                            if (realTime < _gGen.get_nbInsert())
                                                                                if (realTime == 0)
                                                                                    numberOfNodes = 10;
                                                                                else numberOfNodes = 11;
                                                                            else
                                                                                numberOfNodes = (_gGen.get_percentageOfModification() * _gGen.get_nbNodes()) / 100;
                                                                            InfluxWrite[realTime] = numberOfNodes / (double) (endTime - startingTime) * 1000000000;
                                                                            //InfluxStorage[realTime] = FileUtils.sizeOfDirectory(new File("/Users/youradmin/.influxdb/data/" + _gGen.toString()));
                                                                            //InfluxStorage[realTime] = FileUtils.sizeOfDirectory(new File("/var/lib/influxdb/data/" + _gGen.toString()));
                                                                            ctx.continueWith(ctx.newResult());
                                                                        });
                                                            })
                                                            .thenDo(
                                                                    ctx -> {
                                                                        final int realTime = ctx.intVar(graphGenTimeVar);
                                                                        final long startingTime = System.nanoTime();
                                                                        newTask().save().executeFrom(ctx, ctx.result(), SchedulerAffinity.SAME_THREAD,
                                                                                result13 -> {
                                                                                    if (result13.exception() != null)
                                                                                        result13.exception().printStackTrace();
                                                                                    final long endTime = System.nanoTime();
                                                                                    double numberOfNodes;
                                                                                    if (realTime < _gGen.get_nbInsert())
                                                                                        if (realTime == 0)
                                                                                            numberOfNodes = 10;
                                                                                        else
                                                                                            numberOfNodes = 11;
                                                                                    else
                                                                                        numberOfNodes = (_gGen.get_percentageOfModification() * _gGen.get_nbNodes()) / 100;
                                                                                    greyCatWrite[realTime / 100] = numberOfNodes / (double) (endTime - startingTime) * 1000000000;
                                                                                    greyCatStorage[realTime / 100] = FileUtils.sizeOfDirectory(new File(_pathToGrey + _gGen.toString()));
                                                                                    ctx.continueTask();
                                                                                });
                                                                    }
                                                            )
                                                            .ifThen(ctx -> ctx.intVar(graphGenTimeVar) > (_gGen.get_nbInsert() + 100),
                                                                    newTask()

                                                                            //GreyCat
                                                                            .thenDo(ctx -> {
                                                                                int[] nodeIds = new Random().ints(_gGen.getOffset(), _gGen.get_nbNodes() - 1 + _gGen.getOffset()).limit(100).toArray();
                                                                                int numberOfLoadC = 0;
                                                                                for (int index = 0; index < 100; index++) {
                                                                                    numberOfLoadC += numberOfNodeLoaded(nodeIds[index], _gGen.get_nbNodes());
                                                                                }
                                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                                final int time = ctx.intVar(adaptedTimeVar) - 100;
                                                                                int finalNumberOfLoadC = numberOfLoadC;

                                                                                final long startingTime = System.nanoTime();
                                                                                greycatSumOfChildren(nodeIds, time)
                                                                                        .executeFrom(ctx, ctx.result(), SchedulerAffinity.SAME_THREAD, new Callback<TaskResult>() {
                                                                                            @Override
                                                                                            public void on(TaskResult result) {
                                                                                                if (result.exception() != null)
                                                                                                    result.exception().printStackTrace();
                                                                                                final long endTime = System.nanoTime();
                                                                                                greyCatReadC[realTime] = (double) finalNumberOfLoadC / (double) (endTime - startingTime) * 1000000000;
                                                                                                ctx.continueWith(ctx.newResult());
                                                                                            }
                                                                                        });
                                                                            })
                                                                            .thenDo(ctx -> {
                                                                                int[] nodeIds = new Random().ints(_gGen.getOffset(), _gGen.get_nbNodes() - 1 + _gGen.getOffset()).limit(100).toArray();

                                                                                int numberOfLoadS = 0;
                                                                                int nchild = new Random().ints(0, 9).findFirst().getAsInt();
                                                                                for (int index = 0; index < 100; index++) {
                                                                                    numberOfLoadS += numberOfNodeString(nodeIds[index], nchild, _gGen.get_nbNodes());
                                                                                }

                                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                                final int time = ctx.intVar(adaptedTimeVar) - 100;
                                                                                int finalNumberOfLoadS = numberOfLoadS;

                                                                                final long startingTime = System.nanoTime();
                                                                                greycatBuildStringOfNChildren(nodeIds, nchild, time)
                                                                                        .executeFrom(ctx, ctx.result(), SchedulerAffinity.SAME_THREAD, new Callback<TaskResult>() {
                                                                                            @Override
                                                                                            public void on(TaskResult result) {
                                                                                                if (result.exception() != null)
                                                                                                    result.exception().printStackTrace();
                                                                                                final long endTime = System.nanoTime();
                                                                                                greyCatReadS[realTime] = (double) finalNumberOfLoadS / (double) (endTime - startingTime) * 1000000000;
                                                                                                ctx.continueWith(ctx.newResult());
                                                                                            }
                                                                                        });
                                                                            })

                                                                            //Influx
                                                                            .thenDo(ctx -> {
                                                                                int[] nodeIds = new Random().ints(_gGen.getOffset(), _gGen.get_nbNodes() - 1 + _gGen.getOffset()).limit(100).toArray();
                                                                                int numberOfLoadC = 0;
                                                                                for (int index = 0; index < 100; index++) {
                                                                                    numberOfLoadC += numberOfNodeLoaded(nodeIds[index], _gGen.get_nbNodes());
                                                                                }
                                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                                final int time = ctx.intVar(adaptedTimeVar) - 100;
                                                                                int finalNumberOfLoadC = numberOfLoadC;

                                                                                final long startingTime = System.nanoTime();
                                                                                influxSumOfChildren(nodeIds, time);
                                                                                final long endTime = System.nanoTime();
                                                                                InfluxReadC[realTime] = (double) finalNumberOfLoadC / (double) (endTime - startingTime) * 1000000000;
                                                                                ctx.continueTask();

                                                                            }).clearResult()
                                                                            .thenDo(ctx -> {
                                                                                int[] nodeIds = new Random().ints(_gGen.getOffset(), _gGen.get_nbNodes() - 1 + _gGen.getOffset()).limit(100).toArray();

                                                                                int numberOfLoadS = 0;
                                                                                int nchild = new Random().ints(0, 9).findFirst().getAsInt();
                                                                                for (int index = 0; index < 100; index++) {
                                                                                    numberOfLoadS += numberOfNodeString(nodeIds[index], nchild, _gGen.get_nbNodes());
                                                                                }

                                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                                final int time = ctx.intVar(adaptedTimeVar) - 100;
                                                                                int finalNumberOfLoadS = numberOfLoadS;
                                                                                final long startingTime = System.nanoTime();
                                                                                influxBuildStringOfNChildren(nodeIds, nchild, time);
                                                                                final long endTime = System.nanoTime();
                                                                                InfluxReadS[realTime] = (double) finalNumberOfLoadS / (double) (endTime - startingTime) * 1000000000;
                                                                                ctx.continueTask();
                                                                            })
                                                                            //SNAPSHOT
                                                                            .thenDo(ctx -> {
                                                                                int[] nodeIds = new Random().ints(_gGen.getOffset(), _gGen.get_nbNodes() - 1 + _gGen.getOffset()).limit(100).toArray();
                                                                                int numberOfLoadC = 0;
                                                                                for (int index = 0; index < 100; index++) {
                                                                                    numberOfLoadC += numberOfNodeLoaded(nodeIds[index], _gGen.get_nbNodes());
                                                                                }
                                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                                final int time = ctx.intVar(adaptedTimeVar) - 100;
                                                                                int finalNumberOfLoadC = numberOfLoadC;

                                                                                final long startingTime = System.nanoTime();
                                                                                rocksSumOfChildren(nodeIds, time);
                                                                                final long endTime = System.nanoTime();
                                                                                RocksReadC[realTime] = (double) finalNumberOfLoadC / (double) (endTime - startingTime) * 1000000000;
                                                                                ctx.continueTask();

                                                                            })
                                                                            .thenDo(ctx -> {
                                                                                int[] nodeIds = new Random().ints(_gGen.getOffset(), _gGen.get_nbNodes() - 1 + _gGen.getOffset()).limit(100).toArray();

                                                                                int numberOfLoadS = 0;
                                                                                int nchild = new Random().ints(0, 9).findFirst().getAsInt();
                                                                                for (int index = 0; index < 100; index++) {
                                                                                    numberOfLoadS += numberOfNodeString(nodeIds[index], nchild, _gGen.get_nbNodes());
                                                                                }

                                                                                final int realTime = ctx.intVar(graphGenTimeVar);
                                                                                final int time = ctx.intVar(adaptedTimeVar) - 100;
                                                                                int finalNumberOfLoadS = numberOfLoadS;
                                                                                final long startingTime = System.nanoTime();
                                                                                rocksBuildStringOfNChildren(nodeIds, nchild, time);
                                                                                final long endTime = System.nanoTime();
                                                                                RocksReadS[realTime] = (double) finalNumberOfLoadS / (double) (endTime - startingTime) * 1000000000;
                                                                                ctx.continueTask();
                                                                            }).clearResult()
                                                            )
                                                            .thenDo(
                                                                    ctx ->
                                                                    {
                                                                        int time = ctx.intVar(graphGenTimeVar) + 1;
                                                                        ctx.setVariable(graphGenTimeVar, time);
                                                                        int realTime = ctx.intVar(adaptedTimeVar) + ThreadLocalRandom.current().nextInt(1, 6);
                                                                        ctx.setVariable(adaptedTimeVar, realTime);
                                                                        Operations op = _gGen.nextTimeStamp();
                                                                        ctx.setVariable(operationVar, op);
                                                                        ctx.continueTask();
                                                                    }
                                                            )
                                            )
                                    )
                            )
                            .save()
                            .execute(_graph, new Callback<TaskResult>() {
                                @Override
                                public void on(TaskResult result) {
                                    if (result.exception() != null)
                                        result.exception().printStackTrace();

                                    String resultToString = Arrays.toString(IntStream.range(0, RocksReadC.length).toArray()).replace("[", "").replace("]", "");
                                    resultToString += "\n";

                                    resultToString += Arrays.toString(RocksReadC).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(RocksReadS).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(RocksWrite).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(RocksStorage).replace("[", "").replace("]", "");
                                    resultToString += "\n";

                                    resultToString += Arrays.toString(InfluxReadC).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(InfluxReadS).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(InfluxWrite).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(InfluxStorage).replace("[", "").replace("]", "");
                                    resultToString += "\n";

                                    resultToString += Arrays.toString(greyCatReadC).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(greyCatReadS).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(greyCatWrite).replace("[", "").replace("]", "");
                                    resultToString += "\n";
                                    resultToString += Arrays.toString(greyCatStorage).replace("[", "").replace("]", "");
                                    resultToString += "\n";

                                    try {
                                        BufferedWriter out = new BufferedWriter(new FileWriter(_gGen.toString() + ".csv"));
                                        out.write(resultToString);
                                        out.close();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                        System.out.println(resultToString);
                                    }


                                    _graph.disconnect(result14 -> on.on(result14));
                                }
                            })
            );
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("Duplicates")
    public Task snapshot(RocksDB db) {
        return newTask()
                .flat()
                .thenDo(ctx -> {
                    int time = ctx.intVar(adaptedTimeVar);
                    Buffer buffer = new HeapBuffer();
                    Object[] nodes = ctx.result().asArray();
                    for (int i = 0; i < nodes.length; i++) {
                        Node node = (Node) nodes[i];
                        StateChunk stateChunk = (StateChunk) ctx.graph().resolver().resolveState(node);
                        Base64.encodeLongToBuffer(node.id(), buffer);
                        buffer.write(Constants.BUFFER_SEP);
                        stateChunk.save(buffer);
                        buffer.write(Constants.BUFFER_SEP);
                    }

                    TaskResult father = ctx.variable(fatherVar);
                    if (father.size() != 0) {
                        Node fNode = (Node) father.get(0);
                        StateChunk stateChunk = (StateChunk) ctx.graph().resolver().resolveState(fNode);
                        Base64.encodeLongToBuffer(fNode.id(), buffer);
                        buffer.write(Constants.BUFFER_SEP);
                        stateChunk.save(buffer);
                        buffer.write(Constants.BUFFER_SEP);
                        ctx.setVariable(fatherVar, null);
                    }

                    WriteBatch batch = new WriteBatch();
                    BufferIterator it = buffer.iterator();
                    while (it.hasNext()) {
                        Buffer keyView = it.next();
                        Buffer valueView = it.next();
                        if (valueView != null) {
                            batch.put(keyView.data(), valueView.data());
                        }
                    }

                    buffer.free();
                    WriteOptions options = new WriteOptions();
                    options.setSync(false);
                    try {
                        db.write(options, batch);
                        Checkpoint checkpoint = Checkpoint.create(db);
                        checkpoint.createCheckpoint(_pathToRocks + "/" + time);
                        ctx.continueTask();
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                        ctx.continueTask();
                    }
                });
    }

    @SuppressWarnings("Duplicates")
    public Task saveInfluxDB() {
        InfluxDB influxDB = InfluxDBFactory.connect(_influxAddress);
        return newTask()
                .flat()
                .thenDo(ctx ->
                        {
                            TaskResult<Node> tr = ctx.resultAsNodes();
                            int size = ctx.result().size();
                            BatchPoints batchPoints = BatchPoints
                                    .database(_gGen.toString())
                                    .retentionPolicy("autogen")
                                    .consistency(InfluxDB.ConsistencyLevel.ALL)
                                    .build();
                            TaskResult trfather = ctx.variable(fatherVar);
                            if (trfather.size() != 0) {
                                Node fNode = (Node) trfather.get(0);
                                batchPoints.point(createPoint(fNode));
                            }
                            for (int i = 0; i < size; i++) {
                                Node node1 = tr.get(i);
                                batchPoints.point(createPoint(node1));
                            }
                            influxDB.write(batchPoints);
                            influxDB.close();
                            ctx.continueTask();
                        }
                );
    }

    private static Point createPoint(Node node1) {
        Object father = node1.get("father");
        Object children = node1.get("children");
        Point point;
        if (father != null) {
            if (children != null) {
                Relation relationFather = (Relation) father;
                Relation relationChildren = (Relation) children;
                point = Point.measurement(String.valueOf(node1.id()))
                        .time(node1.time(), TimeUnit.SECONDS)
                        .addField("value", (int) node1.get("value"))
                        .addField("rc", (String) node1.get("rc"))
                        .addField("children", Arrays.toString(relationChildren.all()))
                        .addField("father", relationFather.get(0))
                        .build();
            } else {
                Relation relationFather = (Relation) father;
                point = Point.measurement(String.valueOf(node1.id()))
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
                point = Point.measurement(String.valueOf(node1.id()))
                        .time(node1.time(), TimeUnit.SECONDS)
                        .addField("value", (int) node1.get("value"))
                        .addField("rc", (String) node1.get("rc"))
                        .addField("children", Arrays.toString(relationChildren.all()))
                        .addField("father", -1L)
                        .build();
            } else {
                point = Point.measurement(String.valueOf(node1.id()))
                        .time(node1.time(), TimeUnit.SECONDS)
                        .addField("value", (int) node1.get("value"))
                        .addField("rc", (String) node1.get("rc"))
                        .addField("children", "[]")
                        .addField("father", -1L)
                        .build();
            }
        }
        return point;
    }

    @SuppressWarnings("Duplicates")
    public Task greycatSumOfChildren(int[] idsTolook, int time) {
        final String sum = "Sum";
        return newTask()
                .travelInTime("" + time)
                .inject(idsTolook)
                .map(
                        newTask()
                                .declareVar(sum)
                                .lookup("{{result}}")
                                .then(ifNotEmptyThen(
                                        newTask()
                                                .setAsVar("nodes")
                                                .traverse(NODE_VALUE)
                                                .addToVar(sum)
                                                .whileDo(ctx -> ctx.variable("nodes").size() > 0,
                                                        newTask()
                                                                .readVar("nodes")
                                                                .traverse(NODE_CHILDREN)
                                                                .flat()
                                                                .setAsVar("nodes")
                                                                .traverse(NODE_VALUE)
                                                                .addToVar(sum)
                                                )
                                ))
                                .readVar(sum)
                ).thenDo(ctx -> {
                    TaskResult taskResult = ctx.result();
                    int[] result = new int[idsTolook.length];
                    if (taskResult != null) {
                        for (int i = 0; i < taskResult.size(); i++) {
                            TaskResult subTaskResult = (TaskResult) taskResult.get(i);
                            for (int j = 0; j < subTaskResult.size(); j++) {
                                result[i] += (int) subTaskResult.get(j);
                            }
                        }
                    }
                    ctx.continueWith(ctx.wrap(result));
                })
                ;
    }

    @SuppressWarnings("Duplicates")
    public Task greycatBuildStringOfNChildren(int[] idsTolook, int n, int time) {
        final String stringSequence = "StringSequence";
        return newTask()
                .travelInTime("" + time)
                .inject(idsTolook)
                .map(
                        newTask()
                                .declareVar(stringSequence)
                                .lookup("{{result}}")
                                .then(ifNotEmptyThen(
                                        newTask()
                                                .setAsVar("node")
                                                .traverse(NODE_RANDOM_CHAR)
                                                .addToVar(stringSequence)
                                                .whileDo(ctx -> ctx.variable("node").get(0) != null,
                                                        newTask()
                                                                .thenDo(ctx -> {
                                                                            Node node = (Node) ctx.variable("node").get(0);
                                                                            long childid = ((Relation) node.get(NODE_CHILDREN)).get(n);
                                                                            ctx.setVariable("child", childid);
                                                                            ctx.continueTask();
                                                                        }
                                                                )
                                                                .lookup("{{child}}")
                                                                .setAsVar("node")
                                                                .traverse(NODE_RANDOM_CHAR)
                                                                .addToVar(stringSequence)
                                                )
                                ))
                                .readVar(stringSequence)
                )
                .thenDo(ctx -> {
                            TaskResult taskResult = ctx.result();
                            String[] result = new String[idsTolook.length];
                            Arrays.fill(result, "");
                            if (taskResult != null) {
                                for (int i = 0; i < taskResult.size(); i++) {
                                    TaskResult subTaskResult = (TaskResult) taskResult.get(i);
                                    for (int j = 0; j < subTaskResult.size(); j++) {
                                        result[i] += (String) subTaskResult.get(j);
                                    }
                                }
                            }
                            ctx.continueWith(ctx.wrap(result));
                        }
                );

    }

    public int[] influxSumOfChildren(int[] idsToLook, int time) {
        String db = _gGen.toString();
        InfluxDB influxDB = InfluxDBFactory.connect(_influxAddress);
        int[] sums = new int[idsToLook.length];
        for (int k = 0; k < idsToLook.length; k++) {
            List<Integer> listOfId = new ArrayList<>();
            listOfId.add(idsToLook[k]);
            double sum = 0;
            while (listOfId.size() != 0) {
                String ids = listOfId.stream().map(Object::toString).collect(Collectors.joining("\",\"", "\"", "\""));
                String squery = "SELECT value,children FROM " + ids + " WHERE time<=" + time + "s ORDER BY time DESC LIMIT 1";
                org.influxdb.dto.Query query = new org.influxdb.dto.Query(squery, db);
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
            sums[k] = (int) sum;
        }
        influxDB.close();
        return sums;
    }

    public String[] influxBuildStringOfNChildren(int[] idsToLook, int n, int time) {
        String db = _gGen.toString();
        InfluxDB influxDB = InfluxDBFactory.connect(_influxAddress);
        String[] sequences = new String[idsToLook.length];
        for (int k = 0; k < idsToLook.length; k++) {
            int toLook = idsToLook[k];
            String sequence = "";
            while (toLook != -1) {
                String squery = "SELECT rc,children FROM \"" + toLook + "\" WHERE time<=" + time + "s ORDER BY time DESC LIMIT 1";
                org.influxdb.dto.Query query = new org.influxdb.dto.Query(squery, db);
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
            sequences[k] = sequence;
        }
        influxDB.close();
        return sequences;


    }

    @SuppressWarnings("Duplicates")
    public int findClosestTime(int time) {
        File[] directories = new File(_pathToRocks).listFiles(File::isDirectory);
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
    public int[] rocksSumOfChildren(int[] idsToLook, int time) {
        RocksDB db = null;
        int children = HashHelper.hash(NODE_CHILDREN);
        int value = HashHelper.hash(NODE_VALUE);
        int[] sums = new int[idsToLook.length];
        try {
            db = RocksDB.openReadOnly(_pathToRocks + "/" + findClosestTime(time));
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
            return sums;
        }
    }

    @SuppressWarnings("Duplicates")
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
    public String[] rocksBuildStringOfNChildren(int[] idsToLook, int n, int time) {
        RocksDB db = null;
        int children = HashHelper.hash(NODE_CHILDREN);
        int rc = HashHelper.hash(NODE_RANDOM_CHAR);
        String[] sequences = new String[idsToLook.length];
        try {
            db = RocksDB.openReadOnly(_pathToRocks + "/" + findClosestTime(time));
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
            return sequences;
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

    @SuppressWarnings("Duplicates")
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

    public static void main(String[] args) {
        int percentOfModification = 10;
        int nbNodes = 1000;
        int nbSplit = 1;
        int numberOfModification = 120;
        int startPosition = (100 - percentOfModification) * nbNodes / 100;
        GraphGenerator graphGenerator = new BasicGraphGenerator(nbNodes, percentOfModification, nbSplit, numberOfModification, startPosition, 3);
        Experiment experiment = new Experiment(numberOfModification, "grey/grey_", "snap/", "http://127.0.0.1:8086", 1000000, graphGenerator);
        experiment.constructGraph(result -> System.out.println("done"));
    }

}
