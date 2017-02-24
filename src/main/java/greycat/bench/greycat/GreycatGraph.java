package greycat.bench.greycat;

import greycat.*;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;
import greycat.bench.graphgen.Operations;
import greycat.rocksdb.RocksDBStorage;
import greycat.scheduler.HybridScheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static greycat.Tasks.newTask;
import static greycat.bench.BenchConstants.ENTRY_POINT_INDEX;
import static greycat.bench.BenchConstants.NODE_ID;
import static mylittleplugin.MyLittleActions.ifEmptyThen;

public class GreycatGraph {

    protected final Graph _graph;
    private final GraphGenerator _gGen;
    private static final String ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public GreycatGraph(String pathToSave, int memorySize, GraphGenerator graphGenerator) {
        this._graph = new GraphBuilder()
                .withStorage(new RocksDBStorage(pathToSave + graphGenerator.toString()))
                .withMemorySize(memorySize)
                .withScheduler(new HybridScheduler())
                .build();
        this._gGen = graphGenerator;
    }

    public void creatingGraph(int saveEveryModif, Callback<Boolean> callback) {
        final long timeStart = System.currentTimeMillis();
        _graph.connect(
                new Callback<Boolean>() {
                    @Override
                    public void on(Boolean result) {
                        newTask()
                                .thenDo(ctx -> {
                                    ctx.setVariable("time", 0);
                                    ctx.setVariable("realTime",0);
                                    ctx.setVariable("value", 0);
                                    ctx.setVariable("operation", _gGen.nextTimeStamp());
                                    ctx.continueTask();
                                })
                                .readGlobalIndex(ENTRY_POINT_INDEX)
                                .then(
                                        ifEmptyThen(
                                                newTask()
                                                        .createNode()
                                                        .setAttribute(NODE_ID, Type.INT, "-1")
                                                        .addToGlobalIndex(ENTRY_POINT_INDEX, NODE_ID)
                                                        .whileDo(
                                                                ctx -> ctx.variable("operation").get(0) != null,
                                                                newTask()
                                                                        .thenDo(ctx -> {
                                                                            ctx.setTime(ctx.intVar("realTime"));
                                                                            Operations op = (Operations) ctx.variable("operation").get(0);
                                                                            if (op == null) {
                                                                                int i = 0;
                                                                            }
                                                                            ctx.setVariable("nodesId", op.get_arrayOfNodes());
                                                                            ctx.continueWith(ctx.wrap(op.is_insert()));
                                                                        })
                                                                        .ifThenElse(ctx -> (boolean) ctx.result().get(0),
                                                                                //then (insert)
                                                                                newTask()
                                                                                        .readVar("nodesId")
                                                                                        .forEach(
                                                                                                newTask()
                                                                                                        .setAsVar("nodeId")
                                                                                                        .thenDo(ctx -> ctx.continueWith(ctx.wrap(((ctx.intVar("nodeId") - _gGen.getOffset()) / 10) - 1 + _gGen.getOffset())))
                                                                                                        .setAsVar("fatherId")
                                                                                                        .createNode()
                                                                                                        .setAttribute(NODE_ID, Type.INT, "{{nodeId}}")
                                                                                                        .setAttribute("value", Type.INT, "{{value}}")
                                                                                                        .setAttribute("rc", Type.STRING, String.valueOf(ALPHANUM.charAt(ThreadLocalRandom.current().nextInt())))
                                                                                                        .ifThenElse(ctx -> ctx.intVar("fatherId") == -1 + _gGen.getOffset(),
                                                                                                                //rootNode
                                                                                                                newTask()
                                                                                                                        .addToGlobalIndex(ENTRY_POINT_INDEX, NODE_ID)
                                                                                                                ,
                                                                                                                newTask()
                                                                                                                        .setAsVar("node")
                                                                                                                        .lookup("{{fatherId}}")
                                                                                                                        .addVarToRelation("children", "node", NODE_ID)
                                                                                                                        .setAsVar("fatherNode")
                                                                                                                        .readVar("node")
                                                                                                                        .addVarToRelation("father", "fatherNode")
                                                                                                        )
                                                                                        )
                                                                                ,
                                                                                //else (modify)
                                                                                newTask()
                                                                                        .thenDo(ctx -> {
                                                                                            //int value = (ctx.intVar("value") / _gGen.get_nbSplit()) + 1;
                                                                                            int value = ThreadLocalRandom.current().nextInt(-10, 25);
                                                                                            ctx.setVariable("newValue", value);
                                                                                            ctx.continueTask();
                                                                                        })
                                                                                        .lookupAll("{{nodesId}}")
                                                                                        .forEachPar(
                                                                                                newTask()
                                                                                                        .setAttribute("value", Type.INT, "{{newValue}}")
                                                                                                        .setAttribute("rc", Type.STRING, String.valueOf(ALPHANUM.charAt(ThreadLocalRandom.current().nextInt())))
                                                                                        )
                                                                                        //.then(increment("value", 1))
                                                                        )
                                                                        .thenDo(
                                                                                ctx ->
                                                                                {
                                                                                    int time = ctx.intVar("time") + 1;
                                                                                    ctx.setVariable("time", time);
                                                                                    int realTime = ctx.intVar("realTime") +ThreadLocalRandom.current().nextInt(1,6);
                                                                                    // System.out.println(time);
                                                                                    Operations op = _gGen.nextTimeStamp();
                                                                                    ctx.setVariable("operation", op);
                                                                                    ctx.continueTask();
                                                                                }
                                                                        )
                                                                        .ifThen(ctx -> ctx.intVar("time") % saveEveryModif == 0,
                                                                                newTask()
                                                                                        .save()
                                                                        )

                                                        )
                                                        .save()
                                        )
                                )
                                .execute(_graph, new Callback<TaskResult>() {
                                    @Override
                                    public void on(TaskResult result) {
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

    public static Task sumOfAllChildren(String nodeIds) {
        return newTask()
                .lookupAll(nodeIds)
                .mapPar(
                        newTask()
                                .traverse("children")
                                .setAsVar("children")
                                .inject(0)
                                .defineAsVar("sum")
                                .whileDo(ctx -> ctx.variable("children").size() != 0,
                                        newTask()
                                                .readVar("children")
                                                .traverse("value")
                                                .thenDo(ctx -> {
                                                    TaskResult tr = ctx.result();
                                                    int sum = ctx.intVar("sum");
                                                    for (int i = 0; i < tr.size(); i++) {
                                                        sum += (int) tr.get(i);
                                                    }
                                                    ctx.setVariable("sum", sum);
                                                    ctx.continueTask();
                                                })
                                                .readVar("children")
                                                .traverse("children")
                                                .setAsVar("children")
                                )
                                .readVar("sum")
                )
                .thenDo(ctx -> {
                    int sum = 0;
                    TaskResult tr = ctx.result();
                    for (int i = 0; i < tr.size(); i++) {
                        sum += (int) tr.get(i);
                    }
                    ctx.continueWith(ctx.wrap(sum));
                });
    }

    public static Task getNodesValueWithoutTraverse(String nodeId, String time) {
        return newTask()
                .travelInTime(time)
                .lookupAll(nodeId)
                .traverse("value");
    }


    public static void main(String[] args) throws InterruptedException {
        int memorySize = 1000000;//00
        int[] nbNodes = {10000};//0, 100000, 1000000};
        int[] percentOfModification = {10};//{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        int[] nbSplit = {1};//{1, 2, 3, 4};
        int[] nbModification = {10000};//{10000,10000,1000};
        for (int k = 0; k < nbSplit.length; k++) {
            for (int i = 0; i < nbNodes.length; i++) {
                for (int j = 0; j < percentOfModification.length; j++) {

                    int startPosition = (100 - percentOfModification[j]) * nbNodes[i] / 100;
                    int saveEvery = (memorySize * 10 * nbSplit[k]) / (nbNodes[i] * percentOfModification[j] + 1) - 1;

                    if (percentOfModification[j] == 0 && k != 0) break;
                    CountDownLatch loginLatch = new CountDownLatch(1);
                    GreycatGraph grey = new GreycatGraph("grey/grey_", memorySize, new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[i], startPosition, 3));
                    grey.creatingGraph(saveEvery, new Callback<Boolean>() {
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
