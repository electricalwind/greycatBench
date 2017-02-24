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
import static greycat.bench.BenchConstants.*;
import static mylittleplugin.MyLittleActions.ifEmptyThen;

public class GreycatGraph {


    protected final Graph _graph;
    private final GraphGenerator _gGen;

    private static final String ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /**
     * @param pathToSave     where should the database be saved
     * @param memorySize     how many nodes should the cache contains
     * @param graphGenerator generator of graph
     */
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

        final String graphGenTimeVar = "time";
        final String adaptedTimeVar = "realTime";
        final String operationVar = "operation";
        final String valueVar = "value";
        final String nodesIdVar = "nodesId";
        final String nodeIdVar = "nodeId";
        final String fatherIDVar = "fatherId";
        final String nodeVar = "node";
        final String fatherVar = "father";

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

                                                        .ifThenElse(ctx -> (boolean) ctx.result().get(0),
                                                                //If Insert
                                                                newTask()
                                                                        .readVar(nodesIdVar)
                                                                        .forEach(newTask()
                                                                                .setAsVar(nodeIdVar)
                                                                                .thenDo(ctx -> ctx.continueWith(ctx.wrap(((ctx.intVar(nodeIdVar) - _gGen.getOffset()) / 10) - 1 + _gGen.getOffset())))
                                                                                .setAsVar(fatherIDVar)
                                                                                .createNode()
                                                                                .setAttribute(NODE_ID, Type.INT, "{{" + nodeIdVar + "}}")
                                                                                .setAttribute(NODE_VALUE, Type.INT, "{{" + valueVar + "}}")
                                                                                .setAttribute(NODE_RANDOM_CHAR, Type.STRING, String.valueOf(ALPHANUM.charAt(ThreadLocalRandom.current().nextInt())))
                                                                                .ifThenElse(ctx -> ctx.intVar(fatherIDVar) == -1 + _gGen.getOffset(),
                                                                                        //rootNode
                                                                                        newTask()
                                                                                                .addToGlobalIndex(ENTRY_POINT_INDEX, NODE_ID)
                                                                                        ,
                                                                                        //children
                                                                                        newTask()
                                                                                                .setAsVar(nodeVar)
                                                                                                .lookup("{{" + fatherIDVar + "}}")
                                                                                                .addVarToRelation(NODE_CHILDREN, nodeVar, NODE_ID)
                                                                                                .setAsVar(fatherVar)
                                                                                                .readVar(nodeVar)
                                                                                                .addVarToRelation(NODE_FATHER, fatherVar)
                                                                                )
                                                                        )
                                                                ,
                                                                //else modify
                                                                newTask()
                                                                        .thenDo(ctx -> {
                                                                            int value = ThreadLocalRandom.current().nextInt(-10, 25);
                                                                            ctx.setVariable(valueVar, value);
                                                                            ctx.continueTask();
                                                                        })
                                                                        .lookupAll("{{" + nodesIdVar + "}}")
                                                                        .forEachPar(
                                                                                newTask()
                                                                                        .setAttribute(NODE_VALUE, Type.INT, "{{" + valueVar + "}}")
                                                                                        .setAttribute(NODE_RANDOM_CHAR, Type.STRING, String.valueOf(ALPHANUM.charAt(ThreadLocalRandom.current().nextInt())))
                                                                        )
                                                        )
                                                        .thenDo(
                                                                ctx ->
                                                                {
                                                                    int time = ctx.intVar(graphGenTimeVar) + 1;
                                                                    ctx.setVariable(graphGenTimeVar, time);
                                                                    int realTime = ctx.intVar(adaptedTimeVar) + ThreadLocalRandom.current().nextInt(1, 6);
                                                                    ctx.setVariable(adaptedTimeVar, realTime);
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
                        })
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

        //Parameters
        int memorySize;
        int[] percentOfModification;
        int[] nbSplit;
        int[] nbModification;
        int[] nbNodes;

        boolean test = true;
        if (test) { // laptop
            memorySize = 1000000;
            nbNodes = new int[]{10000};
            percentOfModification = new int[]{10};
            nbSplit = new int[]{1};
            nbModification = new int[]{10000};
        } else { //server
            memorySize = 100000000;
            nbNodes = new int[]{10000, 100000, 1000000};
            percentOfModification = new int[]{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
            nbSplit = new int[]{1, 2, 3, 4};
            nbModification = new int[]{10000,10000,1000};
        }


        for (int k = 0; k < nbSplit.length; k++) {
            for (int i = 0; i < nbNodes.length; i++) {
                for (int j = 0; j < percentOfModification.length; j++) {

                    int startPosition = (100 - percentOfModification[j]) * nbNodes[i] / 100;

                    int saveEvery = (memorySize * 10 * nbSplit[k]) / (nbNodes[i] * percentOfModification[j] + 1) - 1;

                    if (percentOfModification[j] == 0 && k != 0) break;
                    CountDownLatch countDownLatch = new CountDownLatch(1);
                    GreycatGraph grey = new GreycatGraph(
                            "grey/grey_", memorySize,
                            new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[i], startPosition, 3));

                    grey.creatingGraph(saveEvery, new Callback<Boolean>() {
                        @Override
                        public void on(Boolean result) {
                            countDownLatch.countDown();
                        }
                    });
                    countDownLatch.await();
                }
            }
        }
    }


}
