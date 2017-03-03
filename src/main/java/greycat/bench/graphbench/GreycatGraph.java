package greycat.bench.graphbench;

import greycat.*;
import greycat.bench.graphgen.BasicGraphGenerator;
import greycat.bench.graphgen.GraphGenerator;
import greycat.bench.graphgen.Operations;
import greycat.rocksdb.RocksDBStorage;
import greycat.scheduler.HybridScheduler;
import greycat.struct.Relation;
import org.rocksdb.RocksDB;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static greycat.Tasks.newTask;
import static greycat.bench.BenchConstants.*;
import static mylittleplugin.MyLittleActions.ifEmptyThen;
import static mylittleplugin.MyLittleActions.ifNotEmptyThen;

public class GreycatGraph implements BenchGraph {


    protected final Graph _graph;
    private final GraphGenerator _gGen;

    private static final String ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private final int _saveEveryModif;
    public final static String adaptedTimeVar = "realTime";
    public final static String fatherVar = "father";

    /**
     * @param pathToSave     where should the database be saved
     * @param memorySize     how many nodes should the cache contains
     * @param graphGenerator generator of graph
     * @
     */
    public GreycatGraph(String pathToSave, int memorySize, int saveEvery, GraphGenerator graphGenerator) {
        this._graph = new GraphBuilder()
                .withStorage(new RocksDBStorage(pathToSave + graphGenerator.toString()))
                .withMemorySize(memorySize)
                .withScheduler(new HybridScheduler())
                .build();
        this._gGen = graphGenerator;
        this._saveEveryModif = saveEvery;
    }

    public void constructGraph(Callback<Boolean> callback) {
        internal_ConstructGraph(null, callback);
    }


    protected void internal_ConstructGraph(RocksDB dbSnap, Callback<Boolean> callback) {
        final String graphGenTimeVar = "time";
        final String operationVar = "operation";
        final String valueVar = "value";
        final String nodesIdVar = "nodesId";
        final String nodeIdVar = "nodeId";
        final String fatherIDVar = "fatherId";
        final String nodeVar = "node";
        final String fatherVar = "father";
        final String charVar = "character";
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
                                                                        .mapPar(
                                                                                newTask()
                                                                                        .setAttribute(NODE_VALUE, Type.INT, "{{" + valueVar + "}}")
                                                                                        .setAttribute(NODE_RANDOM_CHAR, Type.STRING, "{{" + charVar + "}}")
                                                                        )
                                                        )

                                                        .ifThen(ctx -> dbSnap != null,
                                                                newTask().pipe(RockDBSnapshot.snapshot(dbSnap, this.get_path())))

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
                                                        .ifThen(ctx -> ctx.intVar(graphGenTimeVar) % _saveEveryModif == 0,
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

    @SuppressWarnings("Duplicates")
    @Override
    public void sumOfChildren(int[] idsTolook, int time, Callback<int[]> callback) {
        final String sum = "Sum";
        _graph.connect(
                on -> {
                    newTask()

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
                            )
                            .execute(_graph, taskResult -> {
                                if (taskResult.exception() != null)
                                    taskResult.exception().printStackTrace();

                                int[] result = new int[idsTolook.length];
                                if (taskResult != null) {
                                    for (int i = 0; i < taskResult.size(); i++) {
                                        TaskResult subTaskResult = (TaskResult) taskResult.get(i);
                                        for (int j = 0; j < subTaskResult.size(); j++) {
                                            result[i] += (int) subTaskResult.get(j);
                                        }
                                    }
                                }
                                _graph.disconnect(
                                        off -> callback.on(result));
                            });

                });
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void buildStringOfNChildren(int[] idsTolook, int n, int time, Callback<String[]> callback) {
        if (n < 0 || n > 9) throw new RuntimeException("n must be between 0 and 9");
        final String stringSequence = "StringSequence";

        _graph.connect(
                on -> {
                    newTask()

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
                            .execute(_graph,
                                    taskResult -> {
                                        if (taskResult.exception() != null)
                                            taskResult.exception().printStackTrace();

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
                                        _graph.disconnect(
                                                off -> callback.on(result));
                                    });
                }
        );

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
                    //CountDownLatch countDownLatch = new CountDownLatch(1);
                    GreycatGraph grey = new GreycatGraph(
                            "grey/grey_", memorySize, saveEvery,
                            new BasicGraphGenerator(nbNodes[i], percentOfModification[j], nbSplit[k], nbModification[i], startPosition, 3));

                    grey.constructGraph(result -> System.out.println("done!"));
                    /**grey.sumOfChildren(new int[]{11, 10}, 1500, new Callback<int[]>() {
                    @Override public void on(int[] integer) {

                    countDownLatch.countDown();
                    System.out.println(Arrays.toString(integer));
                    }
                    });
                     grey.buildStringOfNChildren(new int[]{11, 10}, 5, 1500, new Callback<String[]>() {
                    @Override public void on(String[] s) {

                    countDownLatch.countDown();
                    System.out.println(Arrays.toString(s));
                    }
                    });*/
                   // countDownLatch.await();
                }
            }
        }
    }


    public String get_path() {
        return null;
    }
}
