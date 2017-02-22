package greycat.bench;

import greycat.*;
import greycat.rocksdb.RocksDBStorage;
import greycat.scheduler.TrampolineScheduler;

import static greycat.Tasks.newTask;
import static greycat.bench.BenchConstants.ENTRY_POINT_INDEX;
import static greycat.bench.BenchConstants.NODE_ID;

public class GreycatGraph {

    private final Graph _graph;
    private final GraphGenerator _gGen;

    public GreycatGraph(String pathToSave, GraphGenerator graphGenerator) {
        this._graph = new GraphBuilder()
                .withStorage(new RocksDBStorage(pathToSave + graphGenerator.toString()))
                .withMemorySize(1000000)
                .withScheduler(new TrampolineScheduler()).build();
        this._gGen = graphGenerator;
    }

    public void creatingGraph(int saveEveryModif) {
        final long timeStart = System.currentTimeMillis();
        _graph.connect(
                new Callback<Boolean>() {
                    @Override
                    public void on(Boolean result) {
                        newTask()
                                .thenDo(ctx -> {
                                    ctx.setVariable("time", 0);
                                    ctx.setVariable("operation", _gGen.nextTimeStamp());
                                    ctx.continueTask();

                                })

                                .createNode()
                                .setAttribute(NODE_ID,Type.INT,"-1")
                                .addToGlobalIndex(ENTRY_POINT_INDEX, NODE_ID)

                                .whileDo(
                                        ctx -> ctx.variable("operation") != null,
                                        newTask()
                                                .thenDo(ctx -> {
                                                    ctx.setTime(ctx.intVar("time"));
                                                    Operations op = (Operations) ctx.variable("operation").get(0);
                                                    ctx.setVariable("nodes", op.get_arrayOfNodes());
                                                    ctx.continueWith(ctx.wrap(op.is_insert()));
                                                })
                                                .ifThenElse(ctx -> (boolean) ctx.result().get(0),
                                                        //then (insert)
                                                        newTask()
                                                                .readVar("nodes")
                                                                .forEach(
                                                                        newTask()
                                                                                .thenDo(ctx -> ctx.continueWith(ctx.wrap((int)ctx.result().get(0) +3)))
                                                                                .setAsVar("node")
                                                                                .thenDo(ctx -> ctx.continueWith(ctx.wrap((ctx.intVar("node") / 10) - 1)))
                                                                                .setAsVar("father")
                                                                                .createNode()
                                                                                .setAttribute(NODE_ID, Type.INT, "{{node}}")
                                                                                .setAttribute("value", Type.INT, "0")

                                                                                .ifThenElse(ctx -> ctx.intVar("father") == -1,
                                                                                        //rootNode
                                                                                        newTask()
                                                                                                .addToGlobalIndex(ENTRY_POINT_INDEX, NODE_ID)
                                                                                        ,
                                                                                        newTask()
                                                                                                .setAsVar("newNode")
                                                                                                .lookup("{{father}}")
                                                                                                .addVarToRelation("children", "newNode", NODE_ID)
                                                                                                .setAsVar("fatherNode")
                                                                                                .readVar("newNode")
                                                                                                .addVarToRelation("father", "fatherNode")
                                                                                )
                                                                )
                                                        ,
                                                        //else (modify)
                                                        newTask()
                                                                .readVar("nodes")
                                                                .forEach(
                                                                        newTask()
                                                                                .thenDo(ctx -> ctx.continueWith(ctx.wrap((int)ctx.result().get(0) +3)))
                                                                                .setAsVar("node")
                                                                                .lookup("{{node}}")
                                                                                .thenDo(
                                                                                        ctx ->
                                                                                        {
                                                                                            int i = 0;
                                                                                            ctx.continueTask();
                                                                                        }
                                                                                )
                                                                                .thenDo(ctx -> {
                                                                                    Node node = ctx.resultAsNodes().get(0);
                                                                                    node.set("value", Type.INT, (int) node.get("value") + 1);
                                                                                    ctx.continueTask();
                                                                                })
                                                                )
                                                )
                                                .thenDo(
                                                        ctx ->
                                                        {
                                                            int time = ctx.intVar("time") + 1;
                                                            ctx.setVariable("time", time);

                                                            ctx.setVariable("operation", _gGen.nextTimeStamp());
                                                            ctx.continueTask();
                                                        }
                                                ).ifThen(ctx -> ctx.intVar("time") % saveEveryModif == 0,
                                                newTask()
                                                        .thenDo(ctx ->
                                                        {
                                                            final long timeEnd = System.currentTimeMillis();
                                                            long timeToProcess = timeEnd - timeStart;
                                                            System.out.println("time to do" + ctx.intVar("time") + " timestamp: " + timeToProcess);
                                                            ctx.continueTask();
                                                        })
                                                        .save()
                                        )

                                )
                                .execute(_graph, new Callback<TaskResult>() {
                                    @Override
                                    public void on(TaskResult result) {
                                        final long timeEnd = System.currentTimeMillis();
                                        System.out.println(timeEnd - timeStart);

                                        _graph.disconnect(new Callback<Boolean>() {
                                            @Override
                                            public void on(Boolean result) {

                                            }
                                        });
                                    }
                                });
                    }
                }
        );
    }

    public static void main(String[] args) {
        GreycatGraph grey = new GreycatGraph("grey_", new SplitBaseGraphGenerator(1000,10,3,1000,500));
        grey.creatingGraph(100);
    }


}
