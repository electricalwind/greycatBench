package greycat.bench.graphbench;

import greycat.Constants;
import greycat.Node;
import greycat.Task;
import greycat.TaskResult;
import greycat.chunk.StateChunk;
import greycat.internal.heap.HeapBuffer;
import greycat.struct.Buffer;
import greycat.struct.BufferIterator;
import greycat.utility.Base64;
import org.rocksdb.*;

import static greycat.Tasks.newTask;
import static greycat.bench.graphbench.GreycatGraph.adaptedTimeVar;
import static greycat.bench.graphbench.GreycatGraph.fatherVar;

public class RockDBSnapshot {

    public static Task snapshot(RocksDB db, String path) {
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

                        checkpoint.createCheckpoint(path + "/" + time);
                        ctx.continueTask();
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                        ctx.continueTask();
                    }
                });
    }


}