package greycat.bench.graphgen;

public interface GraphGenerator {

    /**
     * @return the operations to do at the next timeStamp
     */
    public Operations nextTimeStamp();

    /**
     *
     * @return the offset to add to the node id
     */
    public int getOffset();

    /**
     *
     * @return the number of splits required to modified all node that are supposed to be modified
     */
    public int get_nbSplit();

    /**
     *
     * @return the number of nodes that should be generated
     */
    public int get_nbNodes();

    public int get_nbInsert();

    public int get_percentageOfModification();
}
