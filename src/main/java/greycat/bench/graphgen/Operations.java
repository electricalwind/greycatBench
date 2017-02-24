package greycat.bench.graphgen;

/**
 * Data class containing the operations to do at a timeStamp
 */
public class Operations {

    private final boolean _insert;
    private final int[] _arrayOfNodes;

    /**
     * Constructor
     * @param insert is the operations purely insertion ones
     * @param arrayOfNodes the nodes id to modify
     */
    public Operations(boolean insert, int[] arrayOfNodes){
        this._insert = insert;
        this._arrayOfNodes =arrayOfNodes;
    }
    public boolean is_insert() {
        return _insert;
    }

    public int[] get_arrayOfNodes() {
        return _arrayOfNodes;
    }
}
