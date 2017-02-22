package greycat.bench;

public class Operations {

    private final boolean _insert;
    private final int[] _arrayOfNodes;

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
