package greycat.bench;

public class SplitBaseGraphGenerator implements GraphGenerator {

    private final int _nbNodes;
    private final int _nbSplit;
    private final int _numberOfModification;
    private final int _startPositionOfModification;
    private final int _numberOfNodesToChange;
    private final int _maxCase;
    private final int _sizeArray;
    private final int _percentageOfModification;
    private int _nbInsert;
    private int _currentTimestamp = 0;

    /**
     * SplitBase Generator Class
     *
     * @param nbNodes                     number of Nodes to generate
     * @param percentageOfModification    Which percent of Nodes are supposed to be modified over times
     * @param nbSplit                     how many timestamp does it takes to modify once all the nodes supposed to be modified
     * @param numberOfModification        how many times should theses nodes be modified
     * @param startPositionOfModification which nodes are supposed to be modified
     */
    public SplitBaseGraphGenerator(int nbNodes, int percentageOfModification, int nbSplit, int numberOfModification, int startPositionOfModification) {

        if (percentageOfModification < 0 || percentageOfModification > 100) {
            throw new RuntimeException("A percentage should be between 0 et 100");
        }

        // User Information
        this._nbNodes = nbNodes;
        this._nbSplit = nbSplit;
        this._numberOfModification = numberOfModification;
        this._startPositionOfModification = startPositionOfModification;
        this._percentageOfModification = percentageOfModification;

        //computed Information
        this._numberOfNodesToChange = percentageOfModification * nbNodes / 100;
        this._nbInsert = nbNodes / 10;
        if (nbNodes % 10 != 0) {
            this._nbInsert++;
        }
        this._maxCase = _numberOfNodesToChange / nbSplit;
        this._sizeArray = _numberOfNodesToChange / _nbSplit;

        if (startPositionOfModification + _numberOfNodesToChange > nbNodes)
            throw new RuntimeException("startposition to close to the number of nodes");
    }

    public Operations nextTimeStamp() {
        Operations op = atTimeStamp(_currentTimestamp);
        _currentTimestamp++;
        return op;
    }

    public Operations atTimeStamp(int timeStamp) {
        boolean insert;
        int[] listOfNodes;

        if (timeStamp < _nbInsert) {
            insert = true;
            listOfNodes = new int[10];
            for (int i = 0; i < 10; i++) {
                int toAdd = timeStamp * 10 + i;
                if (toAdd < _nbNodes)
                    listOfNodes[i] = timeStamp * 10 + i;
            }
        } else {
            insert = false;

            int modificationStatus = (timeStamp - _nbInsert) / _nbSplit;
            if (modificationStatus > _numberOfModification) return null;

            int splitNumber = (timeStamp - _nbInsert) % _nbSplit;

            if (splitNumber < _numberOfNodesToChange % _nbSplit) {
                listOfNodes = new int[_sizeArray + 1];
                listOfNodes[_sizeArray] = _startPositionOfModification + _numberOfNodesToChange - splitNumber;
            } else {
                listOfNodes = new int[_sizeArray];
            }

            int cas = modificationStatus % _maxCase;
            if (cas == 0) {
                cas = _maxCase;
            }

            int subsize = 0;
            int nodeToAdd = _startPositionOfModification + splitNumber * cas;
            for (int i = 0; i < _sizeArray; i++) {
                listOfNodes[i] = nodeToAdd;
                subsize++;
                if (subsize == cas) {
                    subsize = 0;
                    nodeToAdd = nodeToAdd + _nbSplit * cas;
                } else {
                    nodeToAdd++;
                }

            }

        }
        return new Operations(insert, listOfNodes);
    }

    public int get_nbNodes() {
        return _nbNodes;
    }

    public int get_nbSplit() {
        return _nbSplit;
    }

    public int get_numberOfModification() {
        return _numberOfModification;
    }

    public int get_startPositionOfModification() {
        return _startPositionOfModification;
    }

    public int get_numberOfNodesToChange() {
        return _numberOfNodesToChange;
    }

    public int get_currentTimestamp() {
        return _currentTimestamp;
    }

    /**
     * public static void main(String[] args) {
     *
     * for (int i = 0; i < 1000000; i++) {
     * int[] arr = atTimestampDo(i, 10000, 10, 3, 20, 8999).get_arrayOfNodes();
     * }
     * }
     */

    @Override
    public String toString() {
        return _nbNodes + "_" + _percentageOfModification + "_" + _nbSplit + "_" + _numberOfModification + "_" + _startPositionOfModification;
    }

    public static Operations atTimestampDo(int timeStamp, int nbNodes, int percentageOfModification, int nbSplit, int numberOfModification, int startPositionOfModification) {
        boolean insert;
        final int numberOfNodesToChange = percentageOfModification * nbNodes / 100;
        int nbInsert = nbNodes / 10;
        if (nbNodes % 10 != 0) {
            nbInsert++;
        }
        int[] listOfNodes;

        if (timeStamp < nbInsert) {
            insert = true;
            listOfNodes = new int[10];
            for (int i = 0; i < 10; i++) {
                int toAdd = timeStamp * 10 + i;
                if (toAdd < nbNodes)
                    listOfNodes[i] = timeStamp * 10 + i;
            }
        } else {

            insert = false;

            if (startPositionOfModification + numberOfNodesToChange > nbNodes)
                throw new RuntimeException("startposition to close to the number of nodes");

            int modificationStatus = (timeStamp - nbInsert) / nbSplit;
            if (modificationStatus > numberOfModification) return null;

            int splitNumber = (timeStamp - nbInsert) % nbSplit;

            int sizeArray = numberOfNodesToChange / nbSplit;
            if (splitNumber < numberOfNodesToChange % nbSplit) {
                listOfNodes = new int[sizeArray + 1];
                listOfNodes[sizeArray] = startPositionOfModification + numberOfNodesToChange - splitNumber;
            } else {
                listOfNodes = new int[sizeArray];
            }


            int maxCase = numberOfNodesToChange / nbSplit;
            int cas = modificationStatus % maxCase;
            if (cas == 0) {
                cas = maxCase;
            }

            int subsize = 0;
            int nodeToAdd = startPositionOfModification + splitNumber * cas;
            for (int i = 0; i < sizeArray; i++) {
                listOfNodes[i] = nodeToAdd;
                subsize++;
                if (subsize == cas) {
                    subsize = 0;
                    nodeToAdd = nodeToAdd + nbSplit * cas;
                } else {
                    nodeToAdd++;
                }

            }

        }
        return new Operations(insert, listOfNodes);

    }
}
