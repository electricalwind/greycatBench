package greycat.bench.graphgen;

public class BasicGraphGenerator implements GraphGenerator {

    private final int _nbNodes;
    private final int _nbSplit;
    private final int _numberOfModification;
    private final int _startPositionOfModification;
    private final int _numberOfNodesToChange;
    private final int _maxCase;
    private final int _sizeArray;
    private final int _percentageOfModification;
    private final int _offset;
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
     * @param offset                      to add to the nodes Id
     */
    public BasicGraphGenerator(int nbNodes, int percentageOfModification, int nbSplit, int numberOfModification, int startPositionOfModification, int offset) {

        if (percentageOfModification < 0 || percentageOfModification > 100) {
            throw new RuntimeException("A percentage should be between 0 et 100");
        }

        // User Information
        this._nbNodes = nbNodes;
        this._nbSplit = nbSplit;
        this._numberOfModification = numberOfModification;
        this._startPositionOfModification = startPositionOfModification;
        this._percentageOfModification = percentageOfModification;
        this._offset = offset;

        //computed Information
        this._numberOfNodesToChange = percentageOfModification * nbNodes / 100;
        this._nbInsert = nbNodes / 10;
        if (nbNodes % 10 != 0) {
            this._nbInsert++;
        }
        this._maxCase = _numberOfNodesToChange / _nbSplit;
        this._sizeArray = _maxCase;


        if (startPositionOfModification + _numberOfNodesToChange > nbNodes && _numberOfNodesToChange != 0)
            throw new RuntimeException("startposition to close to the number of nodes");
    }

    @Override
    public int get_nbInsert() {
        return _nbInsert;
    }

    @Override
    public int get_percentageOfModification() {
        return _percentageOfModification;
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
            listOfNodes = new int[]{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
            for (int i = 0; i < 10; i++) {
                int toAdd = timeStamp * 10 + i;
                if (toAdd < _nbNodes)
                    listOfNodes[i] = timeStamp * 10 + i + _offset;
            }
        } else {

            if (_numberOfNodesToChange == 0) return null;
            insert = false;

            int modificationStatus = (timeStamp - _nbInsert) / _nbSplit;
            if (modificationStatus > _numberOfModification) return null;

            if (_nbSplit == 1) {
                listOfNodes = new int[_sizeArray];
                for (int i = 0; i < _sizeArray; i++) {
                    listOfNodes[i] = _startPositionOfModification + i + _offset;
                }
            } else {


                int splitNumber = (timeStamp - _nbInsert) % _nbSplit;

                if (splitNumber < _numberOfNodesToChange % _nbSplit) {
                    listOfNodes = new int[_sizeArray + 1];
                    listOfNodes[_sizeArray] = _startPositionOfModification + _numberOfNodesToChange - splitNumber - 1;
                } else {
                    listOfNodes = new int[_sizeArray];
                }

                int cas = modificationStatus % _maxCase + 1;

                int subsize = 0;
                int nodeToAdd = _startPositionOfModification + splitNumber * cas;

                int startOfProblem = _numberOfNodesToChange / (cas * _nbSplit);
                int numberOfProblem = _numberOfNodesToChange % (cas * _nbSplit);
                int problemCase = numberOfProblem / _nbSplit;
                if (numberOfProblem % _nbSplit != 0) System.out.println("alert");
                int positionProblem = _startPositionOfModification + (startOfProblem * cas * _nbSplit);


                for (int i = 0; i < _sizeArray; i++) {
                    listOfNodes[i] = nodeToAdd + _offset;
                    subsize++;
                    if (subsize == cas && _nbSplit != 1) {
                        subsize = 0;
                        int newPosition = nodeToAdd + ((_nbSplit - 1) * cas) + 1;
                        if (newPosition < positionProblem)
                            nodeToAdd = newPosition;
                        else {
                            nodeToAdd = positionProblem + (splitNumber * problemCase);
                            cas = problemCase;
                        }

                    } else {
                        nodeToAdd++;
                    }

                }
            }

        }
        return new Operations(insert, listOfNodes);
    }

    //Getter

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

    public int getOffset() {
        return _offset;
    }

    @Override
    public String toString() {
        return _nbNodes + "_" + _percentageOfModification + "_" + _nbSplit + "_" + _numberOfModification;
    }
}
