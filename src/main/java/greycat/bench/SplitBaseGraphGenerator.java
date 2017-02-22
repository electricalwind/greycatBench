package greycat.bench;

public class SplitBaseGraphGenerator {


    public static void main(String[] args) {

        for (int i = 0; i < 1000000; i++) {
            int[] arr = atTimestampDo(i, 10000, 10, 3, 20, 8999).get_arrayOfNodes();
        }
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
