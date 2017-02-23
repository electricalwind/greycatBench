package greycat.bench;

public interface GraphGenerator {
    public Operations nextTimeStamp();

    public int getOffset();

    public int get_nbSplit();
}
