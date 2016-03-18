package hyflow.common;

public class SimpleIdGenerator implements IdGenerator {
    private final int step;
    private int current;

    public SimpleIdGenerator(int start, int step) {
        this.current = start;
        this.step = step;
    }

    public int next() {
        int ret = current;
        current += step;
        return ret;
    }
}
