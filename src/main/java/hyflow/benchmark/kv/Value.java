package hyflow.benchmark.kv;

import hyflow.transaction.AbstractObject;

/**
 * Created by balajiarun on 3/7/16.
 */
public class Value extends AbstractObject {

    private final int id;

    private int value;

    public Value(int id, int value) {
        super();
        this.value = value;
        this.id = id;
    }

    public int getId() {
        return this.id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}

