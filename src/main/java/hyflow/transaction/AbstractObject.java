package hyflow.transaction;

/**
 * Created by balajiarun on 3/7/16.
 */
public abstract class AbstractObject {
    private long version;

    public AbstractObject() {
        version = -1;
    }

    private long getVersion() {
        return version;
    }

    private void setVersion(long nextversion) {
        version = nextversion;
    }

    private void incrementVersion() {
        version += 1;
    }

}

