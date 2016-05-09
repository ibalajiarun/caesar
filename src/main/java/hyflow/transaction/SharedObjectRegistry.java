package hyflow.transaction;

/**
 * Created by balajiarun on 3/7/16.
 */
public class SharedObjectRegistry {

    private final AbstractObject[] registry;

    public SharedObjectRegistry(int capacity) {
        this.registry = new AbstractObject[capacity];
    }

    public void registerObjects(int id, AbstractObject object) {
        registry[id] = object;
    }

    public AbstractObject getObject(int id) {
        return registry[id];
    }

}
