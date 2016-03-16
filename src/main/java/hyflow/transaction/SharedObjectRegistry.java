package hyflow.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by balajiarun on 3/7/16.
 */
public class SharedObjectRegistry {

    private ConcurrentMap<Integer, AbstractObject> registry;
    private int capacity;

    public SharedObjectRegistry(int capacity) {
        this.registry = new ConcurrentHashMap<Integer, AbstractObject>(capacity);
        this.capacity= capacity;
    }

    public void registerObjects(int id, AbstractObject object) {
        registry.put(id, object);
    }

    public AbstractObject getObject(int id) {
        return registry.get(id);
    }

    public int getCapacity() {
        return registry.size();
    }

}
