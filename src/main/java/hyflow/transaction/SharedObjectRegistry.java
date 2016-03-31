package hyflow.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by balajiarun on 3/7/16.
 */
public class SharedObjectRegistry {

    private final ConcurrentMap<Integer, AbstractObject> registry;

    public SharedObjectRegistry(int capacity) {
        this.registry = new ConcurrentHashMap<>(capacity);
    }

    public void registerObjects(int id, AbstractObject object) {
        registry.put(id, object);
    }

    public AbstractObject getObject(int id) {
        return registry.get(id);
    }

}
