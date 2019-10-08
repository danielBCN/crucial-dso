package org.infinispan.crucial;

import java.util.ArrayList;
import java.util.Map;

/**
 * CRUCIAL client to get instances of basic objects.
 *
 * @author Daniel
 */
public class CrucialClient{
    private static final String defaultServer = "127.0.0.1:11222";
    private static CrucialClient client;
    private Factory factory;


    private CrucialClient(String server){
        factory = Factory.get(server);
    }

    public static CrucialClient getClient(String server){
        if (client == null) {
            client = new CrucialClient(server);
        }
        return client;
    }

    public static CrucialClient getClient(){
        return getClient(defaultServer);
    }

    public CLogger getLog(String key){
        return factory.getInstanceOf(CLogger.class, key, true, false, key);
    }

    public <K, V> Map<K, V> getMap(String key){
        return factory.getInstanceOf(Map.class, key);
    }

    public <T> ArrayList<T> getArrayList(String key){
        return factory.getInstanceOf(ArrayList.class, key);
    }

    public <T> ArrayList<T> getArrayList(String key, int initialCapacity){
        return factory.getInstanceOf(ArrayList.class, key, true, true, initialCapacity);
    }

    public <T> CFuture<T> getCleanFuture(String key){
        return factory.getInstanceOf(CFuture.class, key, false, true);
    }

    public <T> CFuture<T> getFuture(String key){
        return factory.getInstanceOf(CFuture.class, key);
    }

    public CCyclicBarrier getCyclicBarrier(String key, int parties){
        return factory.getInstanceOf(CCyclicBarrier.class, key, false, true, parties);
    }

    public CCyclicBarrier getCyclicBarrier(String key){
        return factory.getInstanceOf(CCyclicBarrier.class, key);
    }

    public CSemaphore getSemaphore(String key, int permits){
        return factory.getInstanceOf(CSemaphore.class, key, false, true, permits);
    }

    public CSemaphore getSemaphore(String key){
        return factory.getInstanceOf(CSemaphore.class, key);
    }

    public CAtomicInt getAtomicInt(String key){
        return factory.getInstanceOf(CAtomicInt.class, key);
    }

    public CAtomicInt getAtomicInt(String key, int initialValue){
        return factory.getInstanceOf(CAtomicInt.class, key, false, true, initialValue);
    }

    public CAtomicLong getAtomicLong(String key){
        return factory.getInstanceOf(CAtomicLong.class, key);
    }

    public CAtomicByteArray getAtomicByteArray(String key){
        return factory.getInstanceOf(CAtomicByteArray.class, key);
    }

    public CAtomicBoolean getBoolean(String key, boolean initialValue){
        return factory.getInstanceOf(CAtomicBoolean.class, key, false, true, initialValue);
    }

    public CAtomicBoolean getBoolean(String key){
        return factory.getInstanceOf(CAtomicBoolean.class, key);
    }
}
