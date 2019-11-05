package org.infinispan.crucial;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Daniel
 */
public class CSemaphore implements Externalizable {
    private int permits;

    public CSemaphore() {}

    public CSemaphore(int permits) {
        this.permits = permits;
    }

    public synchronized void acquire() throws InterruptedException {
        while (permits <= 0) this.wait();
        permits--;
    }

    public synchronized void release() {
        permits++;
        this.notify();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(permits);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        permits = in.readInt();
    }
}
