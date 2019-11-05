package org.infinispan.crucial;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A Creson shared object implementing a barrier.
 * <p>
 * Date: 2018-05-21
 *
 * @author Pierre, Gerard
 */
public class CCyclicBarrier implements Externalizable {

    /**
     * The number of parties
     */
    private int parties;
    /**
     * The current generation
     */
    private Generation generation = new Generation();
    /**
     * Number of parties still waiting. Counts down from parties to 0
     * on each generation.  It is reset to parties on each new
     * generation or when broken.
     */
    private int count;

    public CCyclicBarrier() {}

    public CCyclicBarrier(int parties) {
        this.parties = parties;
        this.count = parties;

    }

    public synchronized int await() {
        final Generation g = generation;
        int index = --count;

        if (index == 0) { // tripped
            nextGeneration();
            return 0;
        } else {
            while (g == generation) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return index;
        }
    }

    /**
     * Updates state on barrier trip and wakes up everyone.
     * Called only while holding lock.
     */
    private void nextGeneration() {
        // signal completion of last generation
        this.notifyAll();
        // set up next generation
        count = parties;
        generation = new Generation();
    }

    /**
     * Returns the number of parties required to trip this barrier.
     *
     * @return the number of parties required to trip this barrier
     */
    public int getParties() {
        return parties;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(parties);
        out.writeInt(count);
        out.writeObject(generation);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        parties = in.readInt();
        count = in.readInt();
        generation = (Generation) in.readObject();
    }

    /**
     * Each use of the barrier is represented as a generation instance.
     * The generation changes whenever the barrier is tripped, or
     * is reset.
     */
    private static class Generation {
    }
}
