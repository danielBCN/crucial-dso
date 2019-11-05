package org.infinispan.crucial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Daniel
 */
public class CLogger implements Externalizable {
    private String name;

    public CLogger() {}

    public CLogger(String name) {
        this.name = name;
    }

    public void print(String out) {
        System.out.println("[" + name + "-log] " + out);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(name);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = (String) in.readObject();
    }
}
