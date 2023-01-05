package core;

import utils.Pair;

public class Attribute<T> extends Pair<String, T> {

    public Attribute(String name, T value) {
        super(name, value);
    }

    public Attribute(String name) {
        this(name, null);
    }

    public String getName() {
        return getT1();
    }
    public T getValue() {
        return getT2();
    }

    public Attribute<T> newValue(T newValue) {
        return new Attribute<>(getName(), newValue);
    }

    @Override
    public String toString() {
        return getName() + " -> " + getValue().toString();
    }

    @Override
    public Attribute<T> clone() {
        return new Attribute<>(getName(), getValue());
    }
}
