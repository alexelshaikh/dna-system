package utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BiMap<S, T> {
    private final Map<S, T> forwardMap;
    private final Map<T, S> reverseMap;

    private BiMap(Map<S, T> forwardMap, Map<T, S> reverseMap) {
        this.forwardMap = forwardMap;
        this.reverseMap = reverseMap;
    }

    public BiMap() {
        forwardMap = new HashMap<>();
        reverseMap = new HashMap<>();
    }

    public void put(S s, T t) {
        forwardMap.put(s, t);
        reverseMap.put(t, s);
    }

    public T get(S key) {
        return forwardMap.get(key);
    }
    public S getReverse(T key) {
        return reverseMap.get(key);
    }
    public Map<S, T> getForwardMap() {
        return forwardMap;
    }
    public Map<T, S> getReverseMap() {
        return reverseMap;
    }

    public BiMap<S, T> unmodifiableMap() {
        return new BiMap<>(Collections.unmodifiableMap(forwardMap), Collections.unmodifiableMap(reverseMap));
    }
}
