package utils;

import com.esotericsoftware.kryo.Kryo;

import java.util.HashMap;
import java.util.HashSet;

public class KryoUtils {
    private static final Kryo kryo;

    static {
        kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);

        kryo.register(HashMap.class);
        kryo.register(HashSet.class);
        //kryo.register(Set.class, new java.lang.reflect.TypeToken<Set<String>>() {}.getType());
    }

    public static Kryo getKryoInstance() {
        return kryo;
    }
}

