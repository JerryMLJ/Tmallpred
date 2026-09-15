package common.constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class BehaviorTypes {

    public static final Set<String> ALL = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("pv", "cart", "fav", "buy")));
    public static final Set<String> DEEP = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("cart", "fav", "buy")));

    private BehaviorTypes() {
    }
}