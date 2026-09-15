package task4;

import common.BehaviorRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class PersonalRankGraph {

    static final String USER_PREFIX = "u:";
    static final String ITEM_PREFIX = "i:";

    private final JavaPairRDD<String, List<String>> adjacency;
    private final JavaPairRDD<String, Integer> itemNodes;
    private final Set<String> seenItems;

    private PersonalRankGraph(JavaPairRDD<String, List<String>> adjacency,
            JavaPairRDD<String, Integer> itemNodes,
            Set<String> seenItems) {
        this.adjacency = adjacency;
        this.itemNodes = itemNodes;
        this.seenItems = seenItems;
    }

    // Extract deduplicated (userId, itemId) pairs from behavior records. 
    public static JavaPairRDD<String, String> buildUserItemPairs(JavaRDD<BehaviorRecord> records) {
        return records
                .mapToPair(record -> new Tuple2<>(record.userId, record.itemId))
                .distinct()
                .cache();
    }

    // Build adjacency list from deduplicated user-item pairs. 
    public static JavaPairRDD<String, List<String>> buildAdjacency(JavaPairRDD<String, String> userItemPairs) {
        return userItemPairs
                .flatMapToPair(tuple -> {
                    List<Tuple2<String, String>> edges = new ArrayList<>(2);
                    String userNode = userNode(tuple._1);
                    String itemNode = itemNode(tuple._2);
                    edges.add(new Tuple2<>(userNode, itemNode));
                    edges.add(new Tuple2<>(itemNode, userNode));
                    return edges.iterator();
                })
                .distinct()
                .groupByKey()
                .mapToPair(tuple -> {
                    List<String> neighbors = new ArrayList<>();
                    for (String neighbor : tuple._2) {
                        neighbors.add(neighbor);
                    }
                    neighbors.sort(String::compareTo);
                    return new Tuple2<>(tuple._1, neighbors);
                })
                .cache();
    }

    // Build item node list from deduplicated user-item pairs. 
    public static JavaPairRDD<String, Integer> buildItemNodes(JavaPairRDD<String, String> userItemPairs) {
        return userItemPairs
                .mapToPair(tuple -> new Tuple2<>(itemNode(tuple._2), 1))
                .distinct()
                .cache();
    }

    // Collect the set of items a specific user has interacted with. 
    public static Set<String> collectSeenItems(JavaPairRDD<String, String> userItemPairs, String userId) {
        return new HashSet<>(userItemPairs
                .filter(tuple -> userId.equals(tuple._1))
                .map(tuple -> tuple._2)
                .collect());
    }

    public static PersonalRankGraph build(JavaRDD<BehaviorRecord> records, String targetUserId) {
        JavaPairRDD<String, String> userItemPairs = buildUserItemPairs(records);
        Set<String> seenItems = collectSeenItems(userItemPairs, targetUserId);
        JavaPairRDD<String, Integer> itemNodes = buildItemNodes(userItemPairs);
        JavaPairRDD<String, List<String>> adjacency = buildAdjacency(userItemPairs);
        userItemPairs.unpersist(false);
        return new PersonalRankGraph(adjacency, itemNodes, seenItems);
    }

    public JavaPairRDD<String, List<String>> getAdjacency() {
        return adjacency;
    }

    public JavaPairRDD<String, Integer> getItemNodes() {
        return itemNodes;
    }

    public Set<String> getSeenItems() {
        return seenItems;
    }

    public void close() {
        adjacency.unpersist(false);
        itemNodes.unpersist(false);
    }

    private static String userNode(String userId) {
        return USER_PREFIX + userId;
    }

    private static String itemNode(String itemId) {
        return ITEM_PREFIX + itemId;
    }
}