package task4;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PersonalRankEngine {

    private PersonalRankEngine() {
    }

    public static JavaPairRDD<String, Double> run(JavaSparkContext sc,
            JavaPairRDD<String, List<String>> adjacency,
            String targetUserId,
            double alpha,
            int maxIter,
            double epsilon) {
        String targetNodeId = PersonalRankGraph.USER_PREFIX + targetUserId;
        // Start the random walk with all probability mass on the target user node.
        JavaPairRDD<String, Double> currentRanks = sc
                .parallelizePairs(Collections.singletonList(new Tuple2<>(targetNodeId, 1.0))).cache();
        Map<String, Double> currentRankMap = currentRanks.collectAsMap();

        for (int iteration = 0; iteration < maxIter; iteration++) {
            // Propagate each node's rank equally to its neighbors.
            JavaPairRDD<String, Double> contributions = adjacency
                    .join(currentRanks)
                    .flatMapToPair(tuple -> {
                        List<String> neighbors = tuple._2._1;
                        double rank = tuple._2._2;
                        List<Tuple2<String, Double>> outputs = new ArrayList<>();
                        if (!neighbors.isEmpty()) {
                            double share = rank / neighbors.size();
                            for (String neighbor : neighbors) {
                                outputs.add(new Tuple2<>(neighbor, share));
                            }
                        }
                        return outputs.iterator();
                    });

            // Apply the restart factor and add the target user's restart probability back in.
            JavaPairRDD<String, Double> nextRanks = contributions
                    .reduceByKey(Double::sum)
                    .mapValues(value -> alpha * value)
                    .union(sc.parallelizePairs(Collections.singletonList(new Tuple2<>(targetNodeId, 1.0 - alpha))))
                    .reduceByKey(Double::sum)
                    .cache();

            // Stop once the rank vector changes only within the configured tolerance.
            Map<String, Double> nextRankMap = nextRanks.collectAsMap();
            double maxShift = maxShift(currentRankMap, nextRankMap);

            currentRanks.unpersist(false);
            currentRanks = nextRanks;
            currentRankMap = nextRankMap;

            if (maxShift <= epsilon) {
                break;
            }
        }

        return currentRanks;
    }

    private static double maxShift(Map<String, Double> previous, Map<String, Double> current) {
        // Compare both maps over the union of keys so new nodes are handled correctly.
        Set<String> keys = new HashSet<>();
        keys.addAll(previous.keySet());
        keys.addAll(current.keySet());

        double maxShift = 0.0;
        for (String key : keys) {
            double oldValue = previous.getOrDefault(key, 0.0);
            double newValue = current.getOrDefault(key, 0.0);
            double shift = Math.abs(oldValue - newValue);
            if (shift > maxShift) {
                maxShift = shift;
            }
        }
        return maxShift;
    }
}