package task4;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class PersonalRankRecommendationFormatter {

    private PersonalRankRecommendationFormatter() {
    }

    public static JavaPairRDD<String, Double> filterItemRanks(JavaPairRDD<String, Double> currentRanks,
            JavaPairRDD<String, Integer> itemNodes,
            Set<String> seenItems) {
        // Keep only item nodes and remove any item the target user has already seen.
        return currentRanks
                .join(itemNodes)
                .mapToPair(tuple -> new Tuple2<>(stripItemPrefix(tuple._1), tuple._2._1))
                .filter(tuple -> !seenItems.contains(tuple._1));
    }

    public static String formatRecommendationLine(String targetUserId, List<Tuple2<Double, String>> topItems) {
        // Format as item:score pairs so the output stays compact and easy to parse.
        List<String> recommendationParts = new ArrayList<>();
        for (Tuple2<Double, String> item : topItems) {
            recommendationParts.add(item._2 + ":" + String.format("%.3f", item._1));
        }
        return targetUserId + "\t" + String.join(",", recommendationParts);
    }

    private static String stripItemPrefix(String nodeId) {
        // Strip the graph prefix before writing item ids back to the output file.
        if (nodeId.startsWith(PersonalRankGraph.ITEM_PREFIX)) {
            return nodeId.substring(PersonalRankGraph.ITEM_PREFIX.length());
        }
        return nodeId;
    }
}