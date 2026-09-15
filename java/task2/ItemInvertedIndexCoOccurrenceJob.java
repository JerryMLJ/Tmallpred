package task2;

import common.BehaviorParser;
import common.BehaviorRecord;
import common.SingleFileOutputWriter;
import common.constants.BehaviorTypes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ItemInvertedIndexCoOccurrenceJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ItemInvertedIndexCoOccurrenceJob <inputPath> <outputDir>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputDir = args[1];

        SparkConf conf = new SparkConf().setAppName("Task2_InvertedIndex_CoOccurrence");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> raw = sc.textFile(inputPath);

            // Keep only deep interaction events; all downstream results are based on them.
            JavaRDD<BehaviorRecord> records = raw
                    .map(line -> BehaviorParser.parse(line, BehaviorTypes.DEEP))
                    .filter(r -> r != null)
                    .cache();

            // Build a deduplicated item -> user mapping, shared by both outputs.
            JavaPairRDD<String, String> itemUserPairs = records
                    .mapToPair(r -> new Tuple2<>(r.itemId, r.userId))
                    .distinct()
                    .cache();

            // Inverted index: each item maps to an ordered list of users.
            JavaRDD<String> invertedIndex = itemUserPairs
                    .groupByKey()
                    .mapToPair(tuple -> {
                        List<String> users = new ArrayList<>();
                        for (String user : tuple._2) {
                            users.add(user);
                        }
                        // itemUserPairs is already deduplicated, so just sort the users
                        users.sort(ItemIdOrdering::compare);
                        return new Tuple2<>(tuple._1, String.join(",", users));
                    })
                    .sortByKey(true)
                    .map(tuple -> tuple._1 + "\t" + tuple._2);

            SingleFileOutputWriter.write(invertedIndex, sc, outputDir + "/item_user_inverted_index.txt");

            // Reverse to user -> item and count pairwise co-occurrences.
            JavaPairRDD<String, String> userItemPairs = itemUserPairs
                    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .distinct()
                    .cache();

            // For each user, generate item pairs and aggregate their counts.
            JavaRDD<String> coOccurrence = userItemPairs
                    .groupByKey()
                    .flatMapToPair(tuple -> {
                        List<String> items = new ArrayList<>();
                        for (String item : tuple._2) {
                            items.add(item);
                        }
                        items.sort(ItemIdOrdering::compare);

                        List<Tuple2<String, Integer>> pairs = new ArrayList<>();
                        for (int i = 0; i < items.size(); i++) {
                            for (int j = i + 1; j < items.size(); j++) {
                                String left = items.get(i);
                                String right = items.get(j);
                                pairs.add(new Tuple2<>(left + "," + right, 1));
                            }
                        }
                        return pairs.iterator();
                    })
                    .reduceByKey(Integer::sum)
                    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false)
                    .map(tuple -> tuple._2 + "\t" + tuple._1);

            // Only keep the top 1000 co-occurrence pairs after sorting by count descending.
            SingleFileOutputWriter.write(sc.parallelize(coOccurrence.take(1000)), sc,
                    outputDir + "/item_co_occurrence.txt");
        }
    }
}
