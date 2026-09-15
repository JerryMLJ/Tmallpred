package task4;

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

public class PersonalRankRecommendationJob {
    private static final double DEFAULT_ALPHA = 0.85;
    private static final int DEFAULT_MAX_ITER = 20;
    private static final double DEFAULT_EPSILON = 1.0e-6;
    private static final int TOP_K = 10;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage: PersonalRankRecommendationJob <inputPath> <targetUserId> <outputDir> [alpha] [maxIter] [epsilon]");
            System.exit(1);
        }

        String inputPath = args[0];
        String targetUserId = args[1];
        String outputDir = args[2];
        double alpha = args.length > 3 ? Double.parseDouble(args[3]) : DEFAULT_ALPHA;
        int maxIter = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_MAX_ITER;
        double epsilon = args.length > 5 ? Double.parseDouble(args[5]) : DEFAULT_EPSILON;

        SparkConf conf = new SparkConf().setAppName("Task4_PersonalRank_Recommendation");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<BehaviorRecord> records = sc.textFile(inputPath)
                    .map(line -> BehaviorParser.parse(line, BehaviorTypes.DEEP))
                    .filter(r -> r != null)
                    .cache();

            PersonalRankGraph graph = PersonalRankGraph.build(records, targetUserId);
            JavaPairRDD<String, Double> currentRanks = PersonalRankEngine.run(
                    sc, graph.getAdjacency(), targetUserId, alpha, maxIter, epsilon);

            JavaPairRDD<String, Double> itemRanks = PersonalRankRecommendationFormatter.filterItemRanks(
                    currentRanks, graph.getItemNodes(), graph.getSeenItems());

            List<Tuple2<Double, String>> topItems = itemRanks
                    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false)
                    .take(TOP_K);

            List<String> outputLines = new ArrayList<>();
            outputLines.add(PersonalRankRecommendationFormatter.formatRecommendationLine(targetUserId, topItems));
            SingleFileOutputWriter.write(sc.parallelize(outputLines), sc,
                    outputDir + "/personal_rank_recommendations.txt");

            currentRanks.unpersist(false);
            graph.close();
            records.unpersist(false);
        }
    }
}