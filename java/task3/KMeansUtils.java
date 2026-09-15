package task3;

public final class KMeansUtils {
    private KMeansUtils() {
    }

    public static double distance(double[] left, double[] right) {
        double sum = 0.0;
        for (int i = 0; i < left.length; i++) {
            double diff = left[i] - right[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public static double[] copy(double[] source) {
        double[] target = new double[source.length];
        System.arraycopy(source, 0, target, 0, source.length);
        return target;
    }

    public static double[][] copyCenters(double[][] centers) {
        // Deep-copy the center matrix so broadcasted arrays are never mutated in place.
        double[][] copy = new double[centers.length][];
        for (int i = 0; i < centers.length; i++) {
            copy[i] = copy(centers[i]);
        }
        return copy;
    }

    public static double[] average(double[] sum, long count) {
        double[] center = new double[sum.length];
        for (int i = 0; i < sum.length; i++) {
            center[i] = sum[i] / count;
        }
        return center;
    }

    public static String formatCenter(double[] center) {
        return String.format("%.4f,%.4f,%.4f", center[0], center[1], center[2]);
    }

    public static int findNearestCluster(double[] features, double[][] centers) {
        // Scan all centers and return the index of the closest one by Euclidean distance.
        int bestCluster = 0;
        double bestDistance = distance(features, centers[0]);
        for (int clusterId = 1; clusterId < centers.length; clusterId++) {
            double distance = distance(features, centers[clusterId]);
            if (distance < bestDistance) {
                bestDistance = distance;
                bestCluster = clusterId;
            }
        }
        return bestCluster;
    }
}