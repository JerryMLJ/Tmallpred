package task3;

import java.io.Serializable;

// Lightweight accumulator for one cluster's feature sums and sample count.
final class ClusterAggregate implements Serializable{

    final double[] sum;
    final long count;

    private ClusterAggregate(double[] sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    static ClusterAggregate from(double[] features) {
        return new ClusterAggregate(KMeansUtils.copy(features), 1L);
    }

    ClusterAggregate merge(ClusterAggregate other) {
        double[] mergedSum = new double[this.sum.length];
        for (int i = 0; i < this.sum.length; i++) {
            mergedSum[i] = this.sum[i] + other.sum[i];
        }
        return new ClusterAggregate(mergedSum, this.count + other.count);
    }
}