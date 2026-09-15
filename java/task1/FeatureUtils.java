package task1;

public class FeatureUtils {
    // compute raw features: F1 (days since last action), F2 (count), F3 (score)
    public static double[] computeRaw(UserStats us, long globalMaxTs) {
        double f1 = (globalMaxTs - us.lastTs) / 86400.0;
        double f2 = us.count;
        double f3 = us.score;
        return new double[] { f1, f2, f3 };
    }

    public static double[] normalize(double[] v, double[] min, double[] max) {
        double[] out = new double[3];
        for (int i = 0; i < 3; i++) {
            if (max[i] > min[i]) {
                out[i] = (v[i] - min[i]) / (max[i] - min[i]);
            } else {
                out[i] = 0.0;
            }
        }
        return out;
    }
}
