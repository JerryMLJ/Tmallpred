package task3;

public class UserFeatureVector {
    public final String userId;
    public final double[] features;

    public UserFeatureVector(String userId, double[] features) {
        this.userId = userId;
        this.features = features;
    }

    public static UserFeatureVector parse(String line) {
        if (line == null) {
            return null;
        }

        String s = line.trim();
        if (s.isEmpty() || s.startsWith("user_id,")) {
            return null;
        }

        String[] parts = s.split(",", -1);
        if (parts.length < 4) {
            return null;
        }

        try {
            String userId = parts[0].trim();
            double f1 = Double.parseDouble(parts[1].trim());
            double f2 = Double.parseDouble(parts[2].trim());
            double f3 = Double.parseDouble(parts[3].trim());
            if (userId.isEmpty()) {
                return null;
            }
            return new UserFeatureVector(userId, new double[] { f1, f2, f3 });
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public String toLabelCsv(int clusterId) {
        return String.format("%s,%d", userId, clusterId);
    }
}