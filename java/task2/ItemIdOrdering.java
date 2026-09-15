package task2;

public final class ItemIdOrdering {
    private ItemIdOrdering() {
    }

    public static int compare(String left, String right) {
        Long leftNum = parseLong(left);
        Long rightNum = parseLong(right);
        if (leftNum != null && rightNum != null) {
            return Long.compare(leftNum, rightNum);
        }
        return left.compareTo(right);
    }

    private static Long parseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
