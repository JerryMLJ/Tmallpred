package common;

import java.io.Serializable;

public class BehaviorRecord implements Serializable {
    public String userId;
    public String itemId;
    public String categoryId;
    public String type;
    public long timestamp;

    public BehaviorRecord(String userId, String itemId, String categoryId, String type, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.type = type;
        this.timestamp = timestamp;
    }

    public String toCsvLine() {
        return String.format("%s,%s,%s,%s,%d", userId, itemId, categoryId, type, timestamp);
    }
}