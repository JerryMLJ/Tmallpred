package task1;

import common.BehaviorRecord;

import java.io.Serializable;

public class UserStats implements Serializable {
    // `lastTs`: timestamp of the user's last action (raw). F1 is computed later as
    // (globalMaxTs - lastTs) / 86400.0 to get days difference (freshness).
    // `count`: interaction count (F2)
    // `score`: aggregated score by behavior weights (F3)
    public long lastTs;
    public long count;
    public long score;

    public UserStats(long lastTs, long count, long score) {
        this.lastTs = lastTs;
        this.count = count;
        this.score = score;
    }

    public static UserStats fromRecord(BehaviorRecord r) {
        int s = 1;
        switch (r.type) {
            case "buy":
                s = 4;
                break;
            case "cart":
                s = 3;
                break;
            case "fav":
                s = 2;
                break;
            case "pv":
                s = 1;
                break;
        }
        return new UserStats(r.timestamp, 1, s);
    }

    public UserStats merge(UserStats o) {
        long mergedLastTs = Math.max(this.lastTs, o.lastTs);
        long mergedCount = this.count + o.count;
        long mergedScore = this.score + o.score;
        return new UserStats(mergedLastTs, mergedCount, mergedScore);
    }
}
