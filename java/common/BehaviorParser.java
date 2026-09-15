package common;

import java.util.Set;

import common.constants.BehaviorTypes;

public class BehaviorParser {
    public static final Set<String> DEFAULT_VALID_TYPES = BehaviorTypes.ALL;

    public static BehaviorRecord parse(String line) {
        return parse(line, DEFAULT_VALID_TYPES);
    }

    public static BehaviorRecord parse(String line, Set<String> validTypes) {
        if (line == null) {
            return null;
        }

        String s = line.trim();
        if (s.isEmpty()) {
            return null;
        }

        if (s.toLowerCase().startsWith("user_id")) {
            return null;
        }

        String[] p = s.split(",", -1);
        if (p.length < 5) {
            return null;
        }

        for (int i = 0; i < 5; i++) {
            if (p[i] == null || p[i].trim().isEmpty()) {
                return null;
            }
        }

        String type = p[3].trim();
        if (validTypes == null || !validTypes.contains(type)) {
            return null;
        }

        try {
            long ts = Long.parseLong(p[4].trim());
            return new BehaviorRecord(p[0].trim(), p[1].trim(), p[2].trim(), type, ts);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}