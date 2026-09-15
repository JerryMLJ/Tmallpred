package common;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URI;

public final class SingleFileOutputWriter {
    private SingleFileOutputWriter() {
    }

    public static void write(JavaRDD<String> data, JavaSparkContext sc, String outputFile) throws Exception {
        String tempDir = outputFile + "_tmp";
        data.coalesce(1).saveAsTextFile(tempDir);

        FileSystem fs = FileSystem.get(new URI(sc.hadoopConfiguration().get("fs.defaultFS", "file:///")),
                sc.hadoopConfiguration());
        Path tempPath = new Path(tempDir);
        Path outputPath = new Path(outputFile);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileStatus[] statuses = fs.globStatus(new Path(tempDir + "/part-*"));
        if (statuses == null || statuses.length == 0) {
            throw new IllegalStateException("No part file found in " + tempDir);
        }
        if (statuses.length > 1) {
            throw new IllegalStateException(
                    "Expected exactly one part file in " + tempDir + ", but found " + statuses.length);
        }

        if (!fs.rename(statuses[0].getPath(), outputPath)) {
            throw new IllegalStateException("Failed to rename " + statuses[0].getPath() + " to " + outputPath);
        }

        fs.delete(tempPath, true);
    }
}
