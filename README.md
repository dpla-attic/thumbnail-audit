# Thumbnail Audit

Example use, from root directory:

    sbt package

Given that INPUT_PATH is a path to a parquet file that was the output from
https://github.com/dpla/analytics JsonDumpToJsonL:

    PATH_TO_SPARK/bin/spark-submit --class "la.dp.thumbnailaudit.Cleanup" \
      --master local[3] \
      /target/scala-2.11/thumbnail-audit_2.11-1.0.jar \
      INPUT_PATH PATH_FOR_CLEANED_DATA

Given that SAMPLE_SIZE is the desired sample size for each provider, e.g. 0.02:

    PATH_TO_SPARK/bin/spark-submit --class "la.dp.thumbnailaudit.Sample" \
      --master local[3] \
      /target/scala-2.11/thumbnail-audit_2.11-1.0.jar \
      PATH_FOR_CLEANED_DATA PATH_FOR_SAMPLE_DATA SAMPLE_SIZE
