"""Run the Spark job locally and check it against counts computed independently.

Usage: python analysis/test_local.py <path/to/twitter_combined.txt>
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent))
from follow_counts import compute_counts  # noqa: E402

# Computed with sort -u | awk over the raw file (self-follows excluded).
EXPECTED = {
    "214328887": (180, 214),
    "107830991": (180, 157),
}


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: test_local.py <path/to/twitter_combined.txt>")

    spark = SparkSession.builder.master("local[*]").appName("follow-counts-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    counts = compute_counts(spark, Path(sys.argv[1]).resolve().as_uri())

    rows = {r.user_id: (r.followers, r.followees) for r in counts.filter(counts.user_id.isin(*EXPECTED)).collect()}
    print(f"total users: {counts.count()}")
    failed = False
    for user, expected in EXPECTED.items():
        got = rows.get(user)
        status = "OK" if got == expected else "MISMATCH"
        failed |= got != expected
        print(f"{status}: {user} followers/followees expected {expected}, got {got}")

    spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
