# Imports
import json
import os
import re
import time
from abc import ABC, abstractmethod
from collections import Counter
from itertools import chain

import numpy as np
import pandas as pd
import psutil
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    collect_list,
    expr,
    lag,
    pandas_udf,
    regexp_extract,
    row_number,
    to_timestamp,
    udf,
    unix_timestamp,
    when,
)
from pyspark.sql.types import ArrayType, StringType
from utils import check_parseDF, print_section

# Column patterns for regex extraction
COL_PATTERNS = {
    "date_time": r"^(\d{6} \d{6})",
    "pid": r"^(?:\d{6} \d{6}) (\d+)",
    "level": r"^(?:\d{6} \d{6}) (?:\d+) (\w+)",
    "component": r"^(?:\d{6} \d{6}) (?:\d+) (?:\w+) ([^:]+)",
    "BlockId": r"(blk_-?\d+)",
}


# Base abstract class
class Pipe(ABC):
    def __init__(self, data_dir="data/"):
        self.data_dir = data_dir
        self.metrics = {}
        self.logs = None
        self.anomalies_df = None
        self.actual_events = None
        self.event_markers = None

    def display_metrics(self):
        print(
            json.dumps(
                {
                    k: round(v, 4) if isinstance(v, (int, float)) else v
                    for k, v in self.metrics.items()
                },
                indent=4,
            )
        )

    @staticmethod
    def mem_metrics(metrics_dict, stage_name):
        mem = psutil.virtual_memory()
        metrics_dict[f"{stage_name}_gbtot"] = mem.total / (1024**3)
        metrics_dict[f"{stage_name}_gbused"] = mem.used / (1024**3)
        metrics_dict[f"{stage_name}_mempct"] = mem.percent

    def get_event_markers(self, spark):
        """Get event markers dictionary from templates file."""
        templates_df = (
            spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(os.path.join(self.data_dir, "HDFS.log_templates.csv"))
        )

        # Extract words from templates
        def extract_words(template):
            words = [
                w
                for part in re.split(r"\[\*\]", template)
                for w in re.findall(r"[\w\.:]+", part)
                if w
            ]
            return ["BLOCK*" if w == "BLOCK" else w for w in words]

        # Extract markers from each template
        template_rows = templates_df.select("EventId", "EventTemplate").collect()
        self.event_markers = {
            row["EventId"]: extract_words(row["EventTemplate"]) for row in template_rows
        }

        return self.event_markers

    @abstractmethod
    def load(self):
        """Load data from source."""
        pass

    @abstractmethod
    def parse(self):
        """Parse loaded data."""
        pass

    @abstractmethod
    def group(self, mode=True, partition_strategy=None):
        """Group data into meaningful chunks."""
        pass

    @abstractmethod
    def pipeline(self):
        """Execute full data processing pipeline."""
        pass


# Concrete implementation
class PipeDF(Pipe):
    def __init__(self, spark, data_dir="data/"):
        super().__init__(data_dir)
        self.spark = spark
        self.parsed_df = None
        self.blocks_df = None

    def load(self):
        start = time.time()
        print_section("STEP 1: Loading Data")

        # Get event markers
        self.get_event_markers(self.spark)

        # Load logs
        self.logs = self.spark.read.text(os.path.join(self.data_dir, "HDFS.log"))

        # Load events from NPZ file
        self.actual_events = dict(
            Counter(
                chain.from_iterable(
                    np.load(os.path.join(self.data_dir, "HDFS.npz"), allow_pickle=True)[
                        "x_data"
                    ]
                )
            )
        )

        # Load anomalies
        self.anomalies_df = (
            self.spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(os.path.join(self.data_dir, "anomaly_label.csv"))
            .withColumn(
                "Label",
                when(col("Label") == "Normal", 0)
                .when(col("Label") == "Anomaly", 1)
                .otherwise(col("Label")),
            )
        )

        self.logs.show(3, truncate=False)
        self.metrics["df_load_time"] = time.time() - start
        self.mem_metrics(self.metrics, "df_load")

        return self.logs, self.anomalies_df, self.actual_events

    def parse(self):
        start = time.time()
        print_section("STEP 2: Parse Logs to Extract and Encode Fields")

        # Convert event templates to dictionary for easy lookup in UDF
        bc_event_markers = self.spark.sparkContext.broadcast(self.event_markers)

        @pandas_udf(StringType())
        def assign_event(series: pd.Series) -> pd.Series:
            """Match log messages to event markers from templates."""
            results = []

            for log_message in series:
                # Default to unknown
                event_id = "Unknown"

                # Try to match each event's marker words
                for event, markers in bc_event_markers.value.items():
                    if all(log_message.find(marker) != -1 for marker in markers):
                        event_id = event
                        break

                results.append(event_id)

            return pd.Series(results)

        # Parse log data and set directly to class attribute
        self.parsed_df = (
            self.logs.withColumn("EventId", assign_event(col("value")))
            # Add all regex extracted columns
            .select(
                *[
                    regexp_extract(col("value"), pattern, 1).alias(field)
                    for field, pattern in COL_PATTERNS.items()
                ],
                "value",
                "EventId",
            )
            # Add timestamp from extracted date
            .withColumn("timestamp", to_timestamp(col("date_time"), "yyMMdd HHmmss"))
        )

        # Run validation checks
        check_parseDF(self.parsed_df, self.actual_events)

        # Show sample data
        self.parsed_df.show(5, truncate=False)

        # Record metrics
        self.metrics["df_parse_time"] = time.time() - start
        self.mem_metrics(self.metrics, "df_parse")

        return self.parsed_df

    def group(self, mode=True, partition_strategy=None):
        start = time.time()

        # Add sequence numbers and calculate intervals in chained operations
        processed_df = (
            self.parsed_df.withColumn(
                "seq_num",
                row_number().over(Window.partitionBy("BlockId").orderBy("timestamp")),
            )
            .withColumn(
                "prev_timestamp",
                lag("timestamp", 1).over(
                    Window.partitionBy("BlockId").orderBy("seq_num")
                ),
            )
            .withColumn(
                "interval_seconds",
                when(col("prev_timestamp").isNull(), 0).otherwise(
                    unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
                ),
            )
        )

        # Group by block ID and collect data
        block_events = (
            processed_df.groupBy("BlockId")
            .agg(
                collect_list(expr("struct(seq_num, EventId, interval_seconds)")).alias(
                    "event_data"
                ),
                collect_list("level").alias("levels"),
                collect_list("component").alias("components"),
                collect_list("pid").alias("pids"),
            )
            # Extract events and intervals from structured data
            .withColumn(
                "events",
                expr("TRANSFORM(SORT_ARRAY(event_data, true), x -> x.EventId)"),
            )
            .withColumn(
                "intervals",
                expr(
                    "TRANSFORM(SORT_ARRAY(event_data, true), x -> x.interval_seconds)"
                ),
            )
            # Calculate total latency (sum of all intervals)
            .withColumn(
                "latency",
                expr("aggregate(intervals, CAST(0 as BIGINT), (acc, x) -> acc + x)"),
            )
            # Join with anomaly labels - using simple equijoin syntax
            .join(self.anomalies_df, "BlockId", "left")
        )

        # Store final result with selected columns
        self.blocks_df = block_events.select(
            "BlockId",
            "events",
            "intervals",
            "latency",
            "levels",
            "components",
            "pids",
            "Label",
        )

        # Display and record metrics
        self.blocks_df.show(5, truncate=True)
        self.metrics["df_group_time"] = time.time() - start
        self.mem_metrics(self.metrics, "df_group")

        return self.blocks_df

    def pipeline(self):
        """Run the full data processing pipeline."""
        self.load()
        self.parse()
        self.group()
        return self


class PipeRDD(Pipe):
    def __init__(self, spark, data_dir="data/"):
        super().__init__(data_dir)
        self.spark = spark

        # Data containers
        self.logs_rdd = None
        self.anomalies_rdd = None
        self.actual_events = None
        self.parsed_rdd = None
        self.blocks_rdd = None
        self.event_markers = None

    def load(self):
        start_time = time.time()
        print_section("STEP 1: Loading Data (RDD Implementation)")

        # Get event markers
        self.get_event_markers(self.spark)

        # Load log file as RDD
        self.logs_rdd = self.spark.sparkContext.textFile(
            os.path.join(self.data_dir, "HDFS.log")
        )

        # Load actual events from NPZ file
        self.actual_events = dict(
            Counter(
                chain.from_iterable(
                    np.load(os.path.join(self.data_dir, "HDFS.npz"), allow_pickle=True)[
                        "x_data"
                    ]
                )
            )
        )

        # Load anomalies CSV and convert to RDD
        anomalies_df = (
            self.spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(os.path.join(self.data_dir, "anomaly_label.csv"))
        )

        # Convert DataFrame to RDD with (BlockId, Label) pairs
        self.anomalies_rdd = anomalies_df.rdd.map(
            lambda row: (row["BlockId"], 1 if row["Label"] == "Anomaly" else 0)
        )

        # Show first few log entries
        print("First 3 Log Entries:")
        for line in self.logs_rdd.take(3):
            print(line)

        # Record loading metrics
        self.metrics["rdd_load_time"] = time.time() - start_time
        self.mem_metrics(self.metrics, "rdd_load")

        return self.logs_rdd, self.anomalies_rdd, self.actual_events

    def parse(self):
        start = time.time()
        print_section(
            "STEP 2: Parse Logs to Extract and Encode Fields (RDD Implementation)"
        )

        # Broadcast patterns for efficient access across nodes
        bc_event_markers = self.spark.sparkContext.broadcast(self.event_markers)
        bc_col_patterns = self.spark.sparkContext.broadcast(COL_PATTERNS)

        def parse_log_line(line):
            """Parse a single log line into components"""
            # Extract fields using regex patterns
            fields = {}
            for field, pattern in bc_col_patterns.value.items():
                match = re.search(pattern, line)
                fields[field] = match.group(1) if match else ""

            # Determine event type by matching markers
            event_id = "Unknown"
            for e_id, markers in bc_event_markers.value.items():
                if all(line.find(marker) != -1 for marker in markers):
                    event_id = e_id
                    break

            # Return the parsed log entry
            return (
                fields["BlockId"],
                fields["date_time"],
                fields["pid"],
                fields["level"],
                fields["component"],
                event_id,
                line,  # Keep original line for reference
            )

        # Apply parsing to logs and filter out entries with missing blockIDs
        parsed_rdd = self.logs_rdd.map(parse_log_line).filter(lambda x: x[0])

        # RDD Structure: (BlockId, date_time, pid, level, component, event_id, raw_line)
        self.parsed_rdd = parsed_rdd

        # Display sample of parsed data
        # print("\nSample of parsed RDD data:")
        # print(
        #     "+------------------------+-------------+-----+-------+----------------------------+-------+"
        # )
        # print(
        #     "|BlockId                 |date_time    |pid  |level  |component                   |EventId|"
        # )
        # print(
        #     "+------------------------+-------------+-----+-------+----------------------------+-------+"
        # )
        # for row in self.parsed_rdd.take(5):
        #     blk_id, date_time, pid, level, component, event_id, _ = row
        #     print(
        #         f"|{blk_id:<24}|{date_time:<13}|{pid:<5}|{level:<7}|{component:<28}|{event_id:<7}|"
        #     )
        # print(
        #     "+------------------------+-------------+-----+-------+----------------------------+-------+"
        # )

        # Record metrics
        self.metrics["rdd_parse_time"] = time.time() - start
        self.mem_metrics(self.metrics, "rdd_parse")

        return self.parsed_rdd

    def group(self, mode=True, partition_strategy=None):
        start = time.time()
        # print_section("STEP 3: GroupBy BlockID (RDD Implementation)")

        # Apply partitioning strategy if provided
        working_rdd = self.parsed_rdd

        # Convert date_time to timestamp for sorting
        def to_timestamp(date_time_str):
            if not date_time_str:
                return None
            try:
                # Format: "yyMMdd HHmmss"
                from datetime import datetime

                return datetime.strptime(date_time_str, "%y%m%d %H%M%S")
            except:
                return None

        # Add timestamp to each record
        timestamped_rdd = working_rdd.map(
            lambda x: (
                x[0],  # BlockId
                x[5],  # EventId
                to_timestamp(x[1]),  # timestamp
                x[2],  # pid
                x[3],  # level
                x[4],  # component
                x[6],  # raw_line
            )
        )

        # Group by BlockId
        grouped_rdd = timestamped_rdd.groupBy(lambda x: x[0])

        # Process each block group to calculate intervals, etc.
        def process_block_group(block_group):
            block_id, events_iter = block_group

            # Convert iterator to list for multiple passes
            events_list = list(events_iter)

            # Sort events by timestamp
            sorted_events = sorted(
                events_list, key=lambda x: x[2] if x[2] else datetime.min
            )

            # Extract data
            events = [e[1] for e in sorted_events]  # EventIds
            pids = [e[3] for e in sorted_events]  # PIDs
            levels = [e[4] for e in sorted_events]  # Levels
            components = [e[5] for e in sorted_events]  # Components

            # Calculate intervals
            intervals = [0]  # First event has no interval
            latency = 0
            for i in range(1, len(sorted_events)):
                current_time = sorted_events[i][2]
                prev_time = sorted_events[i - 1][2]

                if current_time and prev_time:
                    interval = int((current_time - prev_time).total_seconds())
                else:
                    interval = 0

                intervals.append(interval)
                latency += interval

            return (block_id, events, intervals, latency, levels, components, pids)

        processed_blocks = grouped_rdd.map(process_block_group)

        # Join with anomaly labels
        def join_with_labels(block_data, anomalies_list):
            block_id, events, intervals, latency, levels, components, pids = block_data

            # Find matching anomaly label or default to 0 (normal)
            label = 0
            for a_id, a_label in anomalies_list:
                if a_id == block_id:
                    label = a_label
                    break

            return (
                block_id,
                events,
                intervals,
                latency,
                levels,
                components,
                pids,
                label,
            )

        # Collect anomalies for local joining (more efficient than distributed join for this case)
        anomalies_list = self.anomalies_rdd.collect()
        bc_anomalies = self.spark.sparkContext.broadcast(anomalies_list)

        # Join blocks with anomaly labels
        self.blocks_rdd = processed_blocks.map(
            lambda block: join_with_labels(block, bc_anomalies.value)
        )

        # Show sample results
        # print("\nSample of grouped block data:")
        # print(
        #     "+------------------------+--------------------+--------------------+-------+--------------------+-----+"
        # )
        # print(
        #     "|BlockId                 |events              |intervals           |latency|components          |Label|"
        # )
        # print(
        #     "+------------------------+--------------------+--------------------+-------+--------------------+-----+"
        # )
        # for block in self.blocks_rdd.take(5):
        #     block_id, events, intervals, latency, levels, components, pids, label = (
        #         block
        #     )
        #     print(
        #         f"|{block_id:<24}|{str(events[:3]):<20}|{str(intervals[:3]):<20}|{latency:<7}|{str(components[:3]):<20}|{label:<5}|"
        #     )
        # print(
        #     "+------------------------+--------------------+--------------------+-------+--------------------+-----+"
        # )

        # Record metrics
        self.metrics["rdd_group_time"] = time.time() - start
        self.mem_metrics(self.metrics, "rdd_group")

        return self.blocks_rdd

    def pipeline(self):
        """Run the full RDD processing pipeline."""
        self.load()
        self.parse()
        self.group()
        return self
