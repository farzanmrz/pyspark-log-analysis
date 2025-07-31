import json

import psutil


def display_json(data):
    print(json.dumps(data, indent=4, sort_keys=False))


def check_parseDF(parsed_df, actual_events):
    # Check event counts
    event_counts = {
        row["EventId"]: row["count"]
        for row in parsed_df.groupBy("EventId").count().collect()
    }

    for event_id, expected in actual_events.items():
        if event_counts.get(event_id, 0) != expected:
            raise ValueError("Event Mismatch")

    # Check for missing blockIDs
    if (
        parsed_df.filter(
            (parsed_df.BlockId.isNull()) | (parsed_df.BlockId == "")
        ).count()
        > 0
    ):
        raise ValueError("Blocks Missing")

    return True


# Func to print section
def print_section(title, char="="):
    print(f"\n{char*80}\n{title}\n{char*80}")


# Func to print psutil metrics
def psutil_metrics(metrics_dict, stage_prefix):
    try:
        mem = psutil.virtual_memory()

        # Add memory metrics to dictionary
        metrics_dict[f"{stage_prefix}_mem_total_gb"] = mem.total / (1024**3)
        metrics_dict[f"{stage_prefix}_mem_used_gb"] = mem.used / (1024**3)
        metrics_dict[f"{stage_prefix}_mem_util_pct"] = mem.percent

    except ImportError:
        # Fallback if psutil isn't installed
        metrics_dict[f"{stage_prefix}_mem_status"] = "psutil not installed"

    return metrics_dict


# Funcs to print metrics memory
def mem_metrics(metrics_dict, prefix):
    """Record memory metrics with the given prefix."""
    mem = psutil.virtual_memory()
    metrics_dict[f"{prefix}_gbtot"] = mem.total / (1024**3)
    metrics_dict[f"{prefix}_gbused"] = mem.used / (1024**3)
    metrics_dict[f"{prefix}_mempct"] = mem.percent
    return metrics_dict
