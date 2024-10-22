from datetime import datetime


def timestamp_datetime(timestamp: float) -> str:
    """
    Convert a timestamp to a human-readable datetime format.

    Args:
        timestamp (float): The timestamp to convert.

    Returns:
        str: A formatted datetime string.
    """
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
