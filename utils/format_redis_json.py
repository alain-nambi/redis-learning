# Convert the Redis data to JSON-serializable format
def redis_to_json(transaction_data: dict) -> dict:
    """
    Convert Redis transaction data to a JSON-serializable format.

    Args:
        transaction_data (dict): The transaction data retrieved from Redis.

    Returns:
        dict: A JSON-serializable dictionary of the transaction data.
    """
    # Decode byte values to UTF-8 strings and convert timestamps
    return {
        key.decode("utf-8"): (
            value.decode("utf-8") if isinstance(value, bytes) else value
        )
        for key, value in transaction_data.items()
    }
