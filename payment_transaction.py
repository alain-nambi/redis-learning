import redis
import uuid
import time
from datetime import datetime
from utils.transaction_status import TRANSACTIONS_STATUS
from utils.format_time import timestamp_datetime
from utils.format_redis_json import redis_to_json
import json
import logging


class TransactionManager:
    """
    TransactionManager handles the creation, updating, and retrieval of transactions in Redis.
    """

    def __init__(self, host='localhost', port=6379, db=0):
        """
        Initialize the connection to the Redis database.

        Args:
            host (str): Redis server hostname.
            port (int): Redis server port.
            db (int): Redis database index.
        """
        self.r = redis.Redis(host=host, port=port, db=db)

    def create_transaction(self, amount: float, user_id: int, status: str) -> dict:
        """
        Create a new transaction with a status of 'pending' by default.

        Args:
            amount (float): The transaction amount.
            user_id (int): The user ID associated with the transaction.

        Returns:
            dict: A dictionary containing the transaction details or None if an error occurs.
        """
        transaction_id = str(uuid.uuid4())
        transaction_data = {
            'user_id': user_id,
            'amount': amount,
            'status': status or 'pending',
            'timestamp': time.time()
        }

        try:
            # Store transaction data in Redis
            self.r.hset(f'transaction:{transaction_id}',
                        mapping=transaction_data)

            # Print transaction details
            print(f'''
------------------- Transaction Created -------------------
Transaction ID : {transaction_id}
User ID        : {user_id}
Amount         : {amount}
Status         : {transaction_data['status']}
Timestamp      : {timestamp_datetime(transaction_data['timestamp'])}
-----------------------------------------------------------
            ''')

            return transaction_data

        except Exception as e:
            print(f'Error creating transaction: {e}')
            return None

    def update_transaction(self, transaction_id: str, updated_status: str) -> json:
        """
        Update the status of a transaction in Redis.

        Args:
            transaction_id (str): The ID of the transaction to update.
            updated_status (str): The new status of the transaction.

        Returns:
            json: The updated transaction data in JSON format or None if an error occurs.
        """
        try:
            transaction = f'transaction:{transaction_id}'
            if self.r.exists(transaction):
                transaction_data = {
                    'status': updated_status,
                    'timestamp': time.time()
                }

                # Update the transaction status in Redis
                self.r.hset(transaction, mapping=transaction_data)

                # Fetch updated data
                transaction_data_updated = self.r.hgetall(transaction)
                redis_data_json = redis_to_json(transaction_data_updated)

                # Ensure keys exist before unpacking
                user_id = redis_data_json.get('user_id')
                amount = redis_data_json.get('amount')
                status = redis_data_json.get('status')
                timestamp = float(redis_data_json.get('timestamp'))

                if all(value is not None for value in (user_id, amount, status, timestamp)):
                    print(f'''
------------------- Transaction Updated -------------------
Transaction ID : {transaction_id}
User ID        : {user_id}
Amount         : {amount}
Status         : {status}
Datetime       : {timestamp_datetime(timestamp)}
-----------------------------------------------------------
                    ''')
                else:
                    print('Error: Not all values could be unpacked.')

                return redis_data_json

        except Exception as e:
            print(f'Error updating transaction: {str(e)}')
            return None

    def get_transaction_details(self, transaction_id) -> dict:
        """
        Retrieve a transaction from Redis using the transaction ID.

        Args:
            transaction_id (str): The ID of the transaction to retrieve.

        Returns:
            dict: A dictionary containing the transaction details or None if the transaction doesn't exist.
        """
        if self.r.exists(transaction_id):
            transaction_data = self.r.hgetall(transaction_id)
            transaction_data_json = redis_to_json(transaction_data)
            return transaction_data_json
        else:
            print(f'Transaction ID {transaction_id} does not exist.')
            return None

    def get_transactions(self) -> list:
        """
        Retrieve all transactions stored in Redis.

        This method fetches all keys from the Redis database and filters
        those that start with the 'transaction:' prefix. It returns a list
        of transaction IDs.

        Returns:
            list: A list containing all transaction IDs.
        """
        # Retrieve all keys from Redis
        transactions = self.r.keys()

        # Filter and collect transactions with the 'transaction:' prefix
        transaction_list = [
            transaction.decode('utf-8') for transaction in transactions
            if transaction.decode('utf-8').startswith('transaction:')
        ]

        if not transaction_list:
            print('No transactions found.')
        else:
            print(f"{len(transaction_list)} transactions found.")
            return transaction_list

    def delete_transactions(self) -> None:
        """
        Deletes all transactions from the Redis database.

        Retrieves all transactions using the `get_transactions` method.
        Iterates over each transaction, logs a confirmation message, 
        and deletes the transaction from Redis.

        Error handling is included to manage any exceptions during deletion.
        """
        # Retrieve the list of transactions
        transactions = self.get_transactions()

        # Check if there are transactions to delete
        if not transactions:
            print("No transactions found to delete.")
            return

        # Use a batch delete if possible (assuming self.r.delete can accept a list)
        try:
            # Log the number of transactions to be deleted
            print(f"Deleting {len(transactions)} transactions...")

            # Delete transactions in bulk
            self.r.delete(*transactions)

            # Log a confirmation message for each transaction deleted
            print("Transactions deleted successfully.")
        except Exception as e:
            print(f"An error occurred while deleting transactions: {e}")


# Example usage:
if __name__ == "__main__":
    import random

    # Initialize the transaction manager
    transaction_manager = TransactionManager()

    # Get status list from TRANSACTIONS_STATUS
    statuses = list(TRANSACTIONS_STATUS.values())

    # Create random transactions
    # for _ in range(1, 50):
    #     random_amount = random.randint(10000, 100000)
    #     random_user_id = str(uuid.uuid4())
    #     random_status = random.choice(statuses)

    #     # Create a new transaction
    #     create_transaction_data = transaction_manager.create_transaction(
    #         amount=random_amount,
    #         user_id=random_user_id,
    #         status=random_status
    #     )

    #     print(create_transaction_data)

    # # Update the transaction
    # update_transaction_data = transaction_manager.update_transaction(
    #     transaction_id='7380ba5c-257a-456d-b9e5-f54526c478e0',
    #     updated_status='completed'
    # )

    # # Retrieve the transaction
    # get_transaction_data = transaction_manager.get_transaction(
    #     transaction_id='7380ba5c-257a-456d-b9e5-f54526c478e0'
    # )

    # get_transactions = transaction_manager.get_transactions()

    # for transaction in get_transactions:
    #     print(transaction_manager.get_transaction_details(
    #         transaction_id=transaction
    #     ))

    transaction_manager.delete_transactions()
