import pandas as pd
import redis
import uuid
from datetime import datetime

class TransactionManager:
    """
    TransactionManager handles transaction details from CSV and Redis.
    """

    def __init__(self, host, port, db):
        """
        Initialize the connection to the Redis database.
        
        :param host: Redis server host
        :param port: Redis server port
        :param db: Redis database number
        """
        self.r = redis.Redis(host=host, port=port, db=db)
        
    def _redis_to_json(self, transactions):
        return {
            key.decode("utf-8"): (
                value.decode("utf-8") if isinstance(value, bytes) else value
            )
            for key, value in transactions.items()
        }

    def _timestamp_to_date(self, timestamp):
        """
        Convert timestamp to python date
        """
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").date()

    def _str_to_date(self, str_date):
        """
        Convert string date to python date
        """
        return datetime.strptime(str_date, '%Y-%m-%d').date()

    def get_transactions_from_redis(self):
        """
        Retrieve transaction keys from Redis that start with 'request:mvola:'.
        
        :return: List of transaction keys from Redis
        """
        transactions = self.r.keys()  # Retrieve all keys from Redis
        transaction_list = [
            str(transaction.decode('utf-8')) for transaction in transactions
            if transaction.decode('utf-8').startswith('request:mvola:')
        ]
        
        # print(transaction_list)
        
        print("================= Get transaction from Redis ==================")
        
        # Log the number of transactions found
        transaction_count = len(transaction_list)
        if transaction_count == 0:
            print('- No transactions found')
        else:
            print('- {} transactions have been found in Redis.'.format(transaction_count))
            
        print("===============================================================\n")
        
        return transaction_list

    def get_transaction_details_from_redis(self, transaction_list):
        """
        Retrieve transaction details from Redis for a list of transaction IDs.
        
        :param transaction_list: List of transaction IDs to retrieve
        :return: List of dictionaries containing transaction details
        """
        
        if not transaction_list:
            print('No transactions found.')
            return []
        
        transaction_details = []
        for transaction in transaction_list:
            if self.r.exists(transaction):
                transactions = self.r.hgetall(transaction)
                transactions_data_json = self._redis_to_json(transactions)
                transaction_details.append(transactions_data_json)
            else:
                print('Transaction ID {} does not exist.'.format(transaction))
        
        return transaction_details
    
    def write_transactions_to_file(self, transaction_details, file_name):
        """
        Write transaction details into a text file.
        
        :param transaction_details: List of dictionaries containing transaction details
        :param file_name: The name of the file to write the transaction details to
        """
        # Check if transaction_details is a list of dictionaries
        if not isinstance(transaction_details, list):
            print("Error: 'transaction_details' is not a list.")
            return

        # Write to file
        with open(file_name, 'w') as file:
            for idx, details in enumerate(transaction_details):
                if 'key' not in details:
                    print("Warning: 'key' not found in transaction details at index {}".format(idx))
                    continue

                # file.write("{} Transaction {}: \n".format(idx + 1, details['key']))
                file.write("{}\n".format(details['key']))
                
                # for key, value in details.items():
                #     file.write("   {}: {}\n".format(key, value))
                # file.write("========================================================\n\n")

        print("{} Transaction details have been written to {}".format(len(transaction_details), file_name))

    def get_transactions_from_csv(self, csv_file):
        """
        Read transactions from a CSV file and categorize them into relevant categories.

        :param csv_file: Dict with 'data' (CSV content) and 'type' (either 'key' or 'transactionID')
        :return: List of transactions based on the CSV type
        """
        try:
            df = pd.read_csv(csv_file['data'])

            # Process based on the type of CSV file
            if csv_file['type'] == 'key':
                return self._process_key_transactions(df)
            
            if csv_file['type'] == 'transactionID':
                return self._process_transaction_id(df)
        
        except Exception as e:
            print('Error processing CSV file: {}'.format(str(e)))
            return []

    def _process_key_transactions(self, df):
        """
        Process transactions from a CSV containing 'key' and 'date'.
        Filters transactions with dates before 31 August 2024.

        :param df: Pandas DataFrame containing 'key' and 'date'
        :return: List of keys with dates before 31 August 2024
        """
        try:
            # Remove duplicates and extract relevant columns
            grouped_df = df[['key', 'date']].drop_duplicates()

            # Initialize list to hold keys before 31 August 2024
            filtered_keys = []
            cutoff_date = self._str_to_date('2024-08-31')

            # Iterate over rows and filter keys by date
            for _, row in grouped_df.iterrows():
                key = str(row['key']).strip()
                date = self._timestamp_to_date(row['date'])

                # Append keys with dates before the cutoff
                if date < cutoff_date:
                    filtered_keys.append(key)
                # else:
                #     print('{} has a later date {}'.format(key, date))

            # Print summary
            print("========== Print transaction dated before 31 Aug 2024 ==========")
            print("- Found {} out of {} transactions dated before 31 Aug 2024 - ".format(
                len(filtered_keys), len(grouped_df)))
            print("================================================================")

            return filtered_keys

        except Exception as e:
            print('Error processing key transactions: {}'.format(str(e)))
            return []

    def _process_transaction_id(self, df):
        """
        Process transactions from a CSV containing 'transaction_id' and 'obs'.
        Returns transaction IDs marked as 'OK'.

        :param df: Pandas DataFrame containing 'transaction_id' and 'obs'
        :return: List of transaction IDs prefixed with 'request:mvola:' where 'obs' is 'OK'
        """
        try:
            # Remove duplicates and extract relevant columns
            grouped_df = df[['transaction_id', 'obs']].drop_duplicates()

            # Initialize list to hold completed transactions
            completed_transactions = []
            key_prefix = 'request:mvola:'

            # Iterate over rows and append 'OK' transactions
            for _, row in grouped_df.iterrows():
                if row['obs'].strip() == 'OK':
                    completed_transactions.append('{}{}'.format(key_prefix, row['transaction_id']))

            # Print summary
            print("================== Get Transaction from CSV ===================")
            print("===> {} transactions marked 'OK' found in CSV".format(len(completed_transactions)))
            print("===============================================================\n")

            return completed_transactions

        except Exception as e:
            print('Error processing transaction IDs: {}'.format(str(e)))
            return []

    def get_transactions_from_txt_file(self, txt_file):
        try:
            with open(txt_file, 'r') as file_transaction:
                transaction_list = []
                for transaction in file_transaction:
                    # print(str(transaction).strip())
                    transactionID = str(transaction).strip()
                    if transactionID:
                        transaction_list.append(str(transaction).strip())
                return transaction_list
        except Exception as e:
            print('Error or parsing data from txt file... \n{}'.format(str(e)))

    def compare_transaction_and_delete(self, transaction_from_redis, transaction_from_csv, transaction_from_txt):
        """
        Compare transactions from Redis and CSV, and delete from Redis if the transaction exists in both.
        """
        
        # Convert lists to sets for faster comparison
        redis_transaction_set = set(transaction_from_redis)
        csv_transaction_set = set(transaction_from_csv)
        txt_transaction_set = set(transaction_from_txt)
        
        # print(csv_transaction_set)
        
        # transaction_details = self.get_transaction_details_from_redis(redis_transaction_set)
        
        # self.write_transactions_to_file(transaction_details, 'redis_transaction_details.txt')
        
        # with open('csv_transactions_data.txt', 'w') as file:
        #     for data in csv_transaction_set:
        #         file.write('{}\n'.format(data))
                
        # print(redis_transaction_set)
        
        # print("TRANSACTION FROM REDIS SET")
        # print(transaction_from_redis)
        # print(completed_transaction_from_csv)

        print("\n=================== REDIS vs CSV ========================")
        # Find the intersection of both sets (transactions that exist in both Redis and CSV)
        transactions_to_delete = redis_transaction_set.intersection(csv_transaction_set)
        
        # ================= REDIS vs TXT ========================
        # Find the intersection of both sets (transactions that exist in both Redis and TXT)
        # transactions_to_delete = redis_transaction_set.intersection(txt_transaction_set)
        
        if transactions_to_delete:
            print("================= Transaction to delete after intersection ================")
            print("- {} transactions".format(len(transactions_to_delete)))
            print("===========================================================================\n")

        if not transactions_to_delete:
            print('No matching transactions found for deletion.')
            return

        # Use Redis pipeline to batch delete operations
        # with self.r.pipeline() as pipe:
        #     with open('deleted_transaction_redis.txt', 'w') as file_deleted:
        #         for transaction_id in transactions_to_delete:
        #             file_deleted.write('Deleted transaction: {}\n'.format(transaction_id))
                    
        #             pipe.delete(transaction_id)
        #             print('Deleted transaction: {}'.format(transaction_id))

        #         # Execute the batch delete
        #         pipe.execute()

        # Summary of the deletion process
        # print('==> Deleted {} transactions from Redis <==.'.format(len(transactions_to_delete)))

    def _create_fake_transactions_from_csv(self, transactions):
            """
            Create fake transactions in Redis based on transaction IDs from the CSV.
            
            :param transactions: List of transaction IDs
            """
            if not self.r:
                print("Error: Redis connection is not available.")
                return

            # Transaction data template
            transaction_data = {
                'offer_refnum': 38302,
                'transaction_amount': 20000,
                'transaction_id': 2307191028187690012,
                'transaction_debitor': 0343500004,
                'transaction_object_reference': 642997587,
                'transaction_original_transaction_reference': '1e6dc4dc-8f9a-4d25-a12c-48b1084f6d5a',
                'operator': 'telma-internet-tv',
                'customer_refnum': 101,
                'transaction_date': '2023-09-29T12:09:58.865Z',
                'transaction_description': 'Decouverte 30j',
                '_is_activated': 1,
                'key': 'request:mvola:2307191028187690012',
                'customer_device_id': 43130694183,  # Corrected leading zeros
                'service_type': 'tv',
                'offer_amount': 20000,
                'transaction_status': 'completed',
                'customer_msisdn': 0343500004,
            }

            for transaction in transactions:
                try:
                    # Set unique transaction data for each transaction
                    transaction_data['key'] = transaction

                    # Save transaction in Redis
                    self.r.hset(transaction, mapping=transaction_data)
                    
                    # Print transaction details
                    print('''
                    ------------------- Transaction Created -------------------
                    Transaction ID : {}
                    -----------------------------------------------------------
                    '''.format(transaction))
                except redis.RedisError as e:
                    print("Error: Unable to create transaction {} in Redis - {}".format(transaction, e))
                except Exception as e:
                    print("Unexpected error: {}".format(e))

# Initialize TransactionManager
transaction_manager_mvola = TransactionManager(host='localhost', port=6379, db=5)

# Get transactions from Redis
transaction_list_from_redis = transaction_manager_mvola.get_transactions_from_redis()

# print(transaction_list_from_redis)

file_name_csv_data = {
    'key': {
        'data': 'transaction_mvola_internet_rivo.csv',
        'type': 'key'
    },
    'transactionID': {
        'data': 'transaction_mvola_verif.csv',
        'type': 'transactionID'
    }
}

# ========= Get transaction from CSV ==========
transaction_list_from_csv = transaction_manager_mvola.get_transactions_from_csv(
    file_name_csv_data['key']
)
# print(completed_transaction_from_csv)

# ========= Get transaction from CSV =========
transaction_list_from_txt = transaction_manager_mvola.get_transactions_from_txt_file('data/old_transaction.txt')

# ======== Compare and delete transactions from Redis ======
transaction_manager_mvola.compare_transaction_and_delete(
    transaction_from_redis=transaction_list_from_redis, 
    transaction_from_csv=transaction_list_from_csv,
    transaction_from_txt=transaction_list_from_txt
)


# 
# print(transaction_manager_mvola.get_transactions_from_txt_file('data/old_transaction.txt'))


# ======== Create fake transaction from CSV TEST =========
# transaction_manager_mvola._create_fake_transactions_from_csv(transactions=completed_transaction_from_csv)



# # Retrieve and print details for each transaction from Redis
# transaction_details = transaction_manager_mvola.get_transaction_details_from_redis(transaction_list=transaction_from_redis)

# # Write transaction details to a file
# transaction_manager_mvola.write_transactions_to_file(transaction_details, 'transaction_details.txt')