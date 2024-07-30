import pandas as pd
from pandas.api.types import is_numeric_dtype
import re
import os
import json
from pypdf import PdfReader

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

total_compile_name = 'All Data.csv'

# STATIC CLASSES -- Do not make instances of >:(
class Account:
    bank = 'None'
class BankOfAmerica(Account):
    bank = 'Bank of America'
    compile_name = 'Bank of America.csv'
    accounts = []
class CapitalOne(Account):
    bank = 'Capital One'
    compile_name = 'Capital One.csv'
    accounts = []
class Discover(Account):
    bank = 'Discover'
    compile_name = 'Discover.csv'
    accounts = []
class BoA_Credit(BankOfAmerica):
    name = 'Bank of America Credit'
    file_dir = 'boa_credit/'
    trans_regex = r"(\d+/\d+ \d+/\d+ .+(?:\n\d+\.\d{2})?.+)"
    date_regex = r"Statement Closing Date.*(\d{2}\/\d{2}/\d{2}\d{2})"

    def get_trans_in_page(page_text):
        transactions = re.findall(BoA_Credit.trans_regex, page_text)
        return transactions
    def get_date(reader):
        date = re.search(BoA_Credit.date_regex, reader.pages[0].extract_text()).group(1)
        return date
    def parse_trans(trans, date):
        data = {}
        splits = trans.split(' ')
        # Get transaction date
        year = int(date[-4:])-1 if (splits[0][:2] == "12" and date[:2] == "01") else int(date[-4:])
        data['Transaction Date'] = f"{splits[0]}/{year}"
        data['Posting Date'] = splits[1]
        if splits[2] == 'INTEREST':
            data['Description'] = (' '.join(splits[2:-1]).replace('\n',' '))
            data['Type'] = "Interest"
        elif ' FEE ' in ' '.join(splits[2:-2]):
            data['Description'] = ' '.join(splits[2:-2]).replace('\n',' ')
            data['Account Number'] = splits[-2]
            data['Type'] = "Fee"
        else:
            desc = ' '.join(splits[2:-3]).replace('\n',' ') + ' ' + splits[-3][:-4]
            data['Description'] = desc

            data['Reference Number'] = splits[-3][-4:]
            data['Account Number'] = splits[-2]
            data['Type'] = "Payment" if float(splits[-1].replace(',','')) < 0 else "Purchase"
        data['Amount'] = splits[-1]
        return data
    def parse_validity_ref(page_text):
        true_vals = {
        'Payment': '-' + re.search(r"(Payments and Other Credits).+\$(\d+\.\d+)", page_text).group(2),
        'Purchase': re.search(r"(Purchases and Adjustments).+\$(\d+\.\d+)", page_text).group(2),
        'Fee': re.search(r"(Fees Charged).+\$(\d+\.\d+)", page_text).group(2),
        'Interest': re.search(r"(Interest Charged).+\$(\d+\.\d+)", page_text).group(2)}
        return true_vals
class BoA_Debit(BankOfAmerica):
    name = 'Bank of America Debit (Checking)'
    file_dir = 'boa_debit/'
    trans_regex = r"(\d{2}/\d{2}/\d{2}) (.+(?:\nID:\d+)?.*)[a-zA-Z ](-?(?:\d+,?)+\.\d{2})"
    date_regex = r"Ending balance on ([a-zA-Z]+ \d{1,2}, \d{4})"

    def get_trans_in_page(page_text):
        transactions = re.findall(BoA_Debit.trans_regex, page_text)
        return transactions
    def get_date(reader):
        date = re.search(BoA_Debit.date_regex, reader.pages[0].extract_text()).group(1)
        return date
    def parse_trans(trans, date):
        data = {}
        # Get transaction date
        #print(trans)
        data['Transaction Date'] = trans[0]
        data['Posting Date'] = trans[0]
        data['Description'] = trans[1].replace('\n',' ')
        data['Type'] = "Withdrawal" if float(trans[-1].replace(',','')) < 0 else "Deposit"
        data['Amount'] = float(trans[-1].replace(',', '')) * -1
        return data
    def parse_validity_ref(page_text):
        true_vals = {}
        # Fix later
        # true_vals = {
        # 'Withdrawal': '-' + re.search(r"(Other subtractions).+(\d+\.\d{2})", page_text).group(2),
        # 'Deposit': re.search(r"(Deposits and other additions).+\$(\d+\.\d{2})", page_text).group(2),
        # 'Fee': re.search(r"(Fees Charged).+\$(\d+\.\d+)", page_text).group(2),
        # 'Interest': re.search(r"(Interest Charged).+\$(\d+\.\d+)", page_text).group(2)}
        return true_vals
class BoA_Savings(BoA_Debit):
    name = 'Bank of America Savings'
    file_dir = 'boa_savings/'
class Cap_Credit(CapitalOne):
    name = 'Capital One Credit'
    file_dir = 'cap_one_credit/'
    trans_regex = r"(?:(?:[A-Z][a-z]{2} \d{1,2}\s*){2}[^\$]*\s*-?\$(?:\d+,)*\d+\.\d{2})|(?:Interest Charge .*\$(?:\d+,)*\d+\.\d{2})"
    date_regex = r"[A-Z][a-z]{2} \d{1,2}, \d{4} - ([A-Z][a-z]{2} \d{1,2}, \d{4})"

    def get_trans_in_page(page_text):
        transactions = re.findall(Cap_Credit.trans_regex, page_text)
        return transactions
    def get_date(reader):
        date = re.search(Cap_Credit.date_regex, reader.pages[0].extract_text()).group(1)
        return date
    def parse_trans(trans, date):
        data = {}
        splits = trans.split(' ')
        # If trans is interest, handle differently
        if splits[0] == 'Interest':
            data['Transaction Date'] = date
            data['Posting Date'] = date
            data['Description'] = trans.split(' $')[0]
            data['Type'] = 'Interest'
        else:
            # Get transaction date
            year = int(date[-4:])-1 if (splits[0] == "Dec" and date[:3] == "Jan") else int(date[-4:])
            data['Transaction Date'] = f"{' '.join(splits[:2])}, {year}"
            data['Posting Date'] = f"{' '.join(splits[2:3])}, {year}"
            if 'PAST DUE FEE' in ' '.join(splits[4:-1]):
                data['Type'] = "Fee"
            elif splits[-2] == '-':
                data['Type'] = "Payment" if 'PYMT' in ' '.join(splits[4:-2]) else "Credit"
                splits = splits[:-2] + [''.join(splits[-2:])]
            else:
                data['Type'] = "Purchase"
            data['Description'] = ' '.join(splits[4:-1]).replace('\n',' ')
        data['Amount'] = splits[-1].replace('$', '')
        return data
    def parse_validity_ref(page_text):
        true_vals = {
        'Payment': '-' + re.search(r"(Payments).*\$(\d+\.\d{2})", page_text).group(2),
        'Credit': '-' + re.search(r"(Other Credits).*\$(\d+\.\d{2})", page_text).group(2),
        'Purchase': re.search(r"(Transactions).*\$(\d+\.\d{2})", page_text).group(2),
        'Fee': re.search(r"(Fees Charged).*\$(\d+\.\d{2})", page_text).group(2),
        'Interest': re.search(r"(Interest Charged).*\$(\d+\.\d{2})", page_text).group(2)}
        return true_vals
class Disc_Credit(Discover):
    name = 'Discover Credit'
    file_dir = 'discover_credit/'
    trans_regex = r"(?:\d{2}/\d{2}\s+(?:[^\$\s]+\s)+\s*-?\$(?:\d+,)*\d+\.\d{2})|(?:INTEREST CHARGE .*\$(?:\d+,)*\d+\.\d{2})|(?:[^(?:INTEREST)]* (?:FEE|CHARGE) .*\$(?:\d+,)*\d+\.\d{2})"
    date_regex = r"\d{2}/\d{2}/\d{4}\s*-\s*(\d{2}/\d{2}/\d{4})"

    def get_trans_in_page(page_text):
        transactions = re.findall(Disc_Credit.trans_regex, page_text)
        return transactions
    def get_date(reader):
        date = re.search(Disc_Credit.date_regex, reader.pages[0].extract_text()).group(1)
        return date
    def parse_trans(trans, date):
        data = {}
        splits = trans.split(' ')
        # If trans is interest, handle differently
        if not splits[0][:2].isdigit():
            data['Transaction Date'] = date
            data['Posting Date'] = date
            data['Description'] = trans.split(' $')[0]
            if splits[0] == 'INTEREST':
                data['Type'] = 'Interest'
            else:
                data['Type'] = 'Fee'
        else:
            # Get transaction date
            year = int(date[-4:])-1 if (splits[0][:2] == "12" and date[:2] == "01") else int(date[-4:])
            data['Transaction Date'] = f"{splits[0]}/{year}"
            data['Posting Date'] = f"{splits[0]}/{year}"
            data['Type'] = "Payment" if ('PAYMENT' in ' '.join(splits[2:-1]) or 'PMT' in ' '.join(splits[2:-1])) else "Credit" if 'CREDIT' in ' '.join(splits[2:-1]) else "Refund" if '-' in splits[-1] else "Purchase"
            data['Description'] = ' '.join(splits[1:-1]).replace('\n',' ')
        data['Amount'] = splits[-1].replace('$', '')
        return data
    def parse_validity_ref(page_text):
        true_vals = {
        'Payment+Credit+Refund': '-' + re.search(r"(Payments and Credits).*\$((\d+,)*\d+\.\d{2})", page_text).group(2),
        'Purchase': re.search(r"(Purchases).*\$((\d+,)*\d+\.\d{2})", page_text).group(2),
        'Fee': re.search(r"(Fees Charged).*\$((\d+,)*\d+\.\d{2})", page_text).group(2),
        'Interest': re.search(r"(Interest Charged).*\$((\d+,)*\d+\.\d{2})", page_text).group(2)}
        return true_vals

BankOfAmerica.accounts = [BoA_Credit, BoA_Debit, BoA_Savings]
CapitalOne.accounts = [Cap_Credit]
Discover.accounts = [Disc_Credit]

# Jason is your friend. He will help you manage data storage with a JSON file.
# Give him life to earn his favor (instantiate and objectify him)
class Jason:
    Instance = None
    file_dir = 'data.json'
    json_init = {"banks":{}}
    indent = 6

    # Returns active Jason object. Creates it if one does not exist.
    @staticmethod
    def find():
        if Jason.Instance is not None:
            return Jason.Instance
        else:
            return Jason()
    # Checks if JSON file exists, creates it if not. Returns true on success
    @staticmethod
    def ensure_file():
        try:
            if not os.path.isfile(Jason.file_dir):
                with open(Jason.file_dir, 'w') as f:
                    json.dump(Jason.json_init, f, indent=Jason.indent)
            return True
        except:
            return False
    # Returns list of keys that direct to storage for a given account
    @staticmethod
    def get_account_path(account):
        path = ['banks', account.bank]
        if hasattr(account, 'name'):
            path.append(account.name)
        return path

    def __init__(self):
        if Jason.Instance is not None:
            return
        Jason.Instance = self
        self.data = self.read_file()
    def read_file(self):
        if Jason.ensure_file():
            with open(Jason.file_dir, 'r') as f:
                self.data = json.load(f)
    def write_file(self):
        with open(Jason.file_dir, 'w') as f:
            json.dump(self.data, f, indent=Jason.indent)

    # Returns data at key, after given path (list of keys). Returns None if none
    def get_data(self, key, path=[]):
        if self.data is None:
            self.read_file()
        entry = self.data
        for nav in path:
            if entry is None:
                return None
            try:
                entry = entry[nav]
            except:
                return None
        if key in entry.keys():
            return entry[key]
        else:
            return None
    # Write data to stored dictionary of JSON data. Returns True on success
    # Takes path, a list of keys to get to where the new key/value should be set
    # If keys in 'path' do not exist, they will be written. Beware typos.
    def write_data(self, key, value, path=[]):
        if self.data is None:
            self.read_file()
        entry = self.data
        for nav in path:
            if nav not in entry.keys():
                entry[nav] = {}
            entry = entry[nav]
        entry[key] = value
        self.write_file()
        return True
    # Removes all data
    def clear_data(self):
        self.data = Jason.json_init
        self.write_file()
    # Removes key from data. Key must exist. Returns True on success
    def remove_data(self, key, path=[]):
        entry = self.data
        for nav in path:
            if entry is None:
                return False
            entry = entry[nav]
        try:
            del entry[key]
            self.write_file()
            return True
        except:
            return False
    # Like remove_data, but returns value or None if not existent
    def pop_data(self, key, path=[]):
        entry = self.data
        for nav in path:
            if entry is None:
                return False
            entry = entry[nav]
        val = entry.pop(key, None)
        self.write_file()
        return val


# Function that finds all eStatement PDFs of a given account type and compiles them into a single DataFrame (returns the DataFrame)
def read_all(account):
    df = pd.DataFrame()
    errors = []
    files = sorted([account.file_dir + f for f in os.listdir(account.file_dir)])
    print(f"{len(files)} files found in {account.name} directory: {', '.join(files)}")
    for f in files:
        new_df = read(f, account)
        write_file_hash(f, account)
        if new_df is None:
            errors.append(f)
            continue
        df = pd.concat([df, new_df])
    good_files = [f for f in files if f not in errors]
    print(f"{len(good_files)} files in the {account.name} directory have been read into a dataframe.")
    if len(errors) > 0:
        print(f"There was a problem with {len(errors)} files: {', '.join(errors)}")
    return df

# Similar functionality to read_all() except that it ignores files which have already been read (denoted by filename). Returns DF of new data. 'check_hash' parameter determines if stored hash of statement files should also be used to identify new data. This is not really necessary though, and in a situation where it would make a difference, it would produce unpredictable output (duplicates, etc.)
def read_new(account, check_hash=False):
    df = pd.DataFrame()
    errors = []
    files = sorted([account.file_dir + f for f in os.listdir(account.file_dir)])
    # Find files already read
    old_files = Jason.find().get_data('hashes', Jason.get_account_path(account)).keys()
    new_files = [f for f in files if f not in old_files]
    # If parameter is set to check hash values of statements
    if check_hash:
        # Check each file in directory that has hash
        for f in [f for f in files if f in old_files]:
            # If hash does not match, add it for re-reading
            if not check_file_hash(f, account):
                new_files.append(f)
    print(f"{len(new_files)} new files found in {account.name} directory: {', '.join(new_files)}")
    for f in new_files:
        new_df = read(f, account)
        write_file_hash(f, account)
        if new_df is None:
            errors.append(f)
            continue
        df = pd.concat([df, new_df])
    good_files = [f for f in new_files if f not in errors]
    print(f"{len(good_files)} new files in the {account.name} directory have been read into a dataframe.")
    if len(errors) > 0:
        print(f"There was a problem with {len(errors)} files: {', '.join(errors)}")
    return df

# Function for reading statement of given account type. Returns DataFrame.
def read(filename, account):
    reader = PdfReader(filename)
    transactions = []
    for page in reader.pages:
        text = page.extract_text()
        transactions.extend(account.get_trans_in_page(text))
    print(f"Found {len(transactions)} transactions in {filename}")
    #print('\n\n'.join(transactions))
    # Get date (for year)
    date = account.get_date(reader)

    # Create dict for data
    df = pd.DataFrame()
    data = {'Statement Date': [], 'Transaction Date': [], 'Posting Date': [], 'Description': [], 'Reference Number': [], 'Account Number': [], 'Amount': [], 'Type': [], 'Account': [], 'Statement/Source': []}
    # Parse lines of transactions and add to dict
    for trans in transactions:
        data['Statement Date'].append(date)
        # Call account's function for parsing transactions (returns dict)
        new_data = account.parse_trans(trans, date)
        # for returned data keys that match data keys, set. for others, set None
        for key, val in new_data.items():
            data[key].append(val)
        # Add account type's name
        data['Account'].append(account.name)
        data['Statement/Source'].append(f"{filename}")
        length = len(data['Statement Date'])
        for key in data.keys():
            if len(data[key]) < length:
                data[key].append(None)
    # Create df from dict
    #print(data)
    df = pd.DataFrame(data)
    # Convert to numeric
    if not is_numeric_dtype(df['Amount']):
        df['Amount'] = pd.to_numeric(df['Amount'].str.replace(',', ''))
    # Convert to datetime
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], format='mixed')

    # Validate by adding transactions and comparing to values on page 1
    text = reader.pages[0].extract_text().replace(',', '')
    # Get actual values from statement totals
    true_values = account.parse_validity_ref(text)
    # Compare actual values to totals of each category
    for cat in true_values.keys():
        # For handling categories that are combined under one true value
        groups = cat.split('+')
        found_value = 0
        for group in groups:
            found_value += round(df[df['Type']==group]['Amount'].sum(), 2)
            print(f"Total after adding {group}: {found_value}")
        valid = int(found_value*100) == int(float(true_values[cat])*100)
        if not valid:
            print(f"Validation failed for {filename}.\nStatement total for {cat} ({true_values[cat]}) does not match the total for extracted transaction amounts ({found_value}).")
            return None
    print(f"Validation success for {filename}.")
    return df

# Get the hash of a file. Returns hash
def get_file_hash(filename):
    import hashlib
    with open(filename, 'rb') as file:
        data = file.read()
        md5 = hashlib.md5(data).hexdigest()
        return md5

# Write hash of updated file. Returns new hash
def write_file_hash(filename, account):
    # Set file hash value to filename key in 'hashes'
    new_hash = get_file_hash(filename)
    Jason.find().write_data(filename, new_hash, Jason.get_account_path(account) + ['hashes'])
    print(f"Hash value ({new_hash}) written to {Jason.file_dir} for {filename}.")
    return new_hash

# Check if hash of file matches stored hash. Returns bool if match. No previous hash data defaults to False (no match)
def check_file_hash(filename, account):
    data = Jason.find().get_data('hashes', Jason.get_account_path(account))
    if data is None:
        print(f"Hash data for {Jason.get_account_path(account)} not present in {Jason.file_dir}.")
        return False
    check = data.get(filename)
    # Find current hash value for file
    current_hash = get_file_hash(filename)
    if check is None:
        print(f"Hash value for {filename} not present in {Jason.file_dir}.")
    # If hash does not match, return false
    elif check == current_hash:
        print(f"File hash in {Jason.file_dir} matches record for {filename}.")
        return True
    else:
        print(f"File {filename} has been modified outside the program (hash value does not match value stored in {Jason.file_dir}).")
    return False


# Searches for any new statements for an account and adds them to compiled data
def update_account(account):
    fname = f"{account.name} - {account.compile_name}"
    # If no file exists, build it and save to CSV
    if not os.path.isfile(fname):
        print(f"No file with name {fname}. Creating one now to store data for account: {account.name}")
        rebuild_account(account)
        return
    # If compiled file does not match hash (modified), rebuild
    if not check_file_hash(fname, account):
        print(f"Assuming file: {fname} has been modified and is not reliable. Rebuilding it from all data.")
        rebuild_account(account)
        return
    # Else, file exists and matches hash, read new data and add.
    df = read_new(account)
    if df is not None:
        from_csv = pd.read_csv(fname)
        df_total = pd.concat([from_csv, df])

        print("Dropping any potential duplicates...")
        df_total['Transaction Date'] = pd.to_datetime(df_total['Transaction Date'])
        df_total['Reference Number'] = pd.to_numeric(df_total['Reference Number'])
        df_total['Account Number'] = pd.to_numeric(df_total['Account Number'])
        df_total.drop_duplicates(inplace=True)
        df_total.to_csv(fname, index=False)
        write_file_hash(fname, account)
        print(f"Compiled account data ({fname}) has been updated.")
    else:
        print(f"No new data found to add to compiled account file ({fname}).")

# Updates accounts in a bank and combines them to the bank data
def update_bank(bank):
    dfs = []
    for acc in bank.accounts:
        dfs.append(get_account(acc))
    df_total = pd.concat(dfs)
    df_total.to_csv(bank.compile_name, index=False)
    write_file_hash(bank.compile_name, bank)
    return df_total
# Updates all accounts for all banks
def update_all():
    dfs = []
    dfs.append(update_bank(BankOfAmerica))
    dfs.append(update_bank(CapitalOne))
    dfs.append(update_bank(Discover))
    df_total = pd.concat(dfs)
    df_total.to_csv(total_compile_name, index=False)
    write_file_hash(total_compile_name, Account)
    return df_total

# Returns a loaded DataFrame of specified account, read from a compiled CSV
def get_account(account):
    update_account(account)
    fname = f"{account.name} - {account.compile_name}"
    df = get_file(fname)
    return df
# Returns dataframe loaded from specified file name
def get_file(filename):
    if not os.path.isfile(filename):
        print(f"No data stored at {filename}.")
        return None
    df = pd.read_csv(filename)
    df = clean_data(df)
    return df
# Converts columns of input df to respective types and returns df
def clean_data(df):
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
    df['Reference Number'] = pd.to_numeric(df['Reference Number'])
    df['Account Number'] = pd.to_numeric(df['Account Number'])
    return df

# Takes a list of account types and builds a compiled DataFrame to the given filename
def rebuild_custom(accounts, compile_name):
    dfs = []
    for acc in accounts:
        new_df = read_all(acc)
        dfs.append(new_df)
    df_total = pd.concat(dfs)
    df_total.to_csv(compile_name, index=False)
    write_file_hash(compile_name, Account)
    return df_total

# Rebuild specified account
def rebuild_account(account):
    df = read_all(account)
    fname = f"{account.name} - {account.compile_name}"
    df.to_csv(fname, index=False)
    write_file_hash(fname, account)
    return df

# Rebuild all accounts for specified bank
def rebuild_bank(bank):
    dfs = []
    for acc in bank.accounts:
        dfs.append(rebuild_account(acc))
    df_total = pd.concat(dfs)
    df_total.to_csv(bank.compile_name, index=False)
    write_file_hash(bank.compile_name, bank)
    return df_total

# Rebuild all data for all banks and accounts
def rebuild_all():
    dfs = []
    dfs.append(rebuild_bank(BankOfAmerica))
    dfs.append(rebuild_bank(CapitalOne))
    dfs.append(rebuild_bank(Discover))
    df_total = pd.concat(dfs)
    df_total.to_csv(total_compile_name, index=False)
    write_file_hash(total_compile_name, Account)
    return df_total

while True:
    inp = input("Select an option:\n" +
                "1: Update Bank of America file\n" +
                "2: Update Capital One file\n" +
                "3: Update Discover file\n" +
                "4: Update All Data file\n" +
                "5: Rebuild Bank of America file\n" +
                "6: Rebuild Capital One file\n" +
                "7: Rebuild Discover file\n" +
                "8: Rebuild All Data file\n" +
                "9: View data file\n" +
                "10: Quit\n")
    if inp == "1":
        update_bank(BankOfAmerica)
    elif inp == "2":
        update_bank(CapitalOne)
    elif inp == "3":
        update_bank(Discover)
    elif inp == "4":
        update_all()
    elif inp == "5":
        rebuild_bank(BankOfAmerica)
    elif inp == "6":
        rebuild_bank(CapitalOne)
    elif inp == "7":
        rebuild_bank(Discover)
    elif inp == "8":
        rebuild_all()
    elif inp == "9":
        files = []
        files.append(total_compile_name)
        files.append(BankOfAmerica.compile_name)
        for acc in BankOfAmerica.accounts:
            fname = f"{acc.name} - {acc.compile_name}"
            files.append(fname)
        files.append(CapitalOne.compile_name)
        for acc in CapitalOne.accounts:
            fname = f"{acc.name} - {acc.compile_name}"
            files.append(fname)
        files.append(Discover.compile_name)
        for acc in Discover.accounts:
            fname = f"{acc.name} - {acc.compile_name}"
            files.append(fname)
        options = [(n+1, f) for n,f in enumerate(files)]
        options.append((len(options)+1, "Go Back"))
        path = input("Choose one of the listed files:\n" +
            ''.join([f"{num}: {op}\n" for num, op in options]))
        try:
            op = int(path)
            if op == len(options):
                continue
            if op < 1 or op > len(options):
                print("Enter a number corresponding to an option.")
                continue
            f = files[op-1]
            df = get_file(f)
            if df is None:
                continue
            print(df.info())
            print(df.head())
            print(df.tail())
        except:
            print("Invalid input")
            continue
    elif inp == "10":
        break
