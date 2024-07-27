import pandas as pd
from pandas.api.types import is_numeric_dtype
import re
import os
import json
from pypdf import PdfReader

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

compile_name_boa = 'Bank of America.csv'

# STATIC CLASSES -- Do not make instances of >:(
class Account:
    pass
class BoA_Credit(Account):
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
class BoA_Debit(Account):
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

# Jason is your friend. He will help you manage data storage with a JSON file.
# Give him life to earn his favor (instantiate and objectify him)
class Jason:
    Instance = None
    file_dir = 'data.json'
    json_init = {"hashes":{}}
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

    def get_data(self):
        if self.data is None:
            self.read_file()
        return self.data
    # Write data to stored dictionary of JSON data. Returns True on success
    # Takes path, a list of keys to get to where the new key/value should be set
    # Keys in 'path' MUST exist in the data, but 'key' can be a new key to add.
    def write_data(self, key, value, path=[]):
        entry = self.data
        for nav in path:
            if entry is None:
                return False
            entry = entry[nav]
        entry[key] = value
        self.write_file()
        return True

# Function that finds all eStatement PDFs of a given account type and compiles them into a single DataFrame (returns the DataFrame)
def read_all(Account):
    df = pd.DataFrame()
    errors = []
    files = sorted(os.listdir(Account.file_dir))
    print(f"{len(files)} files found in {Account.name} directory: {', '.join(files)}")
    for f in files:
        new_df = read(f, Account)
        write_file_hash(Account.file_dir + f)
        if new_df is None:
            errors.append(f)
            continue
        df = pd.concat([df, new_df])
    print(f"Files in the {Account.name} directory have been read into a dataframe.")
    if len(errors) > 0:
        print(f"There was a problem with files: {', '.join(errors)}")
    return df

# Similar functionality to read_all() except that it ignores files which have already been read (denoted by filename). Returns DF of new data. 'check_hash' parameter determines if stored hash of statement files should also be used to identify new data. This is not really necessary though, and in a situation where it would make a difference, it would produce unpredictable output (duplicates, etc.)
def read_new(Account, check_hash=False):
    df = pd.DataFrame()
    errors = []
    files = sorted(os.listdir(Account.file_dir))
    new_files = files
    # Find files already read
    data = Jason.find().get_data()
    old_files = data.get('hashes', {}).keys()
    new_files = [f for f in files if f not in old_files]
    # If parameter is set to check hash values of statements
    if check_hash:
        # Check each file in directory that has hash
        for f in [f for f in files if f in old_files]:
            # If hash does not match, add it for re-reading
            if not check_file_hash(f):
                new_files.append(f)
    print(f"{len(new_files)} new files found in {Account.name} directory: {', '.join(new_files)}")
    return
    for f in new_files:
        new_df = read(f, Account)
        write_file_hash(Account.file_dir + f)
        if new_df is None:
            errors.append(f)
            continue
        df = pd.concat([df, new_df])
    print(f"New files in the {Account.name} directory have been read into a dataframe.")
    if len(errors) > 0:
        print(f"There was a problem with files: {', '.join(errors)}")
    return df

# Function for reading statement of given account type. Returns DataFrame.
def read(filename, Account):
    reader = PdfReader(Account.file_dir + filename)
    transactions = []
    for page in reader.pages:
        text = page.extract_text()
        transactions.extend(Account.get_trans_in_page(text))
    print(f"Found {len(transactions)} transactions in {filename}")
    #print('\n\n'.join(transactions))
    # Get date (for year)
    date = Account.get_date(reader)

    # Create dict for data
    df = pd.DataFrame()
    data = {'Statement Date': [], 'Transaction Date': [], 'Posting Date': [], 'Description': [], 'Reference Number': [], 'Account Number': [], 'Amount': [], 'Type': [], 'Account': [], 'Statement/Source': []}
    # Parse lines of transactions and add to dict
    for trans in transactions:
        data['Statement Date'].append(date)
        # Call account's function for parsing transactions (returns dict)
        new_data = Account.parse_trans(trans, date)
        # for returned data keys that match data keys, set. for others, set None
        for key, val in new_data.items():
            data[key].append(val)
        # Add account type's name
        data['Account'].append(Account.name)
        data['Statement/Source'].append(f"{Account.file_dir}{filename}")
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
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"])

    # Validate by adding transactions and comparing to values on page 1
    text = reader.pages[0].extract_text().replace(',', '')
    # Get actual values from statement totals
    true_values = Account.parse_validity_ref(text)
    # Compare actual values to totals of each category
    for cat in true_values.keys():
        found_value = round(df[df['Type']==cat]['Amount'].sum(), 2)
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
def write_file_hash(filename):
    # Set file hash value to filename key in 'hashes'
    new_hash = get_file_hash(filename)
    Jason.find().write_data(filename, new_hash, ['hashes'])
    print(f"Hash value ({new_hash}) written to {Jason.file_dir} for {filename}.")
    return new_hash

# Check if hash of file matches stored hash. Returns bool if match. No previous hash data defaults to False (no match)
def check_file_hash(filename):
    data = Jason.find().get_data()
    check = data.get('hashes', {}).get(filename)
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


# Searches for any new statements and adds them to a compiled spreadsheet
def update_boa():
    # If no compiled file exists, build it entirely and save to CSV
    if not os.path.isfile(compile_name_boa):
        print(f"No file with name {compile_name_boa}. Creating one now to store compiled Bank of America data.")
        rebuild_boa()
        return
    # If compiled file does not match hash (modified), rebuild
    if not check_file_hash(compile_name_boa):
        print(f"Assuming file: {compile_name_boa} has been modified and is not reliable. Rebuilding it from all data.")
        rebuild_boa()
        return
    # Else, file exists and matches hash, read new data and add.
    df1 = read_new(BoA_Credit)
    df2 = read_new(BoA_Debit)
    df3 = read_new(BoA_Savings)
    new_data = [df for df in [df1, df2, df3] if df is not None]
    if len(new_data) > 0:
        df_total = pd.concat(new_data)
        df_total.to_csv(compile_name_boa, index=False)
        write_file_hash(compile_name_boa)

        print("Dropping any potential duplicates...")
        from_csv['Transaction Date'] = pd.to_datetime(from_csv['Transaction Date'])
        from_csv['Reference Number'] = pd.to_numeric(from_csv['Reference Number'])
        from_csv['Account Number'] = pd.to_numeric(from_csv['Account Number'])
        from_csv.drop_duplicates(inplace=True)
        from_csv.to_csv(compile_name_boa, index=False)
        write_file_hash(compile_name_boa)
        print(f"Compiled Bank of America file ({compile_name_boa}) has been updated.")
    else:
        print(f"No new data found to add to compiled Bank of America file ({compile_name_boa}).")


# Returns a loaded DataFrame of Bank of America data, read from a compiled CSV
def get_boa():
    update_boa()
    df = pd.read_csv(compile_name_boa)
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
    df['Reference Number'] = pd.to_numeric(df['Reference Number'])
    df['Account Number'] = pd.to_numeric(df['Account Number'])
    return df

# Rebuild the compiled file from scratch (using read_all). Returns DF
def rebuild_boa():
    df1 = read_all(BoA_Credit)
    df2 = read_all(BoA_Debit)
    df3 = read_all(BoA_Savings)
    df_total = pd.concat([df1, df2, df3])
    df_total.to_csv(compile_name_boa, index=False)
    write_file_hash(compile_name_boa)
    return df_total


while True:
    inp = input("Select an option:\n" +
                "1: Update BoA file\n" +
                "2: Print BoA file\n" +
                "3: Rebuild BoA file\n" +
                "4: Quit\n")
    if inp[0] == "1":
        update_boa()
    elif inp[0] == "2":
        print(get_boa())
    elif inp[0] == "3":
        rebuild_boa()
    elif inp[0] =="4":
        break
