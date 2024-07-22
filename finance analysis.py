import pandas as pd
from pandas.api.types import is_numeric_dtype
import re
import os
from pypdf import PdfReader

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

read_files = []
compile_name_boa = 'Bank of America.csv'

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
        print(trans)
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

# Function that finds all eStatement PDFs of a given account type and compiles them into a single DataFrame (returns the DataFrame)
def read_all(Account):
    df = pd.DataFrame()
    errors = []
    files = sorted(os.listdir(Account.file_dir))
    print(f"{len(files)} files found in {Account.name} directory: {', '.join(files)}")
    for f in files:
        read_files.append(f)
        new_df = read(f, Account)
        if new_df is None:
            errors.append(f)
            continue
        df = pd.concat([df, new_df])
    print(f"Files in the {Account.name} directory have been read into a dataframe.")
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
    data = {'Statement Date': [], 'Transaction Date': [], 'Posting Date': [], 'Description': [], 'Reference Number': [], 'Account Number': [], 'Amount': [], 'Type': [], 'Account': []}
    # Parse lines of transactions and add to dict
    for trans in transactions:
        data['Statement Date'].append(date)
        # Call account's function for parsing transactions (returns dict)
        new_data = Account.parse_trans(trans, date)
        # for returned data keys that match data keys, set. for others, set None
        for key, val in new_data.items():
            data[key].append(val)
        for key in set(data) - set(new_data) - {'Statement Date', 'Account'}:
            data[key].append(None)
        # Add account type's name
        data['Account'].append(Account.name)
    # Create df from dict
    df = pd.DataFrame(data)
    print(df['Type'].value_counts().to_string())
    print(df)
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

# Searches for any new statements and adds them to a compiled spreadsheet
# def update_boa():
#     if not os.path.isfile(compile_name_boa):
#         print(f"No file with name {compile_name_boa}. Creating one now to store compiled Bank of America data.")
#         df = read_all_boa()
#         df.to_csv(compile_name_boa, index=False)
#         return
#     from_csv = pd.read_csv(compile_name_boa)
#     errors = []
#     files = sorted(os.listdir(dir_boa_credit))
#     new_files = [f for f in files if f not in read_files]
#     print(f"{len(new_files)} new files found in Bank of America directory: {', '.join(new_files)}")
#     if len(new_files) > 0:
#         for filen in new_files:
#             read_files.append(filen)
#             new_df = read_boa(filen)
#             if new_df is None:
#                 errors.append(f)
#                 continue
#             from_csv = pd.concat([from_csv, new_df])
#         print("New files in the Bank of America directory have been read into a dataframe.")
#         if len(errors) > 0:
#             print(f"There was a problem with files: {', '.join(errors)}")
#     print("Dropping any potential duplicates...")
#     from_csv['Transaction Date'] = pd.to_datetime(from_csv['Transaction Date'])
#     from_csv['Reference Number'] = pd.to_numeric(from_csv['Reference Number'])
#     from_csv['Account Number'] = pd.to_numeric(from_csv['Account Number'])
#     from_csv.drop_duplicates(inplace=True)
#     from_csv.to_csv(compile_name_boa, index=False)
#     print("Compiled Bank of America file has been updated.")


# Returns a loaded DataFrame of Bank of America data, read from a compiled CSV
def get_boa():
    update_boa()
    df = pd.read_csv(compile_name_boa)
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
    df['Reference Number'] = pd.to_numeric(df['Reference Number'])
    df['Account Number'] = pd.to_numeric(df['Account Number'])
    return df

# Rebuild the compiled file from scratch (using read_all)
def rebuild_boa():
    df1 = read_all(BoA_Credit)
    df2 = read_all(BoA_Debit)
    df3 = read_all(BoA_Savings)
    df_total = pd.concat([df1, df2, df3])
    df_total.to_csv(compile_name_boa, index=False)
    return df_total

while True:
    inp = input("Select an option:\n" +
                "1: Update BoA file\n" +
                "2: Print BoA file\n" +
                "3: Rebuild BoA file\n" +
                "4: Quit\n")
    if inp[0] == "1":
        #update_boa()
        print('Not currently functional.')
    elif inp[0] == "2":
        print(get_boa())
    elif inp[0] == "3":
        rebuild_boa()
    elif inp[0] =="4":
        break
