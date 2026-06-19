import pandas as pd
import random
from datetime import datetime, timedelta

# Configuration to align with our phone records network
suspect_account = "ACC-98765-SUSPECT"
accomplice_accounts = ["ACC-11111-MULE", "ACC-33333-MULE", "ACC-55555-MULE"]
hawala_channels = ["UPI-HAWALA-999@okaxis", "UPI-SHADY-MERCHANT@ibl", "ACC-77777-SHELL"]

all_destinations = accomplice_accounts + hawala_channels
data = []
start_time = datetime(2026, 6, 15, 10, 0, 0) # Aligned with investigation window

# Generate 30 realistic transactional money flows
for i in range(30):
    start_time += timedelta(hours=random.randint(1, 12), minutes=random.randint(0, 59))
    
    # 1. Determine direction (Is the suspect sending money or laundering it in?)
    is_suspect_sending = random.choices([True, False], weights=[0.7, 0.3])[0]
    
    if is_suspect_sending:
        source_acc = suspect_account
        dest_acc = random.choice(all_destinations)
        txn_type = random.choice(["IMPS OUT", "UPI DEBIT", "NEFT CHARGE"])
        amount = random.randint(5000, 150000)
    else:
        source_acc = random.choice(hawala_channels)
        dest_acc = suspect_account
        txn_type = random.choice(["UPI CREDIT", "IMPS IN"])
        amount = random.randint(25000, 500000)
        
    # Simulate a standardized financial transaction tracking reference string (e.g., UTR/Ref Number)
    ref_number = f"TXN202606{random.randint(100000, 999999)}"

    data.append({
        "Transaction_Date": start_time.strftime("%Y-%m-%d"),
        "Timestamp": start_time.strftime("%H:%M:%S"),
        "Source_Account_No": source_acc,
        "Destination_Account_No": dest_acc,
        "Amount_INR": amount,
        "Transaction_Method": txn_type,
        "Reference_UTR": ref_number
    })

# Save to CSV
df = pd.DataFrame(data)
df.to_csv("dummy_bank_statement.csv", index=False)

print("✅ Success: 'dummy_bank_statement.csv' generated with 30 rows!")
print(f"Suspect Central Bank Node Account: {suspect_account}")
print("Now run your Flask app and test the cross-analysis workflow mapping!")