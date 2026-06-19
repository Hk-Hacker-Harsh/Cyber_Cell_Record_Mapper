import pandas as pd
import random
from datetime import datetime, timedelta

# Configuration
target_suspect = "9111122222"
accomplices = ["1234567890", "9333344444", "9555566666"]
innocent_contacts = ["7012345678", "8123456789", "9012345678", "9898989898", "0987654321"]
all_numbers = accomplices + innocent_contacts

data = []
start_time = datetime(2026, 6, 15, 9, 0, 0) # Simulation timestamp

# Generate 50 realistic call transactions
for i in range(50):
    # Advance time incrementally per call entry
    start_time += timedelta(minutes=random.randint(5, 45), seconds=random.randint(0, 59))
    
    # 1. Determine direction (Is the target calling or receiving?)
    is_target_calling = random.choice([True, False])
    
    if is_target_calling:
        calling_num = target_suspect
        # Target calls accomplices much more frequently than random numbers
        called_num = random.choices([random.choice(accomplices), random.choice(innocent_contacts)], weights=[0.7, 0.3])[0]
        call_type = random.choices(["MOC", "SMS_MO"], weights=[0.8, 0.2])[0]
    else:
        called_num = target_suspect
        calling_num = random.choices([random.choice(accomplices), random.choice(innocent_contacts)], weights=[0.7, 0.3])[0]
        call_type = random.choices(["MTC", "SMS_MT"], weights=[0.8, 0.2])[0]
        
    # 2. Setup realistic duration based on type
    if "SMS" in call_type:
        duration = 0
    else:
        # Calls can be answered (duration > 0) or missed/rejected (duration = 0)
        duration = random.choices([random.randint(10, 300), 0], weights=[0.8, 0.2])[0]
        
    # 3. Simulate tower Cell IDs (LAC-CellID format)
    cell_id = f"404-45-{random.randint(10000, 99999)}"

    data.append({
        "Date": start_time.strftime("%Y-%m-%d"),
        "Time": start_time.strftime("%H:%M:%S"),
        "Calling_Number": calling_num,
        "Called_Number": called_num,
        "Duration": duration,
        "Type": call_type,
        "Cell_ID": cell_id
    })

# Convert to DataFrame and drop to CSV
df = pd.DataFrame(data)
df.to_csv("dummy_cdr_data2.csv", index=False)

print("✅ Success: 'dummy_cdr_data.csv' generated with 50 rows!")
print(f"Target Suspect: {target_suspect}")
print("Upload this file into your web tool to test the graph layout distribution.")