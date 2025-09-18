import os
import pandas as pd
from pathlib import Path
import pandas as pd

def clean_column_labels(df):
    """Clean duplicate prefixes from column names"""
    cleaned_columns = []
    changes_made = []
    
    for col in df.columns:
        parts = col.split('_')
        
        # Check for duplicate prefix (e.g., medications_medications_DESCRIPTION)
        if len(parts) >= 3 and parts[0] == parts[1]:
            # Remove first duplicate prefix
            new_col = '_'.join(parts[1:])
            cleaned_columns.append(new_col)
            changes_made.append(f"{col} → {new_col}")
        else:
            cleaned_columns.append(col)
    
    # Apply cleaned column names
    df.columns = cleaned_columns
    
    print(f"✅ Cleaned {len(changes_made)} column names:")
    for change in changes_made[:10]:  # Show first 10 changes
        print(f"  {change}")
    if len(changes_made) > 10:
        print(f"  ... and {len(changes_made) - 10} more changes")
    
    return df

def clean_patient_csv(input_path, output_path=None):
    """Clean a single patient CSV file"""
    if output_path is None:
        output_path = input_path  # Overwrite original
    
    print(f"🔄 Processing: {input_path}")
    
    # Load CSV
    df = pd.read_csv(input_path, low_memory=False)
    print(f"📊 Original: {len(df.columns)} columns, {len(df)} rows")
    
    # Clean column names
    df = clean_column_labels(df)
    
    # Save cleaned CSV
    df.to_csv(output_path, index=False)
    print(f"💾 Saved: {output_path}")
    print(f"✨ Final: {len(df.columns)} columns, {len(df)} rows")
    
    return df

def batch_clean_all_patients(patients_root_dir):
    """Clean all patient CSV files in batch"""
    
    processed_count = 0
    total_changes = 0
    
    print(f"🚀 Starting batch cleanup of patient CSVs in: {patients_root_dir}")
    
    # Process each patient folder
    for patient_folder in os.listdir(patients_root_dir):
        patient_path = os.path.join(patients_root_dir, patient_folder)
        
        if os.path.isdir(patient_path):
            csv_file = os.path.join(patient_path, 'merged_patient_data.csv')
            
            if os.path.exists(csv_file):
                try:
                    print(f"\n📁 Patient: {patient_folder}")
                    
                    # Load and clean
                    df = pd.read_csv(csv_file, low_memory=False)
                    original_cols = df.columns.tolist()
                    df = clean_column_labels(df)
                    
                    # Count changes
                    changes = sum(1 for orig, new in zip(original_cols, df.columns) if orig != new)
                    total_changes += changes
                    
                    # Save cleaned CSV (backup original first)
                    backup_file = os.path.join(patient_path, 'merged_patient_data_backup.csv')
                    if not os.path.exists(backup_file):
                        df_original = pd.read_csv(csv_file, low_memory=False)
                        df_original.to_csv(backup_file, index=False)
                    
                    df.to_csv(csv_file, index=False)
                    processed_count += 1
                    
                except Exception as e:
                    print(f"❌ Error processing {patient_folder}: {e}")
    
    print(f"\n🎉 Batch cleanup complete!")
    print(f"✅ Processed: {processed_count} patients")
    print(f"🔧 Total column name changes: {total_changes}")

# Usage
batch_clean_all_patients('C:/Users/HP/NeuralDAO-test-sub1/backend/test_set/patients')
