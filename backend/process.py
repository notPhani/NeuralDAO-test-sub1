import pandas as pd
import glob
import os
from pathlib import Path

def clean_data(file_name):
    # Read the CSV file
    file = os.path.join("C:/Users/HP/NeuralDAO-test-sub1/backend/test_set", file_name)
    df = pd.read_csv(file)

    # Extract prefix label from filename, e.g., "allergies" from "allergies.csv"
    label_prefix = file_name[:-4] + "_"
    print(label_prefix)

    # Rename columns except for patient_id or the ID column to keep join key intact
    cols_to_rename = {col: label_prefix + col for col in df.columns if col not in ['patient_id', 'id', "patient", "Id", "PATIENT", "PATIENTID"]}
    print(cols_to_rename)
    df.rename(columns=cols_to_rename, inplace=True)

    # **ADD THIS LINE** - Save the modified dataframe back to the CSV file
    df.to_csv(file, index=False)

    return df
def segregate_patients_separate_tables(directory_path):
    import pandas as pd
    import os
    from pathlib import Path
    
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    patient_id_columns = ['patient_id', 'id', 'Id', 'PATIENTID', 'PATIENT', 'patients_Id']
    
    # Load dataframes
    dataframes = {}
    for file in csv_files:
        filepath = os.path.join(directory_path, file)
        dataframes[file] = pd.read_csv(filepath)
    
    # Get unique patients from patients.csv
    patients_df = dataframes.get('patients.csv')
    
    # CORRECTED LINE: Use is None or .empty instead of direct boolean check
    if patients_df is None or patients_df.empty:
        print("patients.csv not found or empty!")
        return
        
    patient_id_col = None
    for col in patient_id_columns:
        if col in patients_df.columns:
            patient_id_col = col
            break
    
    unique_patients = patients_df[patient_id_col].unique()
    
    # Create individual patient folders
    for patient_id in unique_patients:
        patient_folder = os.path.join(directory_path, 'patients', f'patient_{patient_id}')
        Path(patient_folder).mkdir(parents=True, exist_ok=True)
        
        # Extract data for this patient from each CSV
        for filename, df in dataframes.items():
            current_id_col = None
            for col in patient_id_columns:
                if col in df.columns:
                    current_id_col = col
                    break
            
            if current_id_col:
                patient_data = df[df[current_id_col] == patient_id]
                if not patient_data.empty:
                    output_path = os.path.join(patient_folder, filename)
                    patient_data.to_csv(output_path, index=False)
        
        print(f"Created folder for patient {patient_id}")

def merge_patient(patient_folder):
    """
    Merge all CSV resource files inside patient's folder into one CSV,
    blindly concatenating side by side and dropping duplicated columns.
    """
    csv_files = [f for f in os.listdir(patient_folder) if f.endswith('.csv')]
    dfs = []
    
    for file in csv_files:
        filepath = os.path.join(patient_folder, file)
        df = pd.read_csv(filepath)
        dfs.append(df)
    
    # Concatenate all dataframes horizontally (columns)
    merged_df = pd.concat(dfs, axis=1)
    
    # Drop duplicate columns by name, keeping first occurrence
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
    
    # Save merged file
    output_file = os.path.join(patient_folder, "merged_patient_data.csv")
    merged_df.to_csv(output_file, index=False)
    
    print(f"Merged CSV saved at: {output_file}")

#merge_patient("C:/Users/HP/NeuralDAO-test-sub1/backend/test_set/patients/patient_0a1bd9a2-fc21-7ad3-3d85-cf31b68eec28")

import os

def clean_patient_folder(patient_folder):
    """
    Delete all CSV files except 'merged_patient_data.csv' inside the patient's folder
    """
    for file in os.listdir(patient_folder):
        if file.endswith('.csv') and file != 'merged_patient_data.csv':
            file_path = os.path.join(patient_folder, file)
            try:
                os.remove(file_path)
                print(f"Deleted {file_path}")
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")

def clean_all_patients_folders(patients_root_dir):
    """
    Iterate over all patient folders and clean them
    """
    for patient_folder in os.listdir(patients_root_dir):
        full_path = os.path.join(patients_root_dir, patient_folder)
        if os.path.isdir(full_path):
            clean_patient_folder(full_path)


# make a relational database and host it on supabase to run the SQL 
import pandas as pd
import os
from pathlib import Path

def create_patient_table_sql(patient_csv_path, patient_id):
    """Generate SQL for one patient table"""
    df = pd.read_csv(patient_csv_path, low_memory=False)
    
    # Clean patient ID for table name (remove hyphens, special chars)
    clean_patient_id = patient_id.replace('-', '_').replace(' ', '_')
    table_name = f"patient_{clean_patient_id}"
    
    sql_statements = []
    
    # 1. CREATE TABLE statement
    sql_statements.append(f"-- TABLE FOR PATIENT: {patient_id}")
    sql_statements.append(f"DROP TABLE IF EXISTS {table_name};")
    
    create_table = f"CREATE TABLE {table_name} ("
    
    columns = []
    for col in df.columns:
        # Determine data type based on sample data
        sample_data = df[col].dropna()
        if len(sample_data) == 0:
            col_type = "TEXT"
        elif sample_data.dtype in ['int64', 'int32']:
            col_type = "INTEGER"
        elif sample_data.dtype in ['float64', 'float32']:
            col_type = "REAL"
        else:
            col_type = "TEXT"
        
        # Clean column name for SQL compatibility
        clean_col = col.replace('-', '_').replace(' ', '_').replace('.', '_')
        columns.append(f"    {clean_col} {col_type}")
    
    create_table += "\n" + ",\n".join(columns) + "\n);"
    sql_statements.append(create_table)
    sql_statements.append("")
    
    # 2. INSERT statements
    sql_statements.append(f"-- DATA FOR PATIENT: {patient_id}")
    
    for _, row in df.iterrows():
        values = []
        for col in df.columns:
            val = row[col]
            if pd.isna(val) or val == '':
                values.append('NULL')
            elif isinstance(val, str):
                # Escape single quotes
                safe_val = str(val).replace("'", "''").replace('\n', ' ')
                values.append(f"'{safe_val}'")
            else:
                values.append(str(val))
        
        clean_columns = [col.replace('-', '_').replace(' ', '_').replace('.', '_') for col in df.columns]
        
        insert_sql = f"INSERT INTO {table_name} ({', '.join(clean_columns)}) VALUES ({', '.join(values)});"
        sql_statements.append(insert_sql)
    
    sql_statements.append("")
    return "\n".join(sql_statements)

def create_master_patient_database(patients_root_dir, output_file='patients_database.sql'):
    """Create one SQL file with individual table per patient"""
    all_sql = []
    all_sql.append("-- PATIENT-CENTRIC CLINICAL DATABASE")
    all_sql.append("-- Each patient has their own table for easy querying")
    all_sql.append("-- Query example: SELECT * FROM patient_12345 WHERE medications_DESCRIPTION LIKE '%insulin%';")
    all_sql.append("")
    
    patient_count = 0
    
    # Process each patient folder
    for patient_folder in os.listdir(patients_root_dir):
        patient_path = os.path.join(patients_root_dir, patient_folder)
        if os.path.isdir(patient_path):
            csv_file = os.path.join(patient_path, 'merged_patient_data.csv')
            if os.path.exists(csv_file):
                # Extract patient ID from folder name
                patient_id = patient_folder.replace('patient_', '')
                
                # Generate SQL for this patient
                patient_sql = create_patient_table_sql(csv_file, patient_id)
                all_sql.append(patient_sql)
                
                patient_count += 1
                print(f"Processed patient: {patient_id}")
    
    # Write master SQL file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(all_sql))
    
    print(f"\n🎉 Created {output_file} with {patient_count} patient tables!")
    return output_file

import sqlite3
import pandas as pd

def run_hard_query(query, db_path='patients_database.db'):
    """Execute complex clinical query"""
    conn = sqlite3.connect(db_path)
    try:
        result = pd.read_sql_query(query, conn)
        print(f"✅ Query executed successfully!")
        print(f"📊 Results: {len(result)} rows, {len(result.columns)} columns")
        return result
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None
    finally:
        conn.close()

# Test with your actual patient table name
query = """
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT medications_DESCRIPTION) as unique_medications,
    COUNT(DISTINCT procedures_CODE) as unique_procedures,
    MAX(encounters_START) as latest_encounter
FROM patient_25f30c19_e98a_85ea_6de8_f976388d4678;
"""

result = run_hard_query(query)
print(result)

