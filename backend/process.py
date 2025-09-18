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

