import duckdb
import os
import time
from pathlib import Path

def stress_test_all_patients_real_columns(patients_root_dir):
    """Stress test all patients using actual column names"""
    
    con = duckdb.connect(database=':memory:')
    
    # Get all patient CSV files
    patient_files = []
    for patient_folder in os.listdir(patients_root_dir):
        patient_path = os.path.join(patients_root_dir, patient_folder)
        if os.path.isdir(patient_path):
            csv_file = os.path.join(patient_path, 'merged_patient_data.csv')
            if os.path.exists(csv_file):
                patient_files.append(csv_file)
    
    print(f"🚀 DuckDB Stress Test on {len(patient_files)} patients")
    print("Using actual column names from your CSV structure")
    print("=" * 60)
    
    # Define 3 simple test queries using your actual column names
    test_queries = [
        {
            'name': 'Patient Demographics',
            'query': """
                SELECT 
                    patients_FIRST as first_name,
                    patients_LAST as last_name,
                    patients_GENDER as gender,
                    patients_BIRTHDATE as birth_date
                FROM '{}' 
                LIMIT 1
            """,
            'description': 'Get basic patient information'
        },
        {
            'name': 'Clinical Record Count', 
            'query': """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT medications_DESCRIPTION) as unique_medications,
                    COUNT(DISTINCT conditions_DESCRIPTION) as unique_conditions
                FROM '{}'
            """,
            'description': 'Count clinical records and unique items'
        },
        {
            'name': 'Healthcare Summary',
            'query': """
                SELECT 
                    patients_HEALTHCARE_EXPENSES as total_expenses,
                    patients_HEALTHCARE_COVERAGE as coverage,
                    COUNT(DISTINCT procedures_CODE) as total_procedures
                FROM '{}'
                GROUP BY patients_HEALTHCARE_EXPENSES, patients_HEALTHCARE_COVERAGE
                LIMIT 1
            """,
            'description': 'Healthcare cost and procedure summary'
        }
    ]
    
    # Results tracking
    results = {
        'successful_patients': 0,
        'failed_patients': 0,
        'total_query_time': 0,
        'patient_results': [],
        'errors': [],
        'performance_stats': {
            'fastest_patient': float('inf'),
            'slowest_patient': 0,
            'avg_records_per_patient': 0
        }
    }
    
    start_time = time.time()
    total_records = 0
    
    # Run tests on all patients
    for i, csv_file in enumerate(patient_files, 1):
        patient_id = Path(csv_file).parent.name.replace('patient_', '')
        
        print(f"\n📁 Testing {i}/{len(patient_files)}: {patient_id[:16]}...")
        
        patient_start = time.time()
        patient_success = True
        patient_data = {
            'patient_id': patient_id,
            'file_path': csv_file,
            'queries': {},
            'total_time': 0
        }
        
        # Run each test query
        for test in test_queries:
            try:
                query_start = time.time()
                
                # Format query with CSV file path
                formatted_query = test['query'].format(csv_file)
                
                # Execute query
                result = con.execute(formatted_query).fetchdf()
                
                query_time = time.time() - query_start
                results['total_query_time'] += query_time
                
                # Store result
                patient_data['queries'][test['name']] = {
                    'success': True,
                    'result': result.iloc[0].to_dict() if not result.empty else {},
                    'time': query_time,
                    'rows_returned': len(result)
                }
                
                print(f"  ✅ {test['name']}: {query_time:.3f}s ({len(result)} rows)")
                
            except Exception as e:
                patient_success = False
                error_msg = str(e)
                patient_data['queries'][test['name']] = {
                    'success': False,
                    'error': error_msg,
                    'time': 0
                }
                
                print(f"  ❌ {test['name']}: {error_msg[:40]}...")
                results['errors'].append({
                    'patient': patient_id,
                    'query': test['name'],
                    'error': error_msg
                })
        
        # Calculate patient timing
        patient_time = time.time() - patient_start
        patient_data['total_time'] = patient_time
        
        # Update performance stats
        results['performance_stats']['fastest_patient'] = min(
            results['performance_stats']['fastest_patient'], 
            patient_time
        )
        results['performance_stats']['slowest_patient'] = max(
            results['performance_stats']['slowest_patient'], 
            patient_time
        )
        
        # Update counters
        if patient_success:
            results['successful_patients'] += 1
            print(f"  🎯 Patient complete: {patient_time:.3f}s")
        else:
            results['failed_patients'] += 1
            print(f"  💥 Patient failed: {patient_time:.3f}s")
        
        results['patient_results'].append(patient_data)
        
        # Show progress every 10 patients
        if i % 10 == 0:
            print(f"\n📊 Progress: {i}/{len(patient_files)} patients completed")
            print(f"   Success rate: {results['successful_patients']}/{i} ({results['successful_patients']/i*100:.1f}%)")
    
    total_time = time.time() - start_time
    
    # Calculate final stats
    avg_patient_time = total_time / len(patient_files)
    queries_per_second = (len(patient_files) * 3) / results['total_query_time']
    
    results['performance_stats']['avg_records_per_patient'] = total_records / len(patient_files) if total_records > 0 else 0
    
    # Print comprehensive summary
    print("\n" + "=" * 70)
    print("🎯 DUCKDB STRESS TEST COMPLETE!")
    print("=" * 70)
    
    print(f"📊 OVERALL RESULTS:")
    print(f"   ✅ Successful patients: {results['successful_patients']}/{len(patient_files)} ({results['successful_patients']/len(patient_files)*100:.1f}%)")
    print(f"   ❌ Failed patients: {results['failed_patients']}")
    print(f"   ⏱️  Total execution time: {total_time:.2f}s")
    print(f"   🏃 Average time per patient: {avg_patient_time:.3f}s")
    
    print(f"\n⚡ PERFORMANCE METRICS:")
    print(f"   📈 Total SQL queries executed: {len(patient_files) * 3}")
    print(f"   🚀 Queries per second: {queries_per_second:.1f}")
    print(f"   ⚡ Pure query time: {results['total_query_time']:.2f}s")
    print(f"   🏆 Fastest patient: {results['performance_stats']['fastest_patient']:.3f}s")
    print(f"   🐌 Slowest patient: {results['performance_stats']['slowest_patient']:.3f}s")
    
    # Show sample successful results
    if results['successful_patients'] > 0:
        print(f"\n📋 SAMPLE RESULTS (First 3 successful patients):")
        successful_patients = [p for p in results['patient_results'] 
                             if all(q['success'] for q in p['queries'].values())]
        
        for i, patient in enumerate(successful_patients[:3]):
            print(f"\n  👤 Patient {i+1}: {patient['patient_id'][:16]}...")
            for query_name, query_result in patient['queries'].items():
                if query_result['success']:
                    sample_data = query_result['result']
                    print(f"     {query_name}: {str(sample_data)[:60]}...")
    
    # Show error analysis
    if results['errors']:
        print(f"\n⚠️  ERROR ANALYSIS ({len(results['errors'])} total errors):")
        error_types = {}
        for error in results['errors']:
            error_type = error['error'][:30]
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"   • {error_type}...: {count} occurrences")
    
    # Performance verdict
    print(f"\n🏆 PERFORMANCE VERDICT:")
    if results['successful_patients'] / len(patient_files) > 0.95:
        print("   🎉 EXCELLENT: >95% success rate - Your data is ready for production!")
    elif results['successful_patients'] / len(patient_files) > 0.85:
        print("   ✅ GOOD: >85% success rate - Minor issues to fix")
    else:
        print("   ⚠️  NEEDS WORK: <85% success rate - Data cleaning required")
    
    if queries_per_second > 20:
        print("   ⚡ FAST: >20 queries/sec - Excellent performance for real-time AI")
    elif queries_per_second > 10:
        print("   🏃 DECENT: >10 queries/sec - Good for most use cases")
    else:
        print("   🐌 SLOW: <10 queries/sec - Consider optimization")
    
    con.close()
    return results

# Run the stress test
stress_results = stress_test_all_patients_real_columns('C:/Users/HP/NeuralDAO-test-sub1/backend/test_set/patients')


# next steps are crucial : These will include 
# uploading all the patient files to supabase