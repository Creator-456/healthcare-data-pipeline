"""
Healthcare Data ETL Pipeline
Author: Dev Narayan Chaudhary
Utica University - MBA Business Analytics

Processes NYS Health Data using AWS and Python
Creates Tableau-ready datasets for medical students
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

class HealthcareETL:
    """ETL Pipeline for NYS Healthcare Data"""
    
    def __init__(self):
        self.raw_data = None
        self.processed_data = None
        self.extraction_date = datetime.now()
        
    def extract_data(self, start_date=None, end_date=None):
        """Extract healthcare data"""
        print("="*60)
        print("EXTRACTING NYS HEALTH DATA")
        print("="*60)
        
        # Simulate data extraction from NYS health database
        np.random.seed(42)
        n_records = 10000
        
        # Generate realistic healthcare data
        conditions = ['Diabetes', 'Hypertension', 'Asthma', 'Heart Disease', 
                     'Obesity', 'Depression', 'Arthritis', 'Cancer', 'COPD', 'Stroke']
        
        counties = ['Albany', 'Oneida', 'Erie', 'Monroe', 'Onondaga', 
                   'Nassau', 'Suffolk', 'Westchester', 'Kings', 'Queens']
        
        self.raw_data = pd.DataFrame({
            'patient_id': [f'P{i:06d}' for i in range(1, n_records+1)],
            'record_date': pd.date_range(start='2024-01-01', periods=n_records, freq='H'),
            'age': np.random.randint(18, 90, n_records),
            'gender': np.random.choice(['M', 'F'], n_records),
            'county': np.random.choice(counties, n_records),
            'condition': np.random.choice(conditions, n_records),
            'admission_type': np.random.choice(['Emergency', 'Elective', 'Urgent'], 
                                              n_records, p=[0.4, 0.4, 0.2]),
            'length_of_stay': np.random.lognormal(1, 0.8, n_records),
            'total_cost': np.random.lognormal(9, 1.2, n_records),
            'readmission': np.random.choice([0, 1], n_records, p=[0.85, 0.15])
        })
        
        print(f"✓ Extracted {len(self.raw_data)} health records")
        print(f"✓ Date range: {self.raw_data['record_date'].min()} to {self.raw_data['record_date'].max()}")
        print(f"✓ Unique patients: {self.raw_data['patient_id'].nunique()}")
        
        return self.raw_data
    
    def transform_data(self):
        """Transform and clean healthcare data"""
        print("\n" + "="*60)
        print("TRANSFORMING DATA")
        print("="*60)
        
        if self.raw_data is None:
            raise ValueError("No data to transform. Run extract_data() first.")
        
        df = self.raw_data.copy()
        
        # Data cleaning
        print("Performing data cleaning...")
        
        # Standardize text fields
        df['condition'] = df['condition'].str.title()
        df['county'] = df['county'].str.title()
        
        # Create age groups
        df['age_group'] = pd.cut(df['age'], 
                                  bins=[0, 18, 35, 50, 65, 100], 
                                  labels=['<18', '18-35', '36-50', '51-65', '65+'])
        
        # Extract date components
        df['year'] = df['record_date'].dt.year
        df['month'] = df['record_date'].dt.month
        df['quarter'] = df['record_date'].dt.quarter
        df['day_of_week'] = df['record_date'].dt.day_name()
        
        # Calculate derived metrics
        df['cost_per_day'] = df['total_cost'] / df['length_of_stay']
        df['is_high_cost'] = (df['total_cost'] > df['total_cost'].quantile(0.75)).astype(int)
        df['is_long_stay'] = (df['length_of_stay'] > 7).astype(int)
        
        # Create risk scores
        df['risk_score'] = (
            (df['age'] > 65).astype(int) * 2 +
            (df['admission_type'] == 'Emergency').astype(int) * 3 +
            (df['readmission'] == 1).astype(int) * 4
        ) / 9
        
        print(f"✓ Cleaned {len(df)} records")
        print(f"✓ Created {len(df.columns)} features")
        print(f"✓ Added age groups, date components, and risk scores")
        
        self.processed_data = df
        return df
    
    def generate_analytics(self):
        """Generate key analytics from processed data"""
        print("\n" + "="*60)
        print("GENERATING ANALYTICS")
        print("="*60)
        
        if self.processed_data is None:
            raise ValueError("No processed data. Run transform_data() first.")
        
        df = self.processed_data
        
        # Key metrics
        print("\nKEY METRICS:")
        print(f"Total Patients: {df['patient_id'].nunique():,}")
        print(f"Total Admissions: {len(df):,}")
        print(f"Avg Length of Stay: {df['length_of_stay'].mean():.1f} days")
        print(f"Avg Total Cost: ${df['total_cost'].mean():,.2f}")
        print(f"Readmission Rate: {df['readmission'].mean()*100:.1f}%")
        
        # Top conditions
        print("\nTOP 5 CONDITIONS:")
        top_conditions = df['condition'].value_counts().head(5)
        for i, (condition, count) in enumerate(top_conditions.items(), 1):
            pct = count / len(df) * 100
            print(f"  {i}. {condition}: {count:,} ({pct:.1f}%)")
        
        # Demographics
        print("\nDEMOGRAPHICS:")
        print(f"Average Age: {df['age'].mean():.1f} years")
        gender_dist = df['gender'].value_counts()
        print(f"Gender: {gender_dist.get('M', 0)} Male, {gender_dist.get('F', 0)} Female")
        
        # Regional analysis
        print("\nTOP 5 COUNTIES BY ADMISSIONS:")
        top_counties = df['county'].value_counts().head(5)
        for i, (county, count) in enumerate(top_counties.items(), 1):
            print(f"  {i}. {county}: {count:,}")
        
        # High-risk patients
        high_risk = df[df['risk_score'] > 0.7]
        print(f"\nHigh-Risk Patients: {len(high_risk):,} ({len(high_risk)/len(df)*100:.1f}%)")
        
        return {
            'total_patients': df['patient_id'].nunique(),
            'total_admissions': len(df),
            'avg_los': df['length_of_stay'].mean(),
            'avg_cost': df['total_cost'].mean(),
            'readmission_rate': df['readmission'].mean()
        }
    
    def create_tableau_extracts(self):
        """Prepare data extracts for Tableau"""
        print("\n" + "="*60)
        print("CREATING TABLEAU EXTRACTS")
        print("="*60)
        
        if self.processed_data is None:
            raise ValueError("No processed data. Run transform_data() first.")
        
        # Overview metrics by month
        overview = self.processed_data.groupby(['year', 'month']).agg({
            'patient_id': 'count',
            'total_cost': 'sum',
            'length_of_stay': 'mean',
            'readmission': 'mean'
        }).reset_index()
        overview.columns = ['Year', 'Month', 'Admissions', 'Total_Cost', 'Avg_LOS', 'Readmission_Rate']
        
        # Condition analysis
        condition_stats = self.processed_data.groupby('condition').agg({
            'patient_id': 'count',
            'total_cost': 'mean',
            'length_of_stay': 'mean',
            'readmission': 'mean'
        }).reset_index()
        condition_stats.columns = ['Condition', 'Count', 'Avg_Cost', 'Avg_LOS', 'Readmission_Rate']
        
        # Regional analysis
        regional = self.processed_data.groupby('county').agg({
            'patient_id': 'count',
            'total_cost': 'sum',
            'readmission': 'mean'
        }).reset_index()
        regional.columns = ['County', 'Admissions', 'Total_Cost', 'Readmission_Rate']
        
        print("✓ Created overview metrics extract")
        print("✓ Created condition analysis extract")
        print("✓ Created regional analysis extract")
        print("\nExtracts ready for Tableau import!")
        
        return {
            'overview': overview,
            'conditions': condition_stats,
            'regional': regional
        }
    
    def simulate_s3_upload(self, bucket_name='healthcare-data'):
        """Simulate saving to AWS S3"""
        print("\n" + "="*60)
        print(f"SIMULATING S3 UPLOAD: {bucket_name}")
        print("="*60)
        
        # In production, would use boto3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        print(f"✓ Would upload raw data to: s3://{bucket_name}/raw/health_data_{timestamp}.csv")
        print(f"✓ Would upload processed data to: s3://{bucket_name}/processed/health_data_{timestamp}.csv")
        print(f"✓ Would upload metadata to: s3://{bucket_name}/metadata/metadata_{timestamp}.json")
        
        print("\n✅ S3 upload simulation complete")
    
    def run_full_pipeline(self):
        """Execute complete ETL pipeline"""
        print("\n" + "="*60)
        print("HEALTHCARE DATA ETL PIPELINE")
        print("="*60)
        print("Author: Dev Narayan Chaudhary")
        print("Utica University - MBA Business Analytics")
        print("="*60)
        
        # Extract
        self.extract_data()
        
        # Transform
        self.transform_data()
        
        # Analytics
        analytics = self.generate_analytics()
        
        # Tableau extracts
        extracts = self.create_tableau_extracts()
        
        # Simulate S3 upload
        self.simulate_s3_upload()
        
        print("\n" + "="*60)
        print("✅ ETL PIPELINE COMPLETE!")
        print("="*60)
        print("\nNext Steps:")
        print("1. Import extracts into Tableau")
        print("2. Create dashboards for medical students")
        print("3. Publish to Tableau Server")
        print("="*60)
        
        return {
            'analytics': analytics,
            'extracts': extracts
        }


if __name__ == "__main__":
    # Run the complete pipeline
    pipeline = HealthcareETL()
    results = pipeline.run_full_pipeline()
