
# TAMARA LEAD DATA PIPELINE
# Clean, filter, and standardize lead data

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

RAW_CSV_PATH = 'raw_csv/last few months total lead data.csv'
CLEANED_CSV_PATH = 'cleaned_csv/last_few_months_total_lead_data_filtered_clean.csv'
BLACKLIST_START_DATE = pd.Timestamp('2024-06-01')

print("="*70)
print("TAMARA LEAD DATA PIPELINE - FILTERED & CLEANED")
print("="*70)

# ============================================================================
# COLUMN MAPPING & SELECTION
# ============================================================================

# mapping: { 'Source Column Name': 'target_column_name' }
COLUMN_MAPPING = {
    # Identity
    'Legal Entity Name': 'company_name',
    'Company / Account': 'company_account_fallback', # Fallback for company_name
    'Business Name (Arabic)': 'company_name_ar',
    'CR Number - Unified National Number': 'cr_number',
    'Lead ID': 'lead_id',
    'First Name': 'first_name',
    'Last Name': 'last_name',
    
    # Contact
    'Email': 'email',
    'Mobile': 'mobile',
    'Preferred Language': 'preferred_language',
    
    # Digital Presence
    'Website': 'website',
    'Google Map': 'google_map_id', # Or Google Map ID
    
    # Business Profile
    'Reported Annual Sales': 'reported_annual_sales_tier',
    'Business Type': 'business_type',
    'Category': 'category',
    'Subcategory': 'subcategory',
    'L1': 'L1',
    'L2': 'L2',
    'L3': 'L3',
    'Business Activities (EN)': 'business_activities_en',
    'Country of Registration': 'country_of_registration',
    
    # Compliance
    'CR Issue Date': 'cr_issue_date',
    'CR Expiry Date': 'cr_expiry_date',
    'VAT Number': 'vat_number',
    'IBAN': 'iban',
    'National ID Expiry Date': 'national_id_expiry_date',
    # Blacklist logic generates 'blacklist_status'
    
    # Journey
    'Lead Status': 'lead_status',
    'Complete Application Contacted Status': 'contacted_status',
    'Onboarding Step': 'onboarding_step',
    'Created Date': 'created_date',
    'Timestamp "KYB in Progress"': 'kyb_in_progress_date',
    'Timestamp "KYB Submitted"': 'kyb_submitted_date',
    'Lead Last Status Change DateTime': 'last_status_change_date',
    'Converted Date': 'converted_date',
    
    # Marketing
    'Lead Source': 'lead_source',
    'Lead Created UTM Source': 'utm_source',
    'Lead Created UTM Medium': 'utm_medium',
    'Lead Created UTM Campaign': 'utm_campaign',
    
    # Logic Helpers (Source columns needed for logic but might not be in final output if not selected)
    'Internal Blacklisting Passed': 'blacklist_raw',
}

# The final list of columns to keep in the output
SELECTED_COLUMNS = [
    # Identity
    'company_name',
    'company_name_ar',
    'cr_number',
    'lead_id',

    # Contact
    'email',
    'mobile',
    'preferred_language',
    
    # Digital Presence
    'website',
    'google_map_id',
    
    # Business Profile
    'reported_annual_sales_tier',
    'business_type',
    'L1',
    'L2',
    'L3',
    'business_activities_en',
    'country_of_registration',
    
    # Compliance
    'cr_issue_date',
    'cr_expiry_date',
    'vat_number',
    'iban',
    'national_id_expiry_date',
        
    # Journey
    'lead_status',
    'contacted_status',
    'onboarding_step',
    'created_date',
    'kyb_in_progress_date',
    'kyb_submitted_date',
    'last_status_change_date',
    'converted_date',
    
    # Marketing
    'lead_source',
    'utm_source',
    'utm_medium',
    'utm_campaign'
]

# ============================================================================
# PROCESSING FUNCTIONS
# ============================================================================

def clean_dates(df):
    """Parse date columns standardizing formats"""
    date_cols = [
        'cr_issue_date', 'cr_expiry_date', 'national_id_expiry_date',
        'created_date', 'kyb_in_progress_date', 'kyb_submitted_date',
        'last_status_change_date', 'converted_date'
    ]
    
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def apply_blacklist_logic(df):
    """
    Apply cutoff logic for blacklist:
    - Pre-June 1st 2024: SAFE (Keep)
    - Post-June 1st 2024: CHECK Result (Keep if 1, Drop if 0)
    """
    print("🔍 Applying Blacklist Logic...")
    
    # Ensure created_date is datetime
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    
    df['blacklist_status'] = 0 # Default failure
    
    # Logic iteration
    # We need to map the raw values first
    # 1/Green -> Passed (1)
    # 0/Red -> Failed (0)
    
    def parse_raw_blacklist(val):
        if pd.isna(val): return None
        s_val = str(val)
        if 'green.png' in s_val or s_val == '1': return 1
        if 'red.png' in s_val or s_val == '0': return 0
        return None

    df['blacklist_parsed'] = df['blacklist_raw'].apply(parse_raw_blacklist)
    
    # Vectorized logic
    # Condition 1: Created BEFORE cutoff -> SAFE (1)
    cond_pre_cutoff = df['created_date'] < BLACKLIST_START_DATE
    
    # Condition 2: Created AFTER cutoff AND PASSED -> SAFE (1)
    cond_post_cutoff_pass = (df['created_date'] >= BLACKLIST_START_DATE) & (df['blacklist_parsed'] == 1)
    
    # Assign 1 to safe rows
    df.loc[cond_pre_cutoff | cond_post_cutoff_pass, 'blacklist_status'] = 1
    
    # Specific logic requested: "take that leads who passed the blacklist"
    # This implies we FILTER the dataframe rows here or later.
    # The requirement says: "keep all merchants who didn't pass the blacklist with the exception of applicants where lead created date is post June 2024."
    # Which implies: 
    #   IF Date < June: Keep (Even if blacklist failed? "keep all... who didn't pass... with exception of post June") -> YES, Keep pre-June.
    #   IF Date >= June: Drop if failed. Keep if passed.
    
    return df

def process_identity(df):
    # Coalesce Company Name
    df['company_name'] = df['company_name'].fillna(df['company_account_fallback'])
    
    # Contact Name
    df['contact_name'] = (df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')).str.strip()
    
    return df

def process_digital_contact(df):
    # Email domain
    df['email'] = df['email'].str.lower().str.strip()
    df['email_domain'] = df['email'].str.split('@').str[1]
    
    # Mobile
    df['mobile'] = df['mobile'].astype(str).str.replace(r'[+\s-]', '', regex=True)
    df['mobile'] = df['mobile'].replace(['nan', ''], np.nan)
    
    return df

def process_business_tiers(df):
    # Sales Tier 1-5
    sales_map = {
        'Nano Business (Less than $250 thousand)': 1,
        'Micro Business ($250 thousand to $1 million)': 2,
        'Small Business ($1 million to $5 million)': 3,
        'Medium Business ($5 million to $50 million)': 4,
        'Large Business ($50 million+)': 5
    }
    df['reported_annual_sales_tier'] = df['reported_annual_sales_tier'].map(sales_map).fillna(0).astype(int)
    return df

def main():
    print(f"📂 Loading: {RAW_CSV_PATH}")
    try:
        df = pd.read_csv(RAW_CSV_PATH, encoding='utf-8-sig', low_memory=False)
        print(f"   ✅ Queries Loaded: {len(df):,} records")
    except FileNotFoundError:
        print(f"   ❌ File not found: {RAW_CSV_PATH}")
        return

    # 1. Rename columns based on mapping (Keep all for now)
    # We invert the map to check if cols exist or use nice rename
    # But some source cols map to specific targets.
    
    # Basic rename
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    
    # 2. Logic & Processing
    df = clean_dates(df)
    df = apply_blacklist_logic(df)
    df = process_identity(df)
    df = process_digital_contact(df)
    df = process_business_tiers(df)
    
    # 3. Filtering Rows
    print("\n🔍 Filtering Rows based on Blacklist Logic...")
    initial_count = len(df)
    
    # Logic: Keep if blacklist_status == 1
    # (We already calculated blacklist_status to be 1 for pre-June and passed post-June)
    df_filtered = df[df['blacklist_status'] == 1].copy()
    
    dropped_count = initial_count - len(df_filtered)
    print(f"   ❌ Dropped {dropped_count:,} leads (Failed Post-June Blacklist checks)")
    print(f"   ✅ Remaining {len(df_filtered):,} leads")
    
    # 4. Selecting Columns
    print("\nTarget Columns Selection...")
    
    # Check what's missing
    available_cols = df_filtered.columns.tolist()
    missing_cols = [c for c in SELECTED_COLUMNS if c not in available_cols]
    
    if missing_cols:
        print(f"   ⚠️ Warning: {len(missing_cols)} columns missing from source, filling with NaN:")
        for c in missing_cols:
            print(f"      - {c}")
            df_filtered[c] = np.nan
            
    # Select only the requested subset
    df_final = df_filtered[SELECTED_COLUMNS]
    
    # 5. Export
    os.makedirs(os.path.dirname(CLEANED_CSV_PATH), exist_ok=True)
    df_final.to_csv(CLEANED_CSV_PATH, index=False, encoding='utf-8-sig')
    
    print("\n" + "="*70)
    print("✅ PIPELINE COMPLETE")
    print("="*70)
    print(f"Output: {CLEANED_CSV_PATH}")
    print(f"Shape: {df_final.shape}")

if __name__ == "__main__":
    main()
