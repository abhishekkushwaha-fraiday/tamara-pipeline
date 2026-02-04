
import pandas as pd
import numpy as np
import os
import re

# Config
INPUT_DIR = 'cleaned_csv'
INPUT_FILE = 'last_few_months_total_lead_data_filtered_clean.csv' # Default input from pipeline
OUTPUT_DIR = 'enriched_csv'
OUTPUT_FILE = 'last_few_months_total_lead_data_enriched.csv'

def is_google_maps_url(url):
    """Check if the URL looks like a Google Maps link"""
    if pd.isna(url) or str(url).strip() == '':
        return False
    url_lower = str(url).lower()
    maps_indicators = ['maps.google', 'goo.gl/maps', 'google.com/maps', 'maps.app.goo.gl']
    return any(ind in url_lower for ind in maps_indicators)
def is_social_media_url(url):
    """Check if the URL looks like a Social Media link"""
    if pd.isna(url) or str(url).strip() == '':
        return False
    url_lower = str(url).lower()
    social_domains = [
        'instagram.com', 'snapchat.com', 'youtube.com', 'facebook.com', 
        'twitter.com', 'x.com', 'linkedin.com', 'tiktok.com', 'pinterest.com'
    ]
    return any(domain in url_lower for domain in social_domains)

def is_ecommerce_url(url):
    """Check if the URL looks like an E-commerce link (e.g. Salla)"""
    if pd.isna(url) or str(url).strip() == '':
        return False
    url_lower = str(url).lower()
    ecommerce_domains = [
        'salla.sa'
    ]
    return any(domain in url_lower for domain in ecommerce_domains)

def main():
    input_path = os.path.join(INPUT_DIR, INPUT_FILE)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    
    print(f"📂 Loading data from: {input_path}")
    try:
        df = pd.read_csv(input_path, low_memory=False)
        print(f"   ✅ Queries Loaded: {len(df):,} records")
    except FileNotFoundError:
        print(f"   ❌ File not found: {input_path}")
        return

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("\n🔍 perform enrichment on website, map & social columns...")
    
    # Counter for stats
    moved_maps = 0
    removed_maps = 0
    moved_social = 0
    moved_ecommerce = 0
    
    # Working columns
    website_col = 'website'
    map_col = 'google_map_id'
    social_col = 'social_media_links'
    ecommerce_col = 'e-commerce link'
    
    # Ensure columns exist
    if website_col not in df.columns:
        df[website_col] = np.nan
    if map_col not in df.columns:
        df[map_col] = np.nan
    if social_col not in df.columns:
        df[social_col] = np.nan
    if ecommerce_col not in df.columns:
        df[ecommerce_col] = np.nan

    # Iteration
    for idx, row in df.iterrows():
        site = row[website_col]
        
        # safely get current values as strings for checking emptiness usually
        # But here we rely on the helper functions' NaN checks
        
        # 1. Check for Google Maps in Website
        if is_google_maps_url(site):
            current_map = row[map_col]
            # Check if map column is truly empty (NaN or empty string or 'nan' string)
            is_map_empty = pd.isna(current_map) or str(current_map).strip() == '' or str(current_map).lower() == 'nan'
            
            if is_map_empty:
                df.at[idx, map_col] = site
                moved_maps += 1
            
            # Remove from website (Moved or Redundant)
            df.at[idx, website_col] = np.nan
            removed_maps += 1
            continue # Done with this website value
            
        # 2. Check for Social Media in Website
        if is_social_media_url(site):
            current_social = row[social_col]
            is_social_empty = pd.isna(current_social) or str(current_social).strip() == '' or str(current_social).lower() == 'nan'
            
            if is_social_empty:
                df.at[idx, social_col] = site
            else:
                # Append if social links already exist? Or overwrite? 
                # "move... into this column" - usually implies appending if something exists, 
                # but to avoid duplicates let's just append with space or specific delimiter if needed.
                # User didn't specify delimiter, but ' ' is safe or '|'
                # Let's assume we overwrite or append. Let's append for completeness.
                df.at[idx, social_col] = str(current_social) + ' | ' + str(site)
            
            # Remove from website
            df.at[idx, website_col] = np.nan
            moved_social += 1

            # moved_maps = moved_maps # no change

        # 3. Check for E-commerce in Website
        if is_ecommerce_url(site):
            current_ecom = row[ecommerce_col]
            is_ecom_empty = pd.isna(current_ecom) or str(current_ecom).strip() == '' or str(current_ecom).lower() == 'nan'
            
            if is_ecom_empty:
                df.at[idx, ecommerce_col] = site
            else:
                # Append if exists
                df.at[idx, ecommerce_col] = str(current_ecom) + ' | ' + str(site)
            
            # Remove from website
            df.at[idx, website_col] = np.nan
            moved_ecommerce += 1

    print(f"   ✅ Google Maps: Moved {moved_maps} links to {map_col}")
    print(f"   ✅ Google Maps: Removed {removed_maps} links from {website_col}")
    print(f"   ✅ Social Media: Extracted {moved_social} links to {social_col}")
    print(f"   ✅ E-commerce: Extracted {moved_ecommerce} links to {ecommerce_col}")
    
    # Add Flags
    print("\n🔍 Adding Enrichment Flags...")
    
    # helper for flag creation
    def create_flag(series):
        return series.notna().astype(int) & (series.astype(str).str.strip() != '') & (series.astype(str).str.lower() != 'nan')

    # 1. is_converted
    if 'converted_date' in df.columns:
        df['is_converted'] = df['converted_date'].notna().astype(int)
    else:
        df['is_converted'] = 0
        
    # 2. has_website (Cleaned)
    df['has_website'] = create_flag(df[website_col]).astype(int)
    
    # 3. has_maps
    df['has_maps'] = create_flag(df[map_col]).astype(int)
    
    # 4. has_social_media

    df['has_social_media'] = create_flag(df[social_col]).astype(int)

    # 5. has_ecommerce
    df['has_ecommerce'] = create_flag(df[ecommerce_col]).astype(int)
    
    # ---------------------------------------------------------
    # Journey / Status Tracking
    # ---------------------------------------------------------
    print("\n🛤️  Tracking Lead Journey...")
    
    # helper to parse date columns
    date_cols = ['created_date', 'kyb_in_progress_date', 'kyb_submitted_date', 'last_status_change_date', 'converted_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Clean Onboarding Step Naming
    def clean_step_name(val):
        if pd.isna(val) or val == '':
            return np.nan
        # Remove versions like v2_, v3_
        val = str(val).lower()
        val = re.sub(r'v\d+_', '', val)
        val = val.replace('_step', '').replace('_', ' ').title()
        return val

    def extract_version(val):
        if pd.isna(val) or val == '':
            return 'Unknown'
        val_str = str(val).lower()
        match = re.search(r'(v\d+)', val_str)
        if match:
            return match.group(1).upper() # e.g. V2, V3
        return 'Unknown' # Or None/NaN if preferred, but Unknown is explicit

    if 'onboarding_step' in df.columns:
        df['readable_onboarding_step'] = df['onboarding_step'].apply(clean_step_name)
        df['onboarding_version'] = df['onboarding_step'].apply(extract_version)
    else:
        df['readable_onboarding_step'] = np.nan
        df['onboarding_version'] = np.nan

    # Determine Current Journey Stage
    def get_journey_stage(row):
        # 1. Converted (Highest Priority)
        if 'converted_date' in row and pd.notna(row['converted_date']):
            return 'Converted'
        
        # 2. KYB Submitted
        if 'kyb_submitted_date' in row and pd.notna(row['kyb_submitted_date']):
            return 'KYB Submitted'
            
        # 3. KYB In Progress
        if 'kyb_in_progress_date' in row and pd.notna(row['kyb_in_progress_date']):
            return 'KYB In Progress'
        
        # 4. Onboarding Steps (Granular Status)
        step = row.get('readable_onboarding_step')
        if pd.notna(step):
            if 'Final' in str(step):
                return 'Onboarding Completed' # Ready for KYB likely
            return f"Onboarding: {step}"
            
        # 5. Registered / Created
        if 'created_date' in row and pd.notna(row['created_date']):
            return 'Registered'
            
        return 'Unknown'

    df['journey_stage'] = df.apply(get_journey_stage, axis=1)

    # ---------------------------------------------------------
    # Onboarding Phases (Binary Flags)
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # Onboarding Phases (Binary Flags)
    # ---------------------------------------------------------
    print("   📍 Mapping Specific Onboarding Steps...")
    
    # Mappings from co.txt request
    # Column Name -> List of matching step values
    
    step_config = {
        'otp_sign_up_confirmation_step': ['otp_sign_up_confirmation_step'],
        'business_info_step': ['business_info_step'],
        'v2_personal_details_step': ['v2_personal_details_step'],
        'v2_business_details_step': ['v2_business_details_step'],
        'kyc_step': ['kyc_step'],
        'v2_bank_details_step': ['v2_bank_details_step'],
        'review_application_step': ['review_application_step'],
        'v3_sign_contract_step': ['v3_sign_contract_step'],
        'final_step': ['v2_final_step', 'v3_final_step'] # v2_final_step / v3_final_step
    }

    def check_step(val, valid_steps):
        if pd.isna(val):
            return 0
        return 1 if str(val).strip() in valid_steps else 0

    for col_name, steps in step_config.items():
        df[col_name] = df['onboarding_step'].apply(lambda x: check_step(x, steps))
        print(f"   ✅ Added '{col_name}' (Count: {df[col_name].sum()})")

    # ---------------------------------------------------------
    # Remove Duplicates (Mobile & Email)
    # ---------------------------------------------------------
    print("   ✂️  Removing Duplicates...")
    initial_count = len(df)
    
    # 1. Drop Mobile Duplicates
    if 'mobile' in df.columns:
        # Drop duplicates, keep first. Don't drop NaNs (unless multiple NaNs? usually Mobile NaN allowed?)
        # drop_duplicates considers NaNs equal. We might want to keep rows with missing mobile.
        # So we filter for non-NA, find duplicates, and drop them.
        
        # Actually simplest way:
        # df = df.drop_duplicates(subset=['mobile'], keep='first') 
        # But this drops multiple NaNs. 
        # Strategy: distinct drop on non-null values.
        
        mask = df.duplicated(subset=['mobile'], keep='first') & df['mobile'].notna()
        dupes_mobile_count = mask.sum()
        df = df[~mask]
        print(f"   ✅ Dropped {dupes_mobile_count} duplicate mobile numbers.")

    # 2. Drop Email Duplicates
    if 'email' in df.columns:
        mask = df.duplicated(subset=['email'], keep='first') & df['email'].notna()
        dupes_email_count = mask.sum()
        df = df[~mask]
        print(f"   ✅ Dropped {dupes_email_count} duplicate emails.")

    final_count = len(df)
    print(f"   📉 Total Rows Removed: {initial_count - final_count}")

    print(f"   ✅ Added 'journey_stage'. Top stages:")
    print(df['journey_stage'].value_counts().head(5).to_string(header=False))

    print(f"   ✅ Added 'is_converted' (Count: {df['is_converted'].sum()})")
    print(f"   ✅ Added 'has_website' (Count: {df['has_website'].sum()})")
    print(f"   ✅ Added 'has_maps' (Count: {df['has_maps'].sum()})")
    print(f"   ✅ Added 'has_social_media' (Count: {df['has_social_media'].sum()})")
    print(f"   ✅ Added 'has_ecommerce' (Count: {df['has_ecommerce'].sum()})")
    
    # Export
    print(f"\n💾 Exporting to: {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print("✅ Enrichment Complete.")

if __name__ == "__main__":
    main()
