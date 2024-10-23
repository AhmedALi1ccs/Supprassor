import pandas as pd
import re
import streamlit as st 
def is_personal_name(name):
    # Define a refined list of business-related keywords add department and board
    business_keywords = [
        r"\bllc\b", r"\binc\b", r"\bcorp\b", r"\bltd\b", r"\bgroup\b",r"\bl l p\b",r"\bl l c\b",
        r"\bcompany\b", r"\bplc\b", r"\bco\b", r"\benterprises\b", r"\bproperty\b", r"\bsociety\b",
        r"\bhome\b", r"\bhomes\b", r"\blife\b",  r"\bdevelopment\b", r"\bmoving\b", r"\binvestments\b", 
        r"\bavenue\b", r"\brental\b", r"\balbany\b", r"\borganization\b", r"\btech\b", r"\bfairfield\b", r"\buniversity\b",     
        r"\bchildren\b", r"\bthe\b", r"\bnation\b", r"\bexperts\b", r"\binvesting\b", r"\bhilliard\b", r"\bservice\b",   
        r"\bjohnstown\b", r"\bvillage\b", r"\bclub\b", r"\bcouncil\b", r"\bcountry\b", r"\boffice\b", r"\bdelaware\b",  
        r"\blicking\b", r"\burbancrest\b", r"\bestate\b", r"\bbapist\b", r"\bunion\b", r"\bholding\b",  
        r"\bmission\b", r"\bwater\b", r"\bchurch\b", r"\bstreet\b", r"\bnational\b", r"\bassociation\b", r"\bhouse\b", 
        r"\bbank\b", r"\benterprise\b", r"\bveteran\b", r"\bhousing\b", r"\binsurance\b", r"\bmortgage\b", r"\binvestment\b",
        r"\bohio\b", r"\bcity\b", r"\blimited\b", r"\bproperties\b", r"\bcolumbus\b", r"\bfund\b", r"\bmgmt\b",r"\bCommissioners\b"
        r"\bmanagement\b", r"\bhospital\b", r"\bamerican\b", r"\bpartner\b", r"\blp\b", r"\bllp\b", r"\bpartner\b",r"\bdepartment\b",r"\bboard\b"
        r"\bconstruction\b", r"\breal estate\b", r"\bentertainment\b",r"\bcounty\b",r"\bequity\b",r"\bentity\b",r"\bcorporation\b",r"\bcommunity\b", r"\bgranville\b",
    ]   

    # Check if any business keyword is in the name (case insensitive, whole words)
    if isinstance(name, str):
        name_lower = name.lower()
        for keyword in business_keywords:
            if re.search(keyword, name_lower):
                return False

        # Check if the name contains "Trust" and does not contain "Company"
        if 'trust' or 'tr' or 'trustee' in name_lower and 'company' not in name_lower:
            return True

    # Improved regex to match personal names, accounting for:
    # - Hyphenated first and last names
    # - Middle initials and names
    # - Suffixes like Jr., Sr., II, III, etc.
    # - Handling of "Etal" or "Et Al"
    name_pattern = re.compile(
        r"^[A-Z][a-zA-Z]*"                    # First name
        r"([ '-][A-Z][a-zA-Z]*)?"             # Optional middle name/initial, with hyphen support
        r"( [A-Z][a-zA-Z]*)?"                 # Last name, optionally hyphenated
        r"([ '-][A-Z](\.[a-zA-Z]*)?)?"    
        r"( [A-Z][a-zA-Z]*(-[A-Z][a-zA-Z]*)?)" # Second last name or hyphenated last name
        r"( (Jr\.|Sr\.|II|III|IV|V|Etal|Et Al\.?|[0-9]+)?)?$",  # Suffixes, including "Etal" or "Et Al"
        re.IGNORECASE
    )

    if isinstance(name, str):
        match = name_pattern.match(name)
        if match:
            return True
        else:
            print(f"Classified as non-personal due to regex mismatch: {name}")
    return False
excluded_zips = [
    ""
#     '10170', '10728', '10762', '11050', '12379', '12896', '13715', '13735', '15003', '18128',
#     '23232', '43008', '43016', '43017',  '43023', '43035', '43054', '43061', '43064',
#     '43065', '43071', '43074', '43076', '43080', '43082', '43102', '43103', '43105', '43112',
#     '43140', '43143', '43162', '43203',  '43221', '43315', '43344',
#     '43725', '43804', '43948', '45830',
]

def clean_full_zip(zip_code):
    zip_code = str(zip_code).replace(',', '').replace('.0', '')
    return zip_code[:5]

column_mapping_config = {
    'property_address': ['property address', 'address', 'property_address', 'site address', "Street", 'street_address'],
    'property_city': ['property city', 'city', 'property_city'],
    'property_state': ['property state', 'state', 'property_state', "region"],
    'property_zip': ['property zip', 'property zipcode', 'zip', 'zipcode', 'property_zip', 'property_zipcode',
                     'zip code', "PostalCode", "postal_code"],
    'mailing_address': ['mailing address', 'owner address', 'mailing_address', 'owner_address'],
    'mailing_city': ['mailing city', 'owner city', 'mailing_city', 'owner_city'],
    'mailing_state': ['mailing state', 'owner state', 'mailing_state', 'owner_state'],
    'mailing_zip': ['mailing zip', 'mailing zipcode', 'owner zip', 'owner zipcode', 'mailing_zip', 'mailing_zipcode',
                    'owner_zip', 'owner_zipcode'],
    'full_name': ['full name', 'owner full name', 'first owner full name', 'full_name', 'owner_full_name',
                  'first_owner_full_name', 'owner contact name',"Owner Name"],
    'first_name': ['first name', 'owner first name', 'first owner first name', 'first_name', 'owner_first_name',
                   'first_owner_first_name'],
    'last_name': ['last name', 'owner last name', 'first owner last name', 'last_name', 'owner_last_name',
                  'first_owner_last_name']
}

def map_columns(df, config, standardized_columns_map):
    mapped_columns = {}
    for key, possible_names in config.items():
        for name in possible_names:
            standardized_name = re.sub(r'[\s_]+', ' ', name.strip().lower())
            if standardized_name in standardized_columns_map:
                mapped_columns[key] = standardized_columns_map[standardized_name]
                break
        else:
            mapped_columns[key] = 'none'
    return mapped_columns

def split_name(full_name):
    if pd.isna(full_name) or not isinstance(full_name, str) or not full_name.strip():
        return '', ''
    
    
    full_name = full_name.strip()
    # Handle cases with '&' symbol
    if '&' in full_name:
        parts = full_name.split('&')
        
        # Strip any leading or trailing spaces from parts
        part1 = parts[0].strip()
        part2 = parts[1].strip() if len(parts) > 1 else ''
        
        # Further split the parts into first and last names
        part1_parts = part1.split()
        part2_parts = part2.split()
    
    parts = full_name.split()
    last_word = parts[-1].rstrip('.').lower()
    if len(parts) == 1:
        first_name = parts[0]
        last_name = ""
        return first_name, last_name
    # Check for suffixes and corporate entities
    if last_word in ['llc', 'inc', 'sir', 'jr', 'sr']:
        if last_word in ['llc', 'inc']:
            return full_name, ''
        last_name = parts[-2]
        first_name = parts[0]
        return first_name, last_name
    
    # Check if the second-to-last part is a single letter and handle accordingly
    if len(parts) > 3 and len(parts[-2]) == 1 and parts[-2].isalpha():
        last_name = parts[-3]
        first_name = parts[0]
        return first_name, last_name

    # Check if the last part is a single letter
    if len(parts) > 1 and len(parts[-1]) == 1 and parts[-1].isalpha():
        last_name = parts[-2]
        first_name = parts[0]
        return first_name, last_name

    # Default case: first word as first name, last word as last name
    first_name = parts[0]
    last_name = parts[-1]
    return first_name, last_name

def reorder_name(name):
    """
    Reorder names:
    1. From "Last First MiddleInitial" to "Last MiddleInitial First"
    2. From "Initial First Last" to "First Initial Last"
    Example: 
    - "Dowling William A." -> "Dowling A. William"
    - "W. John Doe" -> "John W. Doe"
    """
    # # Pattern for "Last First MiddleInitial"
    # pattern1 = re.compile(
    #     r"^(?P<last_name>[A-Z][a-zA-Z]*(-[A-Z][a-zA-Z]*)?)"  # Last name
    #     r" (?P<first_name>[A-Z][a-zA-Z]*)"                  # First name
    #     r" (?P<middle_initial>[A-Z]\.?)?$",                 # Optional middle initial with dot
    #     re.IGNORECASE
    # )

    # Pattern for "Initial First Last"
    pattern2 = re.compile(
        r"^(?P<initial>[A-Z]\.?)"                           # First name initial with optional dot
        r" (?P<first_name>[A-Z][a-zA-Z]*)"                  # First name
        r" (?P<last_name>[A-Z][a-zA-Z]*(-[A-Z][a-zA-Z]*)?)$",  # Last name
        re.IGNORECASE
    )

    if isinstance(name, str):
        # Check for pattern 1: "Last First MiddleInitial"
        # match1 = pattern1.match(name)
        # if match1:
        #     reordered_name = f"{match1.group('last_name')} {match1.group('middle_initial')} {match1.group('first_name')}"
        #     return reordered_name.strip()

        # Check for pattern 2: "Initial First Last"
        match2 = pattern2.match(name)
        if match2:
            reordered_name = f"{match2.group('first_name')} {match2.group('initial')} {match2.group('last_name')}"
            return reordered_name.strip()

    return name
def to_proper_case(name):
    if isinstance(name, str):
        return name.title()
    return name

def create_standardized_column_map(df_columns):
    return {re.sub(r'[\s_]+', ' ', col.strip().lower()): col for col in df_columns}

def filter_names(df):
    #df = df[~df[mapped_columns['full_name']].str.contains(r'\d|&', regex=True, na=False)]
    #df = df[df[mapped_columns['full_name']].str.strip().ne('')]  # Remove blank/empty names
    #df = df.drop_duplicates(subset=[mapped_columns['mailing_address']], keep='first')
    #df = df.dropna(subset=[mapped_columns['mailing_address']])
    #df=df.dropna(subset=[mapped_columns['mailing_zip']])
    #df=df.dropna(subset=[mapped_columns['mailing_state]])
    #df-df.dropna(subset=mapped_columns['mailing_city])
    #df = df[df[mapped_columns['mailing_zip']].notna() & df[mapped_columns['mailing_zip']].str.strip().ne('')]
    #df[mapped_columns['property_zip']]=df[mapped_columns['property_zip']].apply(clean_full_zip)
    #df = df[~df[mapped_columns['property_zip']].isin(excluded_zips)]
    #df[mapped_columns['full_name']] = df[mapped_columns['full_name']].apply(to_proper_case)
    #df[mapped_columns['full_name']] = df[mapped_columns['full_name']].apply(reorder_name)
    #personalized_records = df[df[mapped_columns['full_name']].apply(is_personal_name)]
    #non_personalized_records = df[~df[mapped_columns['full_name']].apply(is_personal_name)]
    #personalized_records[[mapped_columns['first_name'], mapped_columns['last_name']]] = personalized_records[mapped_columns['full_name']].apply(split_name).apply(pd.Series)
    #non_personalized_records[[mapped_columns['first_name'], mapped_columns['last_name']]] = non_personalized_records[mapped_columns['full_name']].apply(split_name).apply(pd.Series)
    return personalized_records, non_personalized_records

# Example usage
st.title("Name Filter and Splitter")
st.write("Upload a CSV file to filter and split names into personal and non-personal records.")

# File uploader
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    standardized_columns_map = create_standardized_column_map(df.columns)

    mapped_columns = map_columns(df, column_mapping_config, standardized_columns_map)

    personalized_records, non_personalized_records = filter_names(df)

    st.write("### Personalized Records")
    st.dataframe(personalized_records)

    st.write("### Non-Personalized Records")
    st.dataframe(non_personalized_records)

    st.write("### Download the Results")
    st.download_button(label="Download Personalized CSV", data=personalized_records.to_csv(index=False), file_name="person_records.csv", mime="text/csv")
    st.download_button(label="Download Non-Personalized CSV", data=non_personalized_records.to_csv(index=False), file_name="non_person_records.csv", mime="text/csv")
