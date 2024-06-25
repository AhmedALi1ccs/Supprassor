import requests
import streamlit as st
import pandas as pd
import re
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import chardet
import os

# Compile the regex patterns beforehand
address_patterns = [(re.compile(pattern, re.IGNORECASE), replacement) for pattern, replacement in {
    r'\bavenue\b\.?': 'Ave', r'\bav\b\.?': 'Ave', r'\bave\b\.?': 'Ave',
    r'\bstreet\b\.?': 'St', r'\bstr\b\.?': 'St', r'\bst\b\.?': 'St',
    r'\bboulevard\b\.?': 'Blvd', r'\bblv\b\.?': 'Blvd', r'\bblvd\b\.?': 'Blvd',
    r'\broute\b\.?': 'Rt',
    r'\bbl\b\.?': 'Blvd', r'\broad\b\.?': 'Rd', r'\brd\b\.?': 'Rd',
    r'\bcourt\b\.?': 'Ct', r'\bct\b\.?': 'Ct',
    r'\bdrive\b\.?': 'Dr', r'\bdr\b\.?': 'Dr',
    r'\blane\b\.?': 'Ln', r'\bln\b\.?': 'Ln',
    r'\bterrace\b\.?': 'Ter', r'\bter\b\.?': 'Ter',
    r'\bplace\b\.?': 'Pl', r'\bpl\b\.?': 'Pl',
    r'\bcircle\b\.?': 'Cir', r'\bcir\b\.?': 'Cir',
    r'\bparkway\b\.?': 'Pkwy', r'\bpkwy\b\.?': 'Pkwy',
    r'\bsquare\b\.?': 'Sq', r'\bsq\b\.?': 'Sq',
    r'\bhighway\b\.?': 'Hwy', r'\bhwy\b\.?': 'Hwy',
    r'\bplaza\b\.?': 'Plz', r'\bplz\b\.?': 'Plz',
    r'\bjunction\b\.?': 'Jct', r'\bjct\b\.?': 'Jct',
    r'\bmountain\b\.?': 'Mtn', r'\bmtn\b\.?': 'Mtn',
    r'\bexpressway\b\.?': 'Expy', r'\bexpy\b\.?': 'Expy',
    r'\bextension\b\.?': 'Ext', r'\bext\b\.?': 'Ext',
    r'\btrail\b\.?': 'Trl', r'\btrl\b\.?': 'Trl',
    r'\bgateway\b\.?': 'Gtwy', r'\bgtwy\b\.?': 'Gtwy',
    r'\bcrossing\b\.?': 'Xing', r'\bxing\b\.?': 'Xing',
    r'\bfort\b\.?': 'Ft', r'\bft\b\.?': 'Ft',
    r'\bcreek\b\.?': 'Crk', r'\bcrk\b\.?': 'Crk',
    r'\bnorth\b\.?': 'N', r'\bnorthern\b\.?': 'N', r'\bn\b\.?': 'N',
    r'\bwest\b\.?': 'W', r'\bwestern\b\.?': 'W', r'\bw\b\.?': 'W',
    r'\beast\b\.?': 'E', r'\beastern\b\.?': 'E', r'\be\b\.?': 'E',
    r'\bsouth\b\.?': 'S', r'\bsouthern\b\.?': 'S', r'\bs\b\.?': 'S',
}.items()]

ordinal_mapping = {
    'first': '1st', 'second': '2nd', 'third': '3rd', 'fourth': '4th', 'fifth': '5th',
    'sixth': '6th', 'seventh': '7th', 'eighth': '8th', 'ninth': '9th', 'tenth': '10th',
    'eleventh': '11th', 'twelfth': '12th', 'thirteenth': '13th', 'fourteenth': '14th',
    'fifteenth': '15th', 'sixteenth': '16th', 'seventeenth': '17th', 'eighteenth': '18th',
    'nineteenth': '19th', 'twentieth': '20th', 'thirtieth': '30th', 'fortieth': '40th',
    'fiftieth': '50th', 'sixtieth': '60th', 'seventieth': '70th', 'eightieth': '80th',
    'ninetieth': '90th', 'hundredth': '100th'
}

@lru_cache(maxsize=128)
def get_city_from_zip(zip_code):
    url = f"http://api.zippopotam.us/us/{zip_code}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'places' in data and len(data['places']) > 0:
            return data['places'][0]['place name']
    return None

async def fetch_city(session, zip_code):
    url = f"http://api.zippopotam.us/us/{zip_code}"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            if 'places' in data and len(data['places']) > 0:
                return zip_code, data['places'][0]['place name']
    return zip_code, None

async def fetch_city_map_async(zip_codes, max_concurrent_tasks=100):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        async def fetch_with_sem(zip_code):
            async with semaphore:
                return await fetch_city(session, zip_code)
        tasks = [fetch_with_sem(zip_code) for zip_code in zip_codes]
        results = await asyncio.gather(*tasks)
        return {zip_code: city for zip_code, city in results}

def fetch_city_map(zip_codes):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    city_map = loop.run_until_complete(fetch_city_map_async(zip_codes))
    return city_map

# Column mapping configuration with variations
column_mapping_config = {
    'property_address': ['property address', 'address', 'property_address','site address'],
    'property_city': ['property city', 'city', 'property_city'],
    'property_state': ['property state', 'state', 'property_state'],
    'property_zip': ['property zip', 'property zipcode', 'zip', 'zipcode', 'property_zip', 'property_zipcode','zip code'],
    'mailing_address': ['mailing address', 'owner address', 'mailing_address', 'owner_address'],
    'mailing_city': ['mailing city', 'owner city', 'mailing_city', 'owner_city'],
    'mailing_state': ['mailing state', 'owner state', 'mailing_state', 'owner_state'],
    'mailing_zip': ['mailing zip', 'mailing zipcode', 'owner zip', 'owner zipcode', 'mailing_zip', 'mailing_zipcode', 'owner_zip', 'owner_zipcode'],
    'full_name': ['full name', 'owner full name', 'first owner full name', 'full_name', 'owner_full_name', 'first_owner_full_name','owner contact name'],
    'first_name': ['first name', 'owner first name', 'first owner first name', 'first_name', 'owner_first_name', 'first_owner_first_name'],
    'last_name': ['last name', 'owner last name', 'first owner last name', 'last_name', 'owner_last_name', 'first_owner_last_name']
}

# Function to standardize column names for easier matching
def standardize_column_name(name):
    return re.sub(r'[\s_]+', ' ', name.strip().lower())

# Function to convert text to title case
def to_title_case(text):
    if isinstance(text, str):
        # Split the text into words and capitalize the first letter of each word
        return ' '.join(word.capitalize() for word in text.split())
    return text

# Function to map columns automatically
def map_columns(df, config):
    standardized_columns = {standardize_column_name(col): col for col in df.columns}
    mapped_columns = {}
    for key, possible_names in config.items():
        for name in possible_names:
            standardized_name = standardize_column_name(name)
            if standardized_name in standardized_columns:
                mapped_columns[key] = standardized_columns[standardized_name]
                break
        else:
            mapped_columns[key] = 'none'
    return mapped_columns

# Function to adjust cities based on ZIP codes
def adjust_cities(df, mapped_columns):
    def clean_zip(zip_code):
        return str(zip_code).replace(',', '').replace('.0', '')

    if mapped_columns['property_zip'] != 'none' and mapped_columns['property_city'] != 'none':
        property_zip_col = mapped_columns['property_zip']
        property_city_col = mapped_columns['property_city']

        # Clean and convert the property ZIP codes to strings
        df[property_zip_col] = df[property_zip_col].apply(clean_zip)

        zip_codes = df[property_zip_col].unique()
        city_map = fetch_city_map(zip_codes)

        df[property_city_col] = df[property_zip_col].map(city_map).fillna(df[property_city_col])

    if mapped_columns['mailing_zip'] != 'none' and mapped_columns['mailing_city'] != 'none':
        mailing_zip_col = mapped_columns['mailing_zip']
        mailing_city_col = mapped_columns['mailing_city']

        # Clean and convert the mailing ZIP codes to strings
        df[mailing_zip_col] = df[mailing_zip_col].apply(clean_zip)

        zip_codes = df[mailing_zip_col].unique()
        city_map = fetch_city_map(zip_codes)

        df[mailing_city_col] = df[mailing_zip_col].map(city_map).fillna(df[mailing_city_col])

    return df

def standardize_and_normalize_address(address):
    if isinstance(address, str):
        address = address.lower()
        for pattern, replacement in address_patterns:
            address = pattern.sub(replacement, address)
        words = address.split()
        new_words = [ordinal_mapping.get(word, word) for word in words]
        address = ' '.join(new_words)
        parts = address.split()
        if len(parts) > 2 and parts[-1].isdigit() and parts[0].isdigit():
            street_number1 = parts[0]
            street_number2 = parts[-1]
            street_name = ' '.join(parts[1:-1])
            return f"{street_number1}-{street_number2} {street_name}"
    return address

# Streamlit app starts here
st.title("Address Standardisation")

# File upload
uploaded_file = st.file_uploader("Upload your CSV or Excel file", type=['csv', 'xlsx'])

if uploaded_file is not None:
    # Detect the encoding
    raw_data = uploaded_file.read()
    result = chardet.detect(raw_data)
    encoding = result['encoding']
    uploaded_file.seek(0)  # Reset the file pointer to the beginning after reading

    # Read the file with detected encoding
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file, encoding=encoding)
        elif uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file)
        st.write(f"File Uploaded Successfully with {encoding} encoding")
    except Exception as e:
        st.error(f"Failed to read file: {e}")

    if 'df' in locals():
        # Map columns automatically
        mapped_columns = map_columns(df, column_mapping_config)

        # Check if required columns are mapped
        required_columns = ['property_address', 'property_city', 'property_zip']
        if all(col in mapped_columns for col in required_columns):
            st.write("Mapped Columns: ", mapped_columns)

            # Allow the user to adjust the mappings
            st.write("Please confirm or adjust the column mappings:")
            for key in column_mapping_config.keys():
                options = ['none'] + list(df.columns)
                default_index = options.index(mapped_columns.get(key)) if mapped_columns.get(key) in options else 0
                mapped_columns[key] = st.selectbox(f"Select column for {key.replace('_', ' ').title()}:", options, index=default_index)

            # Standardize button
            if st.button("Standardize"):
                # Apply the functions to the addresses
                if mapped_columns.get('property_address') != 'none':
                    property_address_col = mapped_columns['property_address']
                    df[property_address_col].fillna('', inplace=True)
                    df[property_address_col] = df[property_address_col].apply(standardize_and_normalize_address)

                if mapped_columns.get('mailing_address') != 'none':
                    mailing_address_col = mapped_columns['mailing_address']
                    df[mailing_address_col].fillna('', inplace=True)
                    df[mailing_address_col] = df[mailing_address_col].apply(standardize_and_normalize_address)

                # Adjust cities
                df = adjust_cities(df, mapped_columns)

                # Convert relevant columns to title case if they exist
                for key in ['full_name', 'first_name', 'last_name']:
                    if key in mapped_columns and mapped_columns[key] != 'none':
                        df[mapped_columns[key]] = df[mapped_columns[key]].apply(to_title_case)

                st.write("Addresses Normalized and Names Converted to Name Case Successfully")
                st.write(df.head())

                # Provide a text input for the user to specify the file name
                file_name, file_extension = os.path.splitext(uploaded_file.name)
                output_file_name = f"{file_name}_standardized.csv"

                # Provide download link for the updated file
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button("Download Updated File", data=csv, file_name=output_file_name, mime="text/csv")

                # Instruction for moving the file
                st.markdown("""
                    **Instructions:**
                    - After downloading, you can manually move the file to your desired location.
                    - To move the file, use your file explorer and drag the downloaded file to the preferred folder.
                """)
        else:
            st.error("Required columns are missing in the uploaded file.")
