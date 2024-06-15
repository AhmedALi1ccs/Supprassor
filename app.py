import streamlit as st
import pandas as pd
import re
import requests
import os
import openpyxl
# Define the address mappings and normalization functions
address_mapping = {
    r'\bavenue\b\.?': 'Ave', r'\bav\b\.?': 'Ave', r'\bave\b\.?': 'Ave',
    r'\bstreet\b\.?': 'St', r'\bstr\b\.?': 'St', r'\bst\b\.?': 'St',
    r'\bboulevard\b\.?': 'Blvd', r'\bblv\b\.?': 'Blvd', r'\bblvd\b\.?': 'Blvd',
    r'\bRoute\b\.?': 'Rt',
    r'\bbl\b\.?': 'Blvd', r'\bbl\b\.?': 'Blvd',
    r'\broad\b\.?': 'Rd', r'\brd\b\.?': 'Rd',
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
}

ordinal_mapping = {
    'first': '1st', 'second': '2nd', 'third': '3rd', 'fourth': '4th', 'fifth': '5th',
    'sixth': '6th', 'seventh': '7th', 'eighth': '8th', 'ninth': '9th', 'tenth': '10th',
    'eleventh': '11th', 'twelfth': '12th', 'thirteenth': '13th', 'fourteenth': '14th',
    'fifteenth': '15th', 'sixteenth': '16th', 'seventeenth': '17th', 'eighteenth': '18th',
    'nineteenth': '19th', 'twentieth': '20th', 'thirtieth': '30th', 'fortieth': '40th',
    'fiftieth': '50th', 'sixtieth': '60th', 'seventieth': '70th', 'eightieth': '80th',
    'ninetieth': '90th', 'hundredth': '100th'
}

def ordinal_to_numeric(address):
    if isinstance(address, str):
        words = address.split()
        new_words = [ordinal_mapping.get(word.lower(), word) for word in words]
        return ' '.join(new_words)
    return address

def standardize_address(address):
    if isinstance(address, str):
        for pattern, replacement in address_mapping.items():
            address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
        return address
    return address

def normalize_condo_address(address):
    if isinstance(address, str):
        parts = address.split()
        if len(parts) > 2 and parts[-1].isdigit() and parts[0].isdigit():
            street_number1 = parts[0]
            street_number2 = parts[-1]
            street_name = ' '.join(parts[1:-1])
            return f"{street_number1}-{street_number2} {street_name}"
        return address
    return address

def get_city_from_zip(zip_code):
    url = f"http://api.zippopotam.us/us/{zip_code}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'places' in data and len(data['places']) > 0:
            return data['places'][0]['place name']
    return None

def adjust_cities(df, property_zip_col, property_city_col, mailing_zip_col=None, mailing_city_col=None):
    df[property_city_col] = df.apply(lambda row: get_city_from_zip(row[property_zip_col]) if get_city_from_zip(row[property_zip_col]) else row[property_city_col], axis=1)
    if mailing_zip_col and mailing_city_col:
        df[mailing_city_col] = df.apply(lambda row: get_city_from_zip(row[mailing_zip_col]) if get_city_from_zip(row[mailing_zip_col]) else row[mailing_city_col], axis=1)
    return df

# Streamlit app starts here
st.title("Address Standardisation")

# File upload
uploaded_file = st.file_uploader("Upload your CSV or Excel file", type=['csv', 'xlsx'])

if uploaded_file is not None:
    try:
        # Read the file assuming UTF-8 encoding
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file)
        st.write("File Uploaded Successfully")
    except Exception as e:
        st.error(f"Failed to read file: {e}")

    if 'df' in locals():
        # Select columns for property address and city
        property_address_col = st.selectbox("Select the column for Property Address", df.columns)
        property_city_col = st.selectbox("Select the column for Property City", df.columns)
        property_zip_col = st.selectbox("Select the column for Property Zip Code", df.columns)
        
        # Select columns for mailing address and city (optional)
        mailing_address_col = st.selectbox("Select the column for Mailing Address (Optional)", ['None'] + list(df.columns))
        mailing_city_col = st.selectbox("Select the column for Mailing City (Optional)", ['None'] + list(df.columns))
        mailing_zip_col = st.selectbox("Select the column for Mailing Zip Code (Optional)", ['None'] + list(df.columns))

        if st.button("Normalize Addresses"):
            # Apply the functions to the addresses
            df[property_address_col] = df[property_address_col].apply(ordinal_to_numeric)
            df[property_address_col] = df[property_address_col].apply(standardize_address)
            df[property_address_col] = df[property_address_col].apply(normalize_condo_address)
            
            if mailing_address_col != 'None':
                df[mailing_address_col] = df[mailing_address_col].apply(ordinal_to_numeric)
                df[mailing_address_col] = df[mailing_address_col].apply(standardize_address)
                df[mailing_address_col] = df[mailing_address_col].apply(normalize_condo_address)
            
            # Adjust cities
            df = adjust_cities(df, property_zip_col, property_city_col, mailing_zip_col if mailing_zip_col != 'None' else None, mailing_city_col if mailing_city_col != 'None' else None)

            st.write("Addresses Normalized Successfully")
            st.write(df.head())
            
            # Provide a text input for the user to specify the file name
            file_name, file_extension = os.path.splitext(uploaded_file.name)
            output_file_name = f"{file_name}_standardized{'.csv'}"
            
            # Provide download link for the updated file
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("Download Updated File", data=csv, file_name=output_file_name, mime="text/csv")
            
            # Instruction for moving the file
            st.markdown("""
                **Instructions:**
                - After downloading, you can manually move the file to your desired location.
                - To move the file, use your file explorer and drag the downloaded file to the preferred folder.
            """)
