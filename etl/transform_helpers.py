import re

def categorize_city(value):
    val = str(value).strip().lower()
    if any(keyword in val for keyword in ['a', 'city a', 'sity a']):
        return 'City A'
    elif any(keyword in val for keyword in ['b', 'city b', 'cit b']):
        return 'City B'
    return 'Unknown'

def categorize_province(value):
    province_map = {
        'ab': 'AB', 'alberta': 'AB',
        'bc': 'BC', 'british columbia': 'BC',
        'mb': 'MB', 'manitoba': 'MB',
        'nb': 'NB', 'new brunswick': 'NB',
        'nl': 'NL', 'newfoundland and labrador': 'NL',
        'ns': 'NS', 'nova scotia': 'NS',
        'nt': 'NT', 'northwest territories': 'NT',
        'nu': 'NU', 'nunavut': 'NU',
        'on': 'ON', 'ontario': 'ON',
        'pe': 'PE', 'prince edward island': 'PE',
        'qc': 'QC', 'quebec': 'QC',
        'sk': 'SK', 'saskatchewan': 'SK',
        'yt': 'YT', 'yukon': 'YT'
    }
    if not isinstance(value, str):
        return None
    val = value.strip().lower()
    return province_map.get(val, None)

def standardize_phone(value):
    digits = re.sub(r'\D', '', str(value))
    if digits.startswith('1') and len(digits) == 11:
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return None


def infer_missing_province(row):
    area_code_to_province = {
        '204': 'MB', '431': 'MB',
        '236': 'BC', '250': 'BC', '604': 'BC', '672': 'BC',
        '403': 'AB', '587': 'AB', '780': 'AB', '825': 'AB',
        '306': 'SK', '639': 'SK',
        '416': 'ON', '437': 'ON', '647': 'ON', '905': 'ON',
        '289': 'ON', '519': 'ON', '226': 'ON', '613': 'ON', '343': 'ON',
        '514': 'QC', '438': 'QC', '418': 'QC', '581': 'QC',
        '819': 'QC', '873': 'QC',
        '506': 'NB',
        '709': 'NL',
        '902': 'NS', '782': 'NS',
        '867': 'NT'
    }

    current = str(row['province']).strip().lower()
    if current not in ['', 'none', 'nan', 'ca']:
        return current.upper()

    phone = row['owner_phone']
    digits = re.sub(r'\D', '', str(phone))
    if digits.startswith('1') and len(digits) == 11:
        digits = digits[1:]

    if not digits or len(digits) < 3:
        return None

    area_code = digits[:3]
    return area_code_to_province.get(area_code, "Add area code to dict")

def standardize_species(val):
    val = str(val).strip().lower()
    return 'dog' if val in ['dog', 'd'] else 'cat' if val in ['cat', 'c'] else None

def standardize_unit(val):
    val = str(val).strip().upper()
    return val if len(val) == 1 and val.isalpha() else None
