from datetime import datetime

def calculate_age(dob):
    """Calculate age from date of birth."""
    today = datetime.now()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

def days_since_last_consulted(last_date):
    """Calculate days since last consultation."""
    today = datetime.now()
    return (today - last_date).days

def safe_parse_date(date_str):
    """Safely parse a date with multiple formats."""
    formats = ["%Y%m%d", "%m%d%Y", "%Y-%m-%d %H:%M:%S"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Error parsing date: {date_str}")
