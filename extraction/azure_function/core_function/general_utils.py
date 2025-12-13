import datetime
import logging

logger = logging.getLogger()

def generate_file_name(league, period):
    """Generates a filename like 'English Championship_2023-08_2025-02-03'."""
    try:
        stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{league}_{period}_{stamp}"
        logger.info(f"Generated filename: {filename}")
        return filename
    except Exception as e:
        logger.error(f"Error in generate_file_name: {e}")
        return None

def convert_date_format(date_str):
    """Converts 'Sat 1 Feb 2025' to 'DDMMYYYY'."""
    try:
        parsed_date = datetime.datetime.strptime(date_str, "%a %d %b %Y")
        return parsed_date.strftime("%d%m%Y")
    except ValueError as e:
        logger.error(f"Error parsing date: {e}")
        return None
def getYearMonthString():
    """Returns the previous day's date in YYYY-MM format."""
    try:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        return yesterday.strftime("%Y-%m")
    except Exception as e:
        logger.error(f"Unexpected error in convert_date_format: {e}")
    return None