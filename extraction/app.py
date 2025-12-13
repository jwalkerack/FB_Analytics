import json
import logging

import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))


from extraction.azure_function.core_function import leagues
from extraction.azure_function.core_function import process_games_for_months
from extraction.azure_function.core_function import getYearMonthString

# Logger Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    """Main AWS Lambda handler."""
    # Log the full event for debugging
    logger.info("Lambda function started.")
    logger.info(f"Received event: {json.dumps(event, indent=4)}")

    try:
        # Extract the previous month's YYYY-MM format
        stringYearMonth = getYearMonthString()
        # Process the games for the extracted month
        process_games_for_months([stringYearMonth], leagues)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Daily update completed!"})
        }

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error processing request", "error": str(e)})
        }

if __name__ == "__main__":
    event = {}
    lambda_handler(event, None)








