import json
import logging

import azure.functions as func



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered Function that runs the scraper.

    Optional JSON body:
      {
        "period": "YYYY-MM"   # e.g. "2025-02"
      }

    If not provided, falls back to getYearMonthString().
    """
    logger.info("ScrapeMatchesHttp function started.")

    try:
        from core_function.models import leagues
        from core_function.process_games import process_games_for_months
        from core_function.general_utils import getYearMonthString
        # Try to parse JSON body (may be empty)
        try:
            body = req.get_json()
        except ValueError:
            body = {}

        period = body.get("period") or getYearMonthString()
        logger.info(f"Processing matches for period: {period}")

        # This is your existing core logic
        process_games_for_months([period], leagues)

        return func.HttpResponse(
            status_code=200,
            body=json.dumps({"message": "Scrape completed", "period": period}),
            mimetype="application/json",
        )

    except Exception as e:
        logger.error(f"Error in ScrapeMatchesHttp: {e}", exc_info=True)
        return func.HttpResponse(
            status_code=500,
            body=json.dumps({"message": "Error running scraper", "error": str(e)}),
            mimetype="application/json",
        )
