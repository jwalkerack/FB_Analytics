import logging
from .web_utils import Generate_Soup


from .azure_storage import (
    get_json_from_adls,
    update_json_in_adls,
    save_match_data_to_adls,
)


from .general_utils import generate_file_name
from .extract_game_data import GetGameData , extract_match_identifiers


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_games_for_months(months_to_process, leagues):
    """
    Processes match data for a list of given months.

    Args:
        months_to_process (list): A list of YYYY-MM strings representing months to process.
        leagues (dict): Dictionary of leagues and their URLs.
    """
    for stringYearMonth in months_to_process:
        for league, league_url in leagues.items():
            try:
                leagueYearMonth = f"{league_url}/{stringYearMonth}?filter=results"
                makeCallLeagueYearMonth = Generate_Soup(leagueYearMonth)

                if not makeCallLeagueYearMonth[1]:
                    logger.error(f"Failed to fetch league data: {leagueYearMonth}")
                    continue

                id_dictionary = get_json_from_adls()
                if not isinstance(id_dictionary, dict):
                    logger.error("Failed to fetch valid match identifiers from storage")
                    continue

                JSON_LIST = []
                MATCH_LIST = []
                ERROR_LIST = []
                returnLeagueYearMonthIds = extract_match_identifiers(makeCallLeagueYearMonth[0])
                logger.info(f"Match IDs to be processed: {returnLeagueYearMonthIds}")

                for page in returnLeagueYearMonthIds:
                    if page not in id_dictionary["identifiers"]:
                        matchURL = f"https://www.bbc.co.uk/sport/football/live/{page}"
                        callMatch = Generate_Soup(matchURL)

                        if callMatch[1]:
                            logger.info(f"Processing match: {matchURL}")
                            match_data = GetGameData(callMatch[0], league, page)
                            JSON_LIST.append(match_data)
                            MATCH_LIST.append(page)
                        else:
                            logger.warning(f"Failed to fetch match data: {matchURL}")
                            ERROR_LIST.append(page)
                            logger.info(f"Match Data append to Json list")

                # 🔹 Prevent Saving Empty Files 🔹
                if JSON_LIST:
                    filename = generate_file_name(league, stringYearMonth)
                    saveToBucket = save_match_data_to_adls(JSON_LIST, filename)

                    if not saveToBucket:
                        logger.error(f"Failed to save match data to S3 for {filename}")
                    else:
                        logger.info(f"SaveWorked {filename}")
                else:
                    logger.info(f"No match data to save for {league} {stringYearMonth}, skipping S3 save.")

                # 🔹 Only Update Identifiers If We Actually Processed Matches 🔹
                if MATCH_LIST:
                    for m in MATCH_LIST:
                        id_dictionary["identifiers"][m] = {"status": "uploaded", "version": 2}
                if ERROR_LIST:
                    for m1 in ERROR_LIST:
                        id_dictionary["identifiers"][m1] = {"status": "error", "version": 2}


                success = update_json_in_adls(id_dictionary)
                if not success:
                    logger.error("Failed to update match identifiers in S3.")
                else:
                    logger.info(f"No new match IDs processed for {league} {stringYearMonth}, skipping identifier update.")

            except Exception as e:
                logger.error(f"Unexpected error in process_games_for_months: {e}")



