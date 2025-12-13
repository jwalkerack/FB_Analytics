import re
import unicodedata
from .extract_player import generate_player_dictionaries
import logging
logger = logging.getLogger()
# ----------------------------------------------
# UTF-8 Encoding Fix
# ----------------------------------------------
def clean_text(text):
    """Normalize text encoding to ensure correct special characters (UTF-8)."""
    if text:
        text = unicodedata.normalize("NFKC", text).strip()
        return text.encode("utf-8").decode("utf-8")  #  Forces correct UTF-8 representation
    return None


# ----------------------------------------------
#  1. core_function Data Extraction Functions
# ----------------------------------------------


def extract_match_identifiers(soup_object):
    """Extracts match identifiers from the soup object."""
    try:
        elements = soup_object.find_all('li', attrs={"data-tipo-topic-id": True})
        if not elements:
            logger.warning("No match identifiers found in the soup object.")
        else:
            logger.info([element['data-tipo-topic-id'] for element in elements])
        return [element['data-tipo-topic-id'] for element in elements]
    except Exception as e:
        logger.error(f"Unexpected error in extract_match_identifiers: {e}")
        return []





def get_match_played_on_date(soup):
    """Extract the match played-on date."""
    played_on = soup.find('time', class_='ssrcss-1hjuztf-Date ejf0oom1')
    return clean_text(played_on.text) if played_on else None


def get_venue(soup):
    """Extract the match venue."""
    try:
        # Find any div whose class name ends with 'Venue'
        venue_element = soup.find('div', class_=re.compile(r'Venue$'))
        if not venue_element:
            return None

        # The text often looks like "Venue: Some Stadium"
        text = venue_element.get_text(strip=True)
        if "Venue:" in text:
            text = text.split("Venue:", 1)[-1].strip()

        return clean_text(text)
    except Exception as e:
        logger.error(f"Error in get_venue: {e}", exc_info=True)
        return None


def get_attendance(soup):
    """Extract attendance numbers from the match."""
    attendance_element = soup.find('div', class_='ssrcss-13d7g0c-AttendanceValue')
    return clean_text(attendance_element.text.split("Attendance:")[-1].strip()) if attendance_element else None


def get_home_team_name(soup):
    """Extracts the home team's name, handling missing elements safely."""
    home_team_container = soup.find('div', class_='ssrcss-bon2fo-WithInlineFallback-TeamHome')

    # Check if the container exists
    if home_team_container:
        home_team_name = home_team_container.find('span', class_='ssrcss-1p14tic-DesktopValue')
        if home_team_name:
            return home_team_name.text  # Keep the original extraction logic

    return None


def get_home_score(soup):
    """Extract home team's score."""
    home_score_element = soup.find('div', class_='ssrcss-qsbptj-HomeScore')

    return clean_text(home_score_element.text) if home_score_element else None

def get_away_score(soup):
    """Extract away team's score."""
    away_score_element = soup.find('div', class_='ssrcss-fri5a2-AwayScore')
    return clean_text(away_score_element.text) if away_score_element else None


def get_away_team_name(soup):
    """Extract the away team name."""
    away_team_container = soup.find('div', class_='ssrcss-nvj22c-WithInlineFallback-TeamAway')
    if away_team_container:
        away_team_name = away_team_container.find('span', class_='ssrcss-1p14tic-DesktopValue')
        if away_team_name:
            return away_team_name.text  # Keep the original extraction logic

    return None




HOME_POSSESSION_CLASS = "ssrcss-wtr58o-Value emwj40c0"
AWAY_POSSESSION_CLASS = "ssrcss-1exmi76-Value emwj40c0"

def get_possession(soup):
    """Extract possession statistics."""
    try:
        all_values = soup.find_all(
            "div",
            class_=[HOME_POSSESSION_CLASS, AWAY_POSSESSION_CLASS],
        )

        if len(all_values) < 2:
            logger.warning(
                f"Expected 2 possession values, found {len(all_values)}. "
                f"Classes used: {HOME_POSSESSION_CLASS}, {AWAY_POSSESSION_CLASS}"
            )
            return None, None

        home_possession = clean_text(all_values[0].get_text(strip=True))
        away_possession = clean_text(all_values[1].get_text(strip=True))

        return home_possession, away_possession

    except Exception as e:
        logger.error(f"Error in get_possession: {e}", exc_info=True)
        return None, None


# ----------------------------------------------
# 2. Player and Event Data Extraction
# ----------------------------------------------
def extract_goal_events(soup, event_type_class):
    """Extract goal events for home and away teams."""
    goals_data = {}
    key_events_div = soup.find('div', class_=re.compile(f".*{event_type_class}.*"))

    if not key_events_div:
        return goals_data

    event_items = key_events_div.find_all('li', class_=re.compile(".*StyledAction.*"))

    for item in event_items:
        player_span = item.find('span', role='text')
        if not player_span:
            continue

        player_name = clean_text(player_span.get_text(strip=True))
        hidden_span = item.find('span', class_='visually-hidden ssrcss-1f39n02-VisuallyHidden e16en2lz0')

        if hidden_span:
            hidden_text = hidden_span.get_text(separator=', ', strip=True)
            if "Goal" in hidden_text:
                goal_times = re.findall(r'(\d+)(?: minutes(?: plus (\d+))?)?', hidden_text)
                goals_data[player_name] = [f"{minute}' +{extra}" if extra else f"{minute}'" for minute, extra in
                                           goal_times]

    return goals_data


def extract_players_and_assists(soup, searchString):
    """Extract assists and associated times from the match report."""
    container = soup.find('div', class_=re.compile(searchString))
    if not container:
        return {}

    player_data = {}
    spans = container.find_all('span', class_='visually-hidden')
    if spans:
        spans[0].extract()

    text = container.get_text(strip=True)
    entries = text.split(',')

    for entry in entries:
        if '(' in entry and ')' in entry:
            player_info, time_info = entry.split('(')
            player_name = clean_text(player_info.strip())
            assist_time = time_info.strip(')').strip()

            if player_name not in player_data:
                player_data[player_name] = []

            player_data[player_name].append(assist_time)

    return player_data

def get_formations(soup):
    """
    Returns (home_formation, away_formation) or (None, None) if not found.
    """
    try:
        # Find all elements that hold the formation text
        formation_elems = soup.find_all(class_=re.compile(r'TeamDetailsValue-FormationValue'))

        home_form = formation_elems[0].get_text(strip=True) if len(formation_elems) > 0 else None
        away_form = formation_elems[1].get_text(strip=True) if len(formation_elems) > 1 else None

        return [home_form, away_form]
    except Exception as e:
        logger.error(f"Error in get_formations: {e}", exc_info=True)
        return [None, None]

def _extract_manager_from_details(details_block):
    """Helper: from a TeamDetails block, pull out the 'Manager' value."""
    if not details_block:
        return None

    # Each detail row
    rows = details_block.find_all(class_=re.compile(r'Detail'))
    for row in rows:
        label = row.find(class_=re.compile(r'TeamDetailsLabel'))
        if not label:
            continue
        if "Manager" in label.get_text(strip=True):
            value = row.find(class_=re.compile(r'TeamDetailsValue'))
            if value:
                return clean_text(value.get_text(strip=True))
    return None


def get_managers(soup):
    """
    Returns [home_manager, away_manager].
    """
    try:
        # Both home and away TeamDetails blocks
        details_blocks = soup.find_all(class_=re.compile(r'TeamDetails'))
        home_details = details_blocks[0] if len(details_blocks) > 0 else None
        away_details = details_blocks[1] if len(details_blocks) > 1 else None

        home_manager = _extract_manager_from_details(home_details)
        away_manager = _extract_manager_from_details(away_details)

        return [home_manager, away_manager]
    except Exception as e:
        logger.error(f"Error in get_managers: {e}", exc_info=True)
        return [None, None]
# ----------------------------------------------
# 3. Master Function: GetGameData
# ----------------------------------------------
def GetGameData(soup,league,bbcKey):
    """Extracts all key match details from the given BeautifulSoup object, including players."""
    if not soup:
        return {"error": "Invalid Soup Object"}

    home_possession, away_possession = get_possession(soup)
    home_formation, away_formation = get_formations(soup)
    home_manager, away_manager = get_managers(soup)

    # Extract players (this includes lineup, subs, goals, and assists)
    player_data = generate_player_dictionaries(soup)

    match_data = {
        "match_id": bbcKey,
        "played_on": get_match_played_on_date(soup),
        "venue": get_venue(soup),
        "attendance": get_attendance(soup),
        "League_Name": league,
        "home_team": {
            "formation" : home_formation,
            "manager": home_manager,
            "name": get_home_team_name(soup),
            "score": get_home_score(soup),
            "possession": home_possession,
            "players": player_data[0]  # Home team players with goals, assists, subs
        },
        "away_team": {
            "formation": away_formation,
            "manager": away_manager,
            "name": get_away_team_name(soup),
            "score": get_away_score(soup),
            "possession": away_possession,
            "players": player_data[1]  # Away team players with goals, assists, subs
        }
    }

    return match_data

