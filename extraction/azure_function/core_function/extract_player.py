
import re
from typing import Any

import unicodedata
import logging

# Setup logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def return_player_lists(soup):
    logging.info("Entering function: return_player_lists")
    try:
        # 1. Find the main lineups grid container
        container = soup.find(
            "div",
            class_=re.compile(r"GridContainer-LineupsGridContainer")
        )
        if not container:
            logging.error("Could not find lineups grid container.")
            return None

        # 2. Find all player lists inside that container
        #    Prefer the stable data-testid if present, but fall back to class name.
        player_lists = container.find_all(
            "ul",
            attrs={"data-testid": "player-list"}
        )

        # Fallback: if for some reason data-testid disappeared, use 'PlayerList' in class
        if not player_lists:
            player_lists = container.find_all(
                "ul",
                class_=re.compile(r"PlayerList")
            )

        logging.debug(f"Found {len(player_lists)} player lists inside lineups container.")

        if len(player_lists) != 4:
            logging.warning(
                f"Expected 4 player lists (home starters, home subs, away starters, away subs), "
                f"but found {len(player_lists)}."
            )

        return player_lists

    except Exception as e:
        logging.error(f"Error in return_player_lists: {e}", exc_info=True)
        return None


def clean_text(text):
    logging.info("Entering function: clean_text")
    try:
        if text:
            text = unicodedata.normalize("NFKC", text).strip()
            return text.encode("utf-8").decode("utf-8")
        return None
    except Exception as e:
        logging.error(f"Error in clean_text: {e}")
        return None

def player_extraction_from_list(player_items):
    logging.info("Entering function: player_extraction_from_list")
    players_data = {}
    try:
        for player_item in player_items:
            player_name_tag = player_item.find('span', class_='ssrcss-15c46u3-PlayerName')
            player_name = player_name_tag.get_text(strip=True) if player_name_tag else "Unknown"
            playerNameCleaned = player_name.replace('(c)', '').strip()

            is_captain = '(c)' in player_item.get_text()

            player_number_tag = player_item.find('span', class_=re.compile(r'PlayerNumber'))
            player_number = player_number_tag.get_text(strip=True) if player_number_tag else "N/A"

            yellow_cards = []
            yellow_card_icons = player_item.find_all('img', {'src': re.compile('yellowcard')})
            for card in yellow_card_icons:
                card_time_tag = card.find_next('span', {'aria-hidden': 'true'})
                if card_time_tag:
                    yellow_cards.append(card_time_tag.get_text(strip=True))

            red_cards = []
            red_card_icons = player_item.find_all('img', {'src': re.compile('second-yellow-card|redcard')})
            for card in red_card_icons:
                card_time_tag = card.find_next('span', {'aria-hidden': 'true'})
                if card_time_tag:
                    red_cards.append(card_time_tag.get_text(strip=True))

            substitutions = []
            substitution_wrappers = player_item.find_all('span', class_='ssrcss-mm94gd-Wrapper')
            for sub_event in substitution_wrappers:
                sub_text = sub_event.get_text(strip=True)
                match = re.search(r"(.+?) (\d+'(?:\+\d+)?)", sub_text)

                if match:
                    replaced_by = match.group(1)
                    substitution_time = match.group(2).replace("'", "")

                    substitutions.append({
                        "playerName": player_name,
                        "WasSubstituted": True,
                        "SubstitutionTime": int(substitution_time),
                        "ReplacedBy": replaced_by
                    })

            players_data[playerNameCleaned] = {
                'substitutions_info': substitutions,
                'RedCardMinutes': red_cards,
                'RedCards': len(red_cards),
                'YellowCardMinutes': yellow_cards,
                'YellowCards': len(yellow_cards),
                'is_captain': is_captain,
                'ShirtNumber': player_number
            }

        return players_data
    except Exception as e:
        logging.error(f"Error in player_extraction_from_list: {e}")
        return players_data

def starter_sub_player_merge(starters, Subs):
    logging.info("Entering function: starter_sub_player_merge")
    import copy
    try:
        for subplayer in Subs:
            if subplayer not in starters:
                starters[subplayer] = Subs[subplayer]
                starters[subplayer]['source'] = 'Sub'
        for startPlayer in starters:
            if 'source' not in starters[startPlayer]:
                starters[startPlayer]['source'] = 'Start'

        merged_players = copy.deepcopy(starters)
        return merged_players
    except Exception as e:
        logging.error(f"Error in starter_sub_player_merge: {e}")
        return starters

def swap_subs_to_starter(subList):
    logging.info("Entering function: swap_subs_to_starter")
    try:
        if len(subList) != 1:
            for i in range(len(subList)):
                if i > 0:
                    subList[i]['playerName'] = subList[i - 1].get('ReplacedBy', 'Unknown')
        return subList
    except Exception as e:
        logging.error(f"Error in swap_subs_to_starter: {e}")
        return subList




import re
import logging
from bs4 import BeautifulSoup


def extract_players_and_assists(soup, searchString):
    logging.info("Entering function: extract_players_and_assists")
    try:
        # Using regex properly to match dynamic classes
        container = soup.find('div', class_=re.compile(searchString))
        if not container:
            logging.warning("No container found for player assists.")
            return {}

        player_data = {}
        spans = container.find_all('span', class_='visually-hidden')

        # Remove first span (team name) if it exists
        if spans:
            spans[0].extract()

        text = container.get_text(strip=True)

        # Regex pattern to extract player name and assist times
        pattern = r"([\w\s\.\-]+)\s*\(([^)]+)\)"
        matches = re.findall(pattern, text)

        for player_name, times in matches:
            player_name = player_name.strip()
            assist_times = [t.strip() for t in times.split(',')]

            if player_name not in player_data:
                player_data[player_name] = []

            player_data[player_name].extend(assist_times)

        return player_data

    except Exception as e:
        logging.error(f"Error in extract_players_and_assists: {e}")
        return {}




import logging
import re
from bs4 import BeautifulSoup

# Configure logging


import logging
import re
from bs4 import BeautifulSoup

# Configure logging

import logging
import re
from bs4 import BeautifulSoup

# Configure logging


import logging
import re
from bs4 import BeautifulSoup

# Configure loggingWARNING - Goal scorer J. Bryan not found in AwayTeamProcessed.



def extract_goal_events1(soup, event_type_class):
    logging.info("Entering function: extract_goal_events")
    goals_data = {}

    try:
        key_events_div = soup.find('div', class_=re.compile(f".*{event_type_class}.*"))
        if not key_events_div:
            logging.warning(f"No key events found for class {event_type_class}.")
            return goals_data

        event_items = key_events_div.find_all('li', class_=re.compile(".*StyledAction.*"))
        logging.info(f"Found {len(event_items)} event items.")

        for item in event_items:
            player_span = item.find('span', role='text')
            extra_info_span = item.find('span', class_='ssrcss-1t9po6g-TextBlock e102yuqa0')  # Contains "(26' og)"

            if not player_span:
                logging.warning("Skipping event: No player span found.")
                continue

            player_name = clean_text(player_span.get_text(strip=True))

            # Check if "og" (own goal) is present
            is_own_goal = False
            if extra_info_span and 'og' in extra_info_span.get_text().lower():
                is_own_goal = True  # Flag this as an own goal
                logging.info(f"Detected own goal for {player_name}")

            hidden_span = item.find('span', class_='visually-hidden ssrcss-1f39n02-VisuallyHidden e16en2lz0')

            if hidden_span:
                hidden_text = hidden_span.get_text(separator=', ', strip=True)

                if "Goal" in hidden_text or "Own Goal" in hidden_text:
                    goals = re.findall(r'(\d+)(?: minutes(?: plus (\d+))?)?', hidden_text)

                    goal_times = []
                    for minute, extra in goals:
                        goal_time = f"{minute}'"
                        if extra:
                            goal_time += f" +{extra}'"
                        if is_own_goal:
                            goal_time += " (OG)"  # Append "(OG)" only if it's an own goal
                        goal_times.append(goal_time)

                    if player_name not in goals_data:
                        goals_data[player_name] = []

                    goals_data[player_name].extend(goal_times)
                    goal_type = "Own Goal" if is_own_goal else "Goal"
                    logging.info(f"Recorded {goal_type} for {player_name} at {goal_times}")

        logging.info(f"Extracted goal data: {goals_data}")
        return goals_data

    except Exception as e:
        logging.error(f"Error in extract_goal_events: {e}", exc_info=True)
        return goals_data

import re
import logging
from bs4 import BeautifulSoup

def extract_goal_events_v2(soup, event_type_class):
    logging.info("Entering function: extract_goal_events")
    goals_data = {}

    try:
        key_events_div = soup.find('div', class_=re.compile(f".*{event_type_class}.*"))
        if not key_events_div:
            logging.warning(f"No key events found for class {event_type_class}.")
            return goals_data

        event_items = key_events_div.find_all('li', class_=re.compile(".*StyledAction.*"))
        logging.info(f"Found {len(event_items)} event items.")

        for item in event_items:
            player_span = item.find('span', role='text')
            extra_info_spans = item.find_all('span', class_='ssrcss-1t9po6g-TextBlock e102yuqa0')

            if not player_span:
                logging.warning("Skipping event: No player span found.")
                continue

            player_name = player_span.get_text(strip=True)

            # Extract raw goal texts for each timestamp (this contains 'og' or 'pen' if present)
            raw_goal_texts = [span.get_text(strip=True).lower() for span in extra_info_spans]

            hidden_span = item.find('span', class_='visually-hidden ssrcss-1f39n02-VisuallyHidden e16en2lz0')

            if hidden_span:
                hidden_text = hidden_span.get_text(separator=', ', strip=True)

                # Ignore non-goal events
                if "Goal" not in hidden_text and "Own Goal" not in hidden_text and "Penalty" not in hidden_text:
                    continue

                # Extract goal timestamps while checking if each specific goal is a penalty or own goal
                goal_matches = re.findall(r'(\d+)(?: minutes(?: plus (\d+))?)?', hidden_text)

                goal_times = []
                for idx, (minute, extra) in enumerate(goal_matches):
                    goal_time = f"{minute}'"
                    if extra:
                        goal_time = f"{minute}'+{extra}"

                    # Check if this specific goal is a penalty
                    is_penalty = "pen" in raw_goal_texts[idx] if idx < len(raw_goal_texts) else False
                    # Check if this specific goal is an own goal
                    is_own_goal = "og" in raw_goal_texts[idx] if idx < len(raw_goal_texts) else False

                    # Assign classifications
                    if is_penalty:
                        goal_time += " - P"  # Penalty Goal
                    elif is_own_goal:
                        goal_time += " - O"  # Own Goal
                    else:
                        goal_time += " - S"  # Standard Goal

                    goal_times.append(goal_time)

                if player_name not in goals_data:
                    goals_data[player_name] = []

                goals_data[player_name].extend(goal_times)

        logging.info(f"Extracted goal data: {goals_data}")
        return goals_data

    except Exception as e:
        logging.error(f"Error in extract_goal_events: {e}")
        return {}









def extract_goal_events(soup, event_type_class):
    logging.info("Entering function: extract_goal_events")
    goals_data = {}
    try:
        key_events_div = soup.find('div', class_=re.compile(f".*{event_type_class}.*"))
        if not key_events_div:
            logging.warning(f"No key events found for class {event_type_class}.")
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
                    goals = re.findall(r'(\d+)(?: minutes(?: plus (\d+))?)?', hidden_text)
                    goal_times = [f"{minute}' +{extra}" if extra else f"{minute}'" for minute, extra in goals]

                    if player_name not in goals_data:
                        goals_data[player_name] = []
                    goals_data[player_name].extend(goal_times)

        return goals_data
    except Exception as e:
        logging.error(f"Error in extract_goal_events: {e}")
        return goals_data

def process_sub_data(starters, merged):
    logging.info("Entering function: process_sub_data")
    try:
        for player in starters:
            sub_info = starters[player].get('substitutions_info', [])
            if sub_info:
                sortedSubs = swap_subs_to_starter(sub_info)
                for playerWhoWasSubbed in sortedSubs:
                    pn = playerWhoWasSubbed['playerName']
                    ws = playerWhoWasSubbed['WasSubstituted']
                    sbt = playerWhoWasSubbed['SubstitutionTime']
                    rb = playerWhoWasSubbed['ReplacedBy']

                    if pn in merged:
                        merged[pn]['WasSubstituted'] = ws
                        merged[pn]['SubstitutionTime'] = sbt
                        merged[pn]['ReplacedBy'] = rb
                    else:
                        logging.warning(f"Player {pn} not found in merged data.")

                    if rb in merged:
                        merged[rb]['WasIntroduced'] = True
                        merged[rb]['SubbedOnMinute'] = sbt
                    else:
                        logging.warning(f"Replacement player {rb} not found in merged data.")

        for player in merged:
            source = merged[player].get('source', 'Unknown')
            if source == 'Start':
                merged[player]['WasStarter'] = True
                if 'WasSubstituted' not in merged[player]:
                    merged[player]['WasSubstituted'] = False
                    merged[player]['MinutesPlayed'] = 98
                elif merged[player]['WasSubstituted']:
                    merged[player]['MinutesPlayed'] = merged[player]['SubstitutionTime']
            elif source == 'Sub':
                merged[player]['WasStarter'] = False
                if 'WasIntroduced' not in merged[player]:
                    merged[player]['WasIntroduced'] = False
                    merged[player]['MinutesPlayed'] = 0
                elif merged[player]['WasIntroduced'] and 'WasSubstituted' not in merged[player]:
                    merged[player]['WasSubstituted'] = False
                    merged[player]['MinutesPlayed'] = 98 - merged[player]['SubbedOnMinute']
                elif merged[player]['WasIntroduced'] and merged[player]['WasSubstituted']:
                    merged[player]['MinutesPlayed'] = merged[player]['SubstitutionTime'] - merged[player]['SubbedOnMinute']

            merged.pop('substitutions_info', None)
            merged.pop('source', None)
        return merged
    except Exception as e:
        logging.error(f"Error in process_sub_data: {e}")
        return merged

def generate_player_dictionaries(soup):
    logging.info("Entering function: generate_player_dictionaries")
    try:
        get_team_lists = return_player_lists(soup)

        if not get_team_lists or len(get_team_lists) != 4:
            logging.error("Team lists are incomplete or missing.")
            return [{}, {}]

        HomeTeamStarters = player_extraction_from_list(get_team_lists[0])
        HomeTeamSubs = player_extraction_from_list(get_team_lists[1])
        AwayTeamStarters = player_extraction_from_list(get_team_lists[2])
        AwayTeamSubs = player_extraction_from_list(get_team_lists[3])

        HomeFullTeamUnprocessed = starter_sub_player_merge(HomeTeamStarters, HomeTeamSubs)
        AwayFullTeamUnprocessed = starter_sub_player_merge(AwayTeamStarters, AwayTeamSubs)

        HomeTeamProcessed = process_sub_data(HomeTeamStarters, HomeFullTeamUnprocessed)
        AwayTeamProcessed = process_sub_data(AwayTeamStarters, AwayFullTeamUnprocessed)

        HomeGoals = extract_goal_events_v2(soup, 'KeyEventsHome')
        print (HomeGoals)
        HomeAssists = extract_players_and_assists(soup, ".*GroupedHomeEvent e1ojeme81*")
        for player in HomeGoals:
            if player in HomeTeamProcessed:
                try:
                    HomeTeamProcessed[player]['Goals'] = HomeGoals[player]
                except:
                    pass
            else:
                logging.warning(f"Goal scorer {player} not found in HomeTeamProcessed.")

        for player in HomeAssists:
            if player in HomeTeamProcessed:
                HomeTeamProcessed[player]['Assists'] = HomeAssists[player]
            else:
                logging.warning(f"Assist provider {player} not found in HomeTeamProcessed.")

        AwayGoals = extract_goal_events_v2(soup, 'KeyEventsAway')
        AwayAssists = extract_players_and_assists(soup, ".*GroupedAwayEvent e1ojeme80*")
        print(AwayGoals)
        for player in AwayGoals:
            if player in AwayTeamProcessed:
                try:
                    AwayTeamProcessed[player]['Goals'] = AwayGoals[player]
                except:
                    pass
            else:
                logging.warning(f"Goal scorer {player} not found in AwayTeamProcessed.")

        for player in AwayAssists:
            if player in AwayTeamProcessed:
                AwayTeamProcessed[player]['Assists'] = AwayAssists[player]
            else:
                logging.warning(f"Assist provider {player} not found in AwayTeamProcessed.")

        return [HomeTeamProcessed, AwayTeamProcessed]
    except Exception as e:
        logging.error(f"Error in generate_player_dictionaries: {e}")
        return [{}, {}]
