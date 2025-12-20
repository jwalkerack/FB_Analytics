
import re
from typing import Any

import unicodedata
import logging

# Setup logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def return_player_lists(soup):
    """
    Returns [home_starters, home_subs, away_starters, away_subs]
    Assumes exactly:
      - 2 TeamPlayers sections (starters)
      - 2 SubstitutesSection sections (subs)
    """
    logging.info("Entering function: return_player_lists")

    root = soup.select_one('div[data-testid="styled-match-lineup"]')
    if not root:
        logging.error("styled-match-lineup not found")
        return None

    main_section = root.find("section")
    if not main_section:
        logging.error("Main lineup section not found")
        return None

    # --- Starters ---
    starters = main_section.select('section[class*="TeamPlayers"]')
    if len(starters) < 2:
        logging.error(f"Expected 2 TeamPlayers (starters), found {len(starters)}")
        return None

    home_starters_ul = starters[0].select_one('ul[data-testid="player-list"]')
    away_starters_ul = starters[1].select_one('ul[data-testid="player-list"]')

    # --- Subs ---
    subs_sections = main_section.select('section[class*="SubstitutesSection"]')
    if len(subs_sections) < 2:
        logging.error(f"Expected 2 SubstitutesSection (subs), found {len(subs_sections)}")
        return None

    home_subs_ul = subs_sections[0].select_one('ul[data-testid="player-list"]')
    away_subs_ul = subs_sections[1].select_one('ul[data-testid="player-list"]')

    if not all([home_starters_ul, home_subs_ul, away_starters_ul, away_subs_ul]):
        logging.error("One or more player lists missing")
        return None

    return [home_starters_ul, home_subs_ul, away_starters_ul, away_subs_ul]


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

            # ----------------------------
            # 1. Player name (CLEAN, SINGLE SCOPE)
            # ----------------------------
            #name_span = player_item.select_one('span[class*="PlayerName"]')
            #player_name = name_span.get_text(strip=True) if name_span else "Unknown"
            name_span = player_item.select_one(
                'span[class*="-PlayerName"]:not([class*="Wrapper"])'
            )
            player_name = name_span.get_text(strip=True) if name_span else "Unknown"
            # ----------------------------
            # 2. Captain detection (STRUCTURAL, SEPARATE)
            # ----------------------------
            captain_marker = player_item.select_one(
                'span[role="text"] > span[aria-hidden="true"]'
            )
            is_captain = bool(
                captain_marker and captain_marker.get_text(strip=True) == "(c)"
            )

            # ----------------------------
            # 3. Shirt number
            # ----------------------------
            shirt_number_div = player_item.select_one(
                'div[aria-hidden="true"][class*="ShirtNumber"]'
            )
            shirt_number = (
                shirt_number_div.get_text(strip=True)
                if shirt_number_div else "N/A"
            )

            # ----------------------------
            # 4. Cards
            # ----------------------------
            yellow_cards = []
            red_cards = []

            for card in player_item.select('img[src*="yellowcard"]'):
                minute = card.find_next('span', {'aria-hidden': 'true'})
                if minute:
                    yellow_cards.append(minute.get_text(strip=True))

            for card in player_item.select('img[src*="redcard"], img[src*="second-yellow-card"]'):
                minute = card.find_next('span', {'aria-hidden': 'true'})
                if minute:
                    red_cards.append(minute.get_text(strip=True))

            # ----------------------------
            # 5. Substitutions
            # ----------------------------
            substitutions = []
            sub_container = player_item.select_one('span[class*="PlayerSubstitutes"]')

            if sub_container:
                for wrapper in sub_container.select('span[class*="Wrapper"]'):
                    visible = wrapper.select_one('span[aria-hidden="true"]')
                    sub_text = (
                        visible.get_text(" ", strip=True)
                        if visible else wrapper.get_text(" ", strip=True)
                    )

                    match = re.search(r"(.+?)\s+(\d+'(?:\+\d+)?)$", sub_text)
                    if match:
                        replaced_by = match.group(1).strip()
                        raw_time = match.group(2).replace("'", "")

                        try:
                            sub_time = int(raw_time)
                        except ValueError:
                            base = raw_time.split("+", 1)[0]
                            sub_time = int(base) if base.isdigit() else 0

                        substitutions.append({
                            "playerName": player_name,
                            "WasSubstituted": True,
                            "SubstitutionTime": sub_time,
                            "ReplacedBy": replaced_by
                        })

            # ----------------------------
            # 6. Store player (KEYED BY NAME – as per your current design)
            # ----------------------------
            players_data[player_name] = {
                "substitutions_info": substitutions,
                "RedCardMinutes": red_cards,
                "RedCards": len(red_cards),
                "YellowCardMinutes": yellow_cards,
                "YellowCards": len(yellow_cards),
                "is_captain": is_captain,
                "ShirtNumber": shirt_number
            }

        return players_data

    except Exception as e:
        logging.error(
            f"Error in player_extraction_from_list: {e}",
            exc_info=True
        )
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




def extract_goal_events_as_events(soup):
    """
    Extract ALL goal events as a unified list.

    Each event:
    {
      "scorer": "D. Daniels",
      "time_text": "7' og" | "45'+4" | "58' pen" | "90'+5" ...,
      "type": "NORMAL" | "PENALTY" | "OWN_GOAL",
      "credited_team_side": "home" | "away"
    }
    """
    events = []
    logging.info("Entering function : extract_goal_events_as_events")

    def parse_side(container_substring: str, credited_side: str):
        block = soup.select_one(f'div[class*="{container_substring}"]')
        if not block:
            return

        for item in block.select('li[class*="StyledAction"]'):
            scorer_span = item.find('span', role='text')
            if not scorer_span:
                continue
            scorer = clean_text(scorer_span.get_text(strip=True))

            # Visible bracket text blocks, e.g. "(58' pen)" / "(7' og)" / "(45'+4)"
            time_spans = item.find_all('span', class_=re.compile(r'TextBlock'))
            raw = " ".join(ts.get_text(" ", strip=True) for ts in time_spans).strip()

            # Clean the raw: remove parentheses and weird whitespace
            time_text = raw.replace("(", "").replace(")", "").strip().lower()

            # Hidden descriptive text e.g. "Penalty 58 minutes" / "Own Goal 7 minutes" / "Goal 31 minutes"
            hidden_span = item.find('span', class_=re.compile(r'VisuallyHidden'))
            hidden_text = hidden_span.get_text(" ", strip=True).lower() if hidden_span else ""

            # Classify type (prefer hidden; fallback to time_text)
            if "own goal" in hidden_text or " og" in time_text:
                goal_type = "OWN_GOAL"
            elif "penalty" in hidden_text or " pen" in time_text:
                goal_type = "PENALTY"
            else:
                goal_type = "NORMAL"

            events.append({
                "scorer": scorer,
                "time_text": time_text,          # <-- keep as raw text
                "type": goal_type,
                "credited_team_side": credited_side
            })

    parse_side("KeyEventsHome", "home")
    parse_side("KeyEventsAway", "away")
    return events




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

        # ---- Extract starters/subs ----
        HomeTeamStarters = player_extraction_from_list(get_team_lists[0])
        HomeTeamSubs = player_extraction_from_list(get_team_lists[1])
        AwayTeamStarters = player_extraction_from_list(get_team_lists[2])
        AwayTeamSubs = player_extraction_from_list(get_team_lists[3])

        # ---- Merge + compute minutes/sub info ----
        HomeFullTeamUnprocessed = starter_sub_player_merge(HomeTeamStarters, HomeTeamSubs)
        AwayFullTeamUnprocessed = starter_sub_player_merge(AwayTeamStarters, AwayTeamSubs)

        HomeTeamProcessed = process_sub_data(HomeTeamStarters, HomeFullTeamUnprocessed)
        AwayTeamProcessed = process_sub_data(AwayTeamStarters, AwayFullTeamUnprocessed)

        # ----------------------------------------------------------
        # GOALS (event-first, supports OWN GOALS + PENALTIES)
        # ----------------------------------------------------------
        goal_events = extract_goal_events_as_events(soup)

        def find_player_team(player_name: str):
            """Return (team_dict, team_label) where team_label is 'home' or 'away'."""
            if player_name in HomeTeamProcessed:
                return HomeTeamProcessed, "home"
            if player_name in AwayTeamProcessed:
                return AwayTeamProcessed, "away"
            return None, None

        unresolved_goal_events = []

        for ev in goal_events:
            scorer = ev.get("scorer", "Unknown")
            team_dict, _ = find_player_team(scorer)

            if not team_dict:
                unresolved_goal_events.append(ev)
                continue

            team_dict[scorer].setdefault("Goals", []).append(ev)

        # Optional: keep unresolved goals for QA/debugging
        if unresolved_goal_events:
            logging.warning(
                f"{len(unresolved_goal_events)} goal events could not be matched to a player."
            )
            HomeTeamProcessed.setdefault("_unresolved_goal_events", []).extend(unresolved_goal_events)

        # ----------------------------------------------------------
        # ASSISTS (keep your existing logic as-is)
        # ----------------------------------------------------------
        HomeAssists = extract_players_and_assists(soup, ".*GroupedHomeEvent e1ojeme81*")
        for player in HomeAssists:
            if player in HomeTeamProcessed:
                HomeTeamProcessed[player]['Assists'] = HomeAssists[player]
            else:
                logging.warning(f"Assist provider {player} not found in HomeTeamProcessed.")

        AwayAssists = extract_players_and_assists(soup, ".*GroupedAwayEvent e1ojeme80*")
        for player in AwayAssists:
            if player in AwayTeamProcessed:
                AwayTeamProcessed[player]['Assists'] = AwayAssists[player]
            else:
                logging.warning(f"Assist provider {player} not found in AwayTeamProcessed.")

        return [HomeTeamProcessed, AwayTeamProcessed]

    except Exception as e:
        logging.error(f"Error in generate_player_dictionaries: {e}", exc_info=True)
        return [{}, {}]
