import os
import discord
from discord.ext import commands, tasks
from discord import app_commands
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import asyncio
import re
from discord import ui, ButtonStyle, Member
from discord.ui import View, Button, Modal, TextInput
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
import gspread.exceptions
import math
from pathlib import Path # Import Path for export function
from zoneinfo import ZoneInfo

# ---------------------------
# 🔹 Constants
# ---------------------------
CST = ZoneInfo('America/Chicago')
GUILD_ID = 1272629330115297330

# Channel and Role IDs
SANG_CHANNEL_ID = 1338295765759688767
STAFF_ROLE_ID = 1272635396991221824
MEMBER_ROLE_ID = 1272633036814946324
MENTOR_ROLE_ID = 1306021911830073414
SANG_ROLE_ID = 1387153629072592916
TOB_ROLE_ID = 1272694636921753701
SENIOR_STAFF_CHANNEL_ID = 1336473990302142484
ADMINISTRATOR_ROLE_ID = 1272961765034164318
SENIOR_STAFF_ROLE_ID = 1336473488159936512
SANG_VC_CATEGORY_ID = 1376645103803830322
SANG_POST_CHANNEL_ID = 1338295765759688767

# GSheet Config
SANG_SHEET_ID = "1CCpDAJO7Cq581yF_-rz3vx7L_BTettVaKglSvOmvTOE"
SANG_SHEET_TAB_NAME = "SangSignups"
SANG_HISTORY_TAB_NAME = "History"
SANG_SHEET_HEADER = ["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Mentor_Request", "Timestamp"]

# Message Content
SANG_MESSAGE_IDENTIFIER = "Sanguine Sunday Sign Up"
SANG_MESSAGE = f"""\
# {SANG_MESSAGE_IDENTIFIER} – Hosted by Macflag <:sanguine_sunday:1388100187985154130>

Looking for a fun Sunday activity? Look no farther than **Sanguine Sunday!**
Spend an afternoon or evening sending **Theatre of Blood** runs with clan members.
The focus on this event is on **Learners** and general KC.

We plan to have mentors on hand to help out with the learners.
A learner is someone who needs the mechanics explained for each room.

───────────────────────────────
**ToB Learner Resource Hub**

All Theatre of Blood guides, setups, and related resources are organized here:
➤ [**ToB Resource Hub**](https://discord.com/channels/1272629330115297330/1426262876699496598)

───────────────────────────────

LEARNERS – please review this thread, watch the xzact guides, and get your plugins set up before Sunday:
➤ [**Guides & Plugins**](https://discord.com/channels/1272629330115297330/1388887895837773895)

No matter if you're a learner or an experienced raider, we strongly encourage you to use one of the setups in this thread:

⚪ [**Learner Setups**](https://discord.com/channels/1272629330115297330/1426263868950450257)
🔵 [**Rancour Meta Setups**](https://discord.com/channels/1272629330115297330/1426272592452391012)

───────────────────────────────
**Sign-Up Here!**

Click a button below to sign up for the event.
- **Raider:** Fill out the form with your KC and gear.
- **Mentor:** Fill out the form to sign up as a mentor.
- **Withdraw:** Remove your name from this week's signup list.

The form will remember your answers from past events! 
You only need to edit Kc's and Roles.

Event link: <https://discord.com/events/1272629330115297330/1386302870646816788>

||<@&{MENTOR_ROLE_ID}> <@&{SANG_ROLE_ID}> <@&{TOB_ROLE_ID}>||
"""

LEARNER_REMINDER_IDENTIFIER = "Sanguine Sunday Learner Reminder"
LEARNER_REMINDER_MESSAGE = f"""\
# {LEARNER_REMINDER_IDENTIFIER} ⏰ <:sanguine_sunday:1388100187985154130>

This is a reminder for all learners who signed up for Sanguine Sunday!

Please make sure you have reviewed the following guides and have your gear and plugins ready to go:
• **[ToB Resource Hub](https://discord.com/channels/1272629330115297330/1426262876699496598)**
• **[Learner Setups](https://discord.com/channels/1272629330115297330/1426263868950450257)**
• **[Rancour Meta Setups](https://discord.com/channels/1272629330115297330/1426272592452391012)**
• **[Guides & Plugins](https://discord.com/channels/1272629330115297330/1426263621440372768)**

We look forward to seeing you there!
"""

# Store last generated teams in memory
last_generated_teams: List[List[Dict[str, Any]]] = []

# ---------------------------
# 🔹 Helper Functions
# ---------------------------
def sanitize_nickname(name: str) -> str:
    """Removes special characters and bot-added tags from display names."""
    if not name:
        return ""
    # Remove (RSN#1234) or [RSN#1234] tags
    name = re.sub(r'\s*\([^)]*#\d{4}\)', '', name)
    name = re.sub(r'\s*\[[^\]]*#\d{4}\]', '', name)
    # Remove leading @ from names
    name = re.sub(r'^@', '', name)
    return name.strip()

def normalize_role(p: dict) -> str:
    """Standardizes a player's proficiency based on their sheet data."""
    prof = str(p.get("proficiency","")).strip().lower()
    if prof == "mentor":
        return "mentor"
    try:
        kc = int(p.get("kc") or p.get("KC") or 0)
    except Exception:
        # Fallback for "X" KC or other non-int
        return prof
    if kc <= 10: return "new"
    if 11 <= kc <= 25: return "learner"
    if 26 <= kc <= 100: return "proficient"
    return "highly proficient"

PROF_ORDER = {"mentor": 0, "highly proficient": 1, "proficient": 2, "learner": 3, "new": 4}

def prof_rank(p: dict) -> int:
    """Returns a sortable integer for a player's proficiency."""
    return PROF_ORDER.get(normalize_role(p), 99)

def scythe_icon(p: dict) -> str:
    """Returns a Scythe emoji icon based on player data."""
    return "✅" if p.get("has_scythe") else "❌"

def freeze_icon(p: dict) -> str:
    """Returns a freeze icon if the player wants to learn."""
    return "❄️ Learn Freeze" if p.get("learning_freeze") else ""

def is_proficient_plus(p: dict) -> bool:
    """Checks if a player is proficient, highly proficient, or mentor."""
    role = normalize_role(p)
    return role in ("mentor", "highly proficient", "proficient")

def parse_roles(roles_str: str) -> (bool, bool):
    """Parses the 'Favorite Roles' string to check for range/melee knowledge."""
    if not roles_str or roles_str == "N/A":
        return False, False
    roles_str = roles_str.lower()
    knows_range = any(s in roles_str for s in ["range", "ranger", "rdps"])
    knows_melee = any(s in roles_str for s in ["melee", "mdps", "meleer"])
    return knows_range, knows_melee

def matchmaking_algorithm(available_raiders: List[Dict[str, Any]]):
    """
    Core algorithm for sorting players into teams.
    
    This is a complex placement algorithm. It prioritizes:
    1.  Splitting N players into teams of 4 or 5 (preferring 5s).
    2.  Using Mentors, then strongest players, as "anchors" for each team.
    3.  Placing mentees (users who requested a mentor) onto Mentor-led teams.
    4.  Distributing "New" and "Learner" players evenly, one per team if possible.
    5.  Filling remaining spots with proficient raiders.
    6.  Applying hard constraints (e.g., no 5-man teams with a "New" player).
    """
    
    # ---------- Sort and segment ----------
    # Sort all raiders by proficiency, then scythe, then KC
    available_raiders.sort(
        key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0)))
    )

    mentors = [p for p in available_raiders if normalize_role(p) == "mentor"]
    non_mentors = [p for p in available_raiders if normalize_role(p) != "mentor"]

    # Separate non-mentors into pools
    strong_pool = [p for p in non_mentors if prof_rank(p) <= PROF_ORDER["proficient"]]   # HP/Pro
    learners    = [p for p in non_mentors if normalize_role(p) == "learner"]
    news        = [p for p in non_mentors if normalize_role(p) == "new"]

    # Pull out mentees from all non-mentor pools
    mentees = [p for p in non_mentors if p.get("wants_mentor")]
    mentee_ids = {m["user_id"] for m in mentees}
    def _without_mentees(pool): return [p for p in pool if p["user_id"] not in mentee_ids]
    strong_pool = _without_mentees(strong_pool)
    learners    = _without_mentees(learners)
    news        = _without_mentees(news)

    # ---------- Decide target team sizes (only 4/5; 3 only for N in {6,7,11}) ----------
    N = len(available_raiders)
    if N == 0:
        return [], []

    def split_into_4_5(n: int):
        # Finds non-negative integers a, b such that 4a + 5b = n
        # Prioritizes maximizing 'b' (number of 5-person teams)
        for b in range(n // 5, -1, -1):
            rem = n - 5*b
            if rem % 4 == 0:
                a = rem // 4
                return [5]*b + [4]*a  # prefer 5s first
        return None # No solution (e.g., N=1, 2, 3, 7, 11)

    sizes = split_into_4_5(N)
    if sizes is None:
        # Handle special cases not solvable by 4s and 5s
        if N == 6:
            sizes = [3,3]
        elif N == 7:
            sizes = [4,3]
        elif N == 11:
            sizes = [4,4,3]
        else:
            # Fallback guard (shouldn't trigger for N>=12)
            q, r = divmod(N, 4)
            sizes = [4]*q + ([3] if r == 3 else ([] if r == 0 else [4])) # Should only be for N<12

    T = len(sizes) # Total number of teams

    # ---------- Build anchors (Mentors first, then strongest HP/Pro) ----------
    anchors: List[Dict[str, Any]] = []
    if len(mentors) >= T:
        # More mentors than teams; use the first T as anchors
        anchors = mentors[:T]
        extra_mentors = mentors[T:]
    else:
        # Fewer mentors than teams; use all mentors, then fill with strong players
        anchors = mentors[:] + strong_pool[: (T - len(mentors))]
        strong_pool = strong_pool[(T - len(mentors)):] # Remaining strong players
        extra_mentors = []

    # If still short (edge case, very few players), backfill from any pool
    for pool in (strong_pool, learners, news, mentees):
        while len(anchors) < T and pool:
            anchors.append(pool.pop(0))

    teams: List[List[Dict[str, Any]]] = [[a] for a in anchors] # Initialize teams with their anchors

    # ---------- Helper for safe placement ----------
    def can_add(player, team, max_size) -> bool:
        """Checks if a player can be added to a team based on constraints."""
        if len(team) >= max_size:
            return False # Team is full
        
        future_size = len(team) + 1

        # Hard constraints for 3-man teams (must all be proficient+)
        if max_size == 3:
            if not is_proficient_plus(player):
                return False # Player not strong enough
            if not all(is_proficient_plus(p) for p in team):
                return False # Team not strong enough
        
        # Two "Learn Freeze" players cannot be on the same team
        if player.get('learning_freeze') and any(p.get('learning_freeze') for p in team):
            return False
            
        # No 5-man teams with a "New" player (too hard)
        if normalize_role(player) == 'new' and future_size == 5:
            return False

        return True

    max_sizes = list(sizes) # [5, 4, 4]

    # ---------- Place mentees onto Mentor teams first ----------
    mentor_idxs = [i for i, t in enumerate(teams) if normalize_role(t[0]) == "mentor"]
    mentees.sort(key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0))))
    if mentor_idxs and mentees:
        forward = True # Zig-zag placement
        while mentees:
            placed = False
            idxs = mentor_idxs if forward else mentor_idxs[::-1]
            forward = not forward
            for i in idxs:
                if not mentees:
                    break
                if can_add(mentees[0], teams[i], max_sizes[i]):
                    teams[i].append(mentees.pop(0))
                    placed = True
            if not placed:
                break # No mentor teams have space

    # ---------- One-pass seeding (Distribute learners/newbs evenly) ----------
    # First pass: try to give each team one learner/newb
    for i in range(T):
        if news and can_add(news[0], teams[i], max_sizes[i]):
            teams[i].append(news.pop(0))
        elif learners and can_add(learners[0], teams[i], max_sizes[i]):
            teams[i].append(learners.pop(0))
    # Second pass: fill with strong players
    for i in range(T):
        if strong_pool and can_add(strong_pool[0], teams[i], max_sizes[i]):
            teams[i].append(strong_pool.pop(0))

    # ---------- Distribute leftovers ----------
    # Combine all remaining players into one pool
    leftovers = strong_pool + learners + news + mentees + extra_mentors
    forward = True
    safety = 0 # Failsafe
    while leftovers and safety < 10000:
        safety += 1
        placed_any = False
        idxs = list(range(T)) if forward else list(range(T-1, -1, -1))
        forward = not forward
        
        # Try to place the next leftover player
        for i in idxs:
            if not leftovers:
                break
            if can_add(leftovers[0], teams[i], max_sizes[i]):
                teams[i].append(leftovers.pop(0))
                placed_any = True
        
        if not placed_any:
            # This happens if the next player in `leftovers` can't fit anywhere
            # (e.g., a "New" player, and all teams are full or are 4-mans)
            # Try to "borrow" a strong player from a full team to fill a 3-man
            need_idxs = [i for i in range(T) if max_sizes[i] == 3 and len(teams[i]) < 3]
            borrowed = False
            for ti in need_idxs:
                for dj in range(T): # Find a donor team
                    if dj == ti: continue
                    donor_min_keep = 5 if max_sizes[dj] == 5 else (4 if max_sizes[dj] == 4 else 3)
                    if len(teams[dj]) <= donor_min_keep: continue # Can't borrow
                    
                    # Find a proficient player on the donor team
                    donor = next((p for p in teams[dj] if is_proficient_plus(p)), None)
                    if donor and can_add(donor, teams[ti], max_sizes[ti]):
                        teams[ti].append(donor)
                        teams[dj].remove(donor)
                        borrowed = True
                        placed_any = True
                        break
                if borrowed: break
            
            if not placed_any:
                # Cycle the player to the back of the queue
                leftovers.append(leftovers.pop(0))
    
    # Any players still in `leftovers` are stranded
    return teams, leftovers

def format_player_line_plain(guild: discord.Guild, p: dict) -> str:
    """Formats a player's info for the no-ping /sangmatchtest command."""
    nickname = p.get("user_name") or "Unknown"
    role_text = p.get("proficiency", "Unknown").replace(" ", "-").capitalize().replace("-", " ")
    kc_raw = p.get("kc", 0)
    kc_text = f"({kc_raw} KC)" if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" and kc_raw != 9999 else ""
    scythe = scythe_icon(p)
    freeze = freeze_icon(p)
    return f"{nickname} • **{role_text}** {kc_text} • {scythe} Scythe {freeze}"

# ---------------------------
# 🔹 UI Modals & Views
# ---------------------------

class UserSignupForm(Modal, title="Sanguine Sunday Signup"):
    """Modal form for regular raiders to sign up."""
    roles_known = TextInput(label="Favorite Roles (Leave blank if None)", placeholder="Inputs: All, Nfrz, Sfrz, Mdps, Rdps", style=discord.TextStyle.short, max_length=4, required=False)
    kc = TextInput(label="What is your N͟o͟r͟m͟a͟l͟ Mode ToB KC?", placeholder="0-10 = New, 11-25 = Learner, 26-100 = Proficient, 100+ = Highly Proficient", style=discord.TextStyle.short, max_length=5, required=True)
    has_scythe = TextInput(label="Do you have a Scythe? (Yes/No)", placeholder="Yes or No O͟N͟L͟Y͟", style=discord.TextStyle.short, max_length=3, required=True)
    learning_freeze = TextInput(label="Learn freeze role? (Yes or leave blank)", placeholder="Yes or blank/No O͟N͟L͟Y͟", style=discord.TextStyle.short, max_length=3, required=False)

    def __init__(self, cog: 'SanguineCog', previous_data: dict = None):
        super().__init__(title="Sanguine Sunday Signup")
        self.cog = cog # Store the cog instance to access sheets
        if previous_data:
            # Pre-fill the form with data from the History sheet
            self.roles_known.default = previous_data.get("Favorite Roles", "")
            kc_val = previous_data.get("KC", "")
            self.kc.default = str(kc_val) if kc_val not in ["", None, "X"] else ""
            self.has_scythe.default = "Yes" if previous_data.get("Has_Scythe", False) else "No"
            self.learning_freeze.default = "Yes" if previous_data.get("Learning Freeze", False) else ""

    async def on_submit(self, interaction: discord.Interaction):
        # Use self.cog.sang_sheet and self.cog.history_sheet
        if not self.cog.sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Sunday signup sheet is not connected. Please contact staff.", ephemeral=True)
            return
        
        # --- Validation ---
        try:
            kc_value = int(str(self.kc))
            if kc_value < 0: raise ValueError("KC cannot be negative.")
        except ValueError:
            await interaction.response.send_message("⚠️ Error: Kill Count must be a valid number.", ephemeral=True)
            return
        
        scythe_value = str(self.has_scythe).strip().lower()
        if scythe_value not in ["yes", "no", "y", "n"]:
            await interaction.response.send_message("⚠️ Error: Scythe must be 'Yes' or 'No'.", ephemeral=True)
            return
        has_scythe_bool = scythe_value in ["yes", "y"]

        # --- Data Processing ---
        proficiency_value = ""
        if kc_value <= 10: proficiency_value = "New"
        elif 11 <= kc_value <= 25: proficiency_value = "Learner"
        elif 26 <= kc_value <= 100: proficiency_value = "Proficient"
        else: proficiency_value = "Highly Proficient"

        roles_known_value = str(self.roles_known).strip() or "None"
        learning_freeze_value = str(self.learning_freeze).strip().lower()
        learning_freeze_bool = learning_freeze_value in ["yes", "y"]

        user_id = str(interaction.user.id)
        user_name = sanitize_nickname(interaction.user.display_name)
        timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
        
        # Prepare the row based on the sheet header order
        row_data = [user_id, user_name, roles_known_value, kc_value, has_scythe_bool, proficiency_value, learning_freeze_bool, False, timestamp]
        
        try:
            # --- Write to SangSignups sheet ---
            cell = self.cog.sang_sheet.find(user_id, in_column=1)
            if cell is None:
                self.cog.sang_sheet.append_row(row_data) # New signup
            else:
                self.cog.sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:I{cell.row}') # Update existing

            # --- Write to History sheet ---
            if self.cog.history_sheet:
                try:
                    history_cell = self.cog.history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        self.cog.history_sheet.append_row(row_data)
                    else:
                        self.cog.history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}')
                except Exception as e:
                    print(f"🔥 GSpread error on HISTORY (User Form) write: {e}")
            else:
                print("🔥 History sheet not available, skipping history append.")

        except gspread.CellNotFound:
             # Fallback if find() fails but user isn't there
             self.cog.sang_sheet.append_row(row_data)
             if self.cog.history_sheet:
                try:
                    history_cell = self.cog.history_sheet.find(user_id, in_column=1)
                    if history_cell is None: self.cog.history_sheet.append_row(row_data)
                    else: self.cog.history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}')
                except Exception as e: print(f"🔥 GSpread error on HISTORY (User Form) write: {e}")
             else: print("🔥 History sheet not available, skipping history append.")
        except Exception as e:
            print(f"🔥 GSpread error on signup: {e}")
            await interaction.response.send_message("⚠️ An error occurred while saving your signup.", ephemeral=True)
            return

        # --- Success Message ---
        await interaction.response.send_message(
            f"✅ **You are signed up as {proficiency_value}!**\n"
            f"**KC:** {kc_value}\n"
            f"**Scythe:** {'Yes' if has_scythe_bool else 'No'}\n"
            f"**Favorite Roles:** {roles_known_value}\n"
            f"**Learn Freeze:** {'Yes' if learning_freeze_bool else 'No'}",
            ephemeral=True
        )

class MentorSignupForm(Modal, title="Sanguine Sunday Mentor Signup"):
    """Modal form for Mentors to sign up."""
    roles_known = TextInput(label="Favorite Roles (Leave blank if None)", placeholder="Inputs: All, Nfrz, Sfrz, Mdps, Rdps", style=discord.TextStyle.short, max_length=4, required=True)
    kc = TextInput(label="What is your N͟o͟r͟m͟a͟l͟ Mode ToB KC?", placeholder="150+", style=discord.TextStyle.short, max_length=5, required=True)
    has_scythe = TextInput(label="Do you have a Scythe? (Yes/No)", placeholder="Yes or No", style=discord.TextStyle.short, max_length=3, required=True)

    def __init__(self, cog: 'SanguineCog', previous_data: dict = None):
        super().__init__(title="Sanguine Sunday Mentor Signup")
        self.cog = cog # Store the cog instance
        if previous_data:
             # Pre-fill the form with data from the History sheet
             self.roles_known.default = previous_data.get("Favorite Roles", "")
             kc_val = previous_data.get("KC", "")
             self.kc.default = str(kc_val) if kc_val not in ["", None, "X"] else ""
             self.has_scythe.default = "Yes" if previous_data.get("Has_Scythe", False) else "No"

    async def on_submit(self, interaction: discord.Interaction):
        # Use self.cog.sang_sheet and self.cog.history_sheet
        if not self.cog.sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Sunday signup sheet is not connected.", ephemeral=True)
            return
        
        # --- Validation ---
        try:
            kc_value = int(str(self.kc))
            if kc_value < 50:
                await interaction.response.send_message("⚠️ Mentors should have 50+ KC to sign up via form.", ephemeral=True)
                return
        except ValueError:
            await interaction.response.send_message("⚠️ Error: Kill Count must be a valid number.", ephemeral=True)
            return
        
        scythe_value = str(self.has_scythe).strip().lower()
        if scythe_value not in ["yes", "no", "y", "n"]:
            await interaction.response.send_message("⚠️ Error: Scythe must be 'Yes' or 'No'.", ephemeral=True)
            return
        has_scythe_bool = scythe_value in ["yes", "y"]

        # --- Data Processing ---
        proficiency_value = "Mentor"
        roles_known_value = str(self.roles_known).strip()
        learning_freeze_bool = False # Mentors don't learn freeze

        user_id = str(interaction.user.id)
        user_name = sanitize_nickname(interaction.user.display_name)
        timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
        
        # Prepare the row based on the sheet header order
        row_data = [user_id, user_name, roles_known_value, kc_value, has_scythe_bool, proficiency_value, learning_freeze_bool, False, timestamp]
        
        try:
            # --- Write to SangSignups sheet ---
            cell = self.cog.sang_sheet.find(user_id, in_column=1)
            if cell is None:
                self.cog.sang_sheet.append_row(row_data) # New signup
            else:
                self.cog.sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:I{cell.row}') # Update existing

            # --- Write to History sheet ---
            if self.cog.history_sheet:
                try:
                    history_cell = self.cog.history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        self.cog.history_sheet.append_row(row_data)
                    else:
                        self.cog.history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}')
                except Exception as e:
                    print(f"🔥 GSpread error on HISTORY (Mentor Form) write: {e}")
            else:
                print("🔥 History sheet not available, skipping history append.")
        except gspread.CellNotFound:
             # Fallback if find() fails but user isn't there
            self.cog.sang_sheet.append_row(row_data)
            if self.cog.history_sheet:
                try:
                    history_cell = self.cog.history_sheet.find(user_id, in_column=1)
                    if history_cell is None: self.cog.history_sheet.append_row(row_data)
                    else: self.cog.history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}')
                except Exception as e: print(f"🔥 GSpread error on HISTORY (Mentor Form) write: {e}")
            else: print("🔥 History sheet not available, skipping history append.")
        except Exception as e:
            print(f"🔥 GSpread error on mentor signup: {e}")
            await interaction.response.send_message("⚠️ An error occurred while saving your signup.", ephemeral=True)
            return

        # --- Success Message ---
        await interaction.response.send_message(
            f"✅ **You are signed up as a Mentor!**\n"
            f"**KC:** {kc_value}\n"
            f"**Scythe:** {'Yes' if has_scythe_bool else 'No'}\n"
            f"**Favorite Roles:** {roles_known_value}",
            ephemeral=True
        )

class WithdrawalButton(ui.Button):
    """A simple button to withdraw from the event."""
    def __init__(self, cog: 'SanguineCog'):
        super().__init__(label="Withdraw", style=ButtonStyle.secondary, custom_id="sang_withdraw", emoji="❌")
        self.cog = cog # Store the cog instance

    async def callback(self, interaction: discord.Interaction):
        user_id = str(interaction.user.id)
        user_name = interaction.user.display_name

        if not self.cog.sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Signup sheet is not connected.", ephemeral=True)
            return

        try:
            # Use self.cog.sang_sheet
            cell = self.cog.sang_sheet.find(user_id, in_column=1)
            if cell is None:
                await interaction.response.send_message(f"ℹ️ {user_name}, you are not currently signed up for this week's event.", ephemeral=True)
                return
            
            self.cog.sang_sheet.delete_rows(cell.row)
            await interaction.response.send_message(f"✅ **{user_name}**, you have been successfully withdrawn from this week's Sanguine Sunday signups.", ephemeral=True)
            print(f"✅ User {user_id} ({user_name}) withdrew from SangSignups.")
        except Exception as e:
            print(f"🔥 GSpread error on withdrawal: {e}")
            await interaction.response.send_message("⚠️ An error occurred while processing your withdrawal.", ephemeral=True)

class SignupView(View):
    """The persistent view with the 3 main buttons: Raider, Mentor, Withdraw."""
    def __init__(self, cog: 'SanguineCog'):
        super().__init__(timeout=None)
        self.cog = cog # Store the cog instance
        self.add_item(WithdrawalButton(self.cog))

    @ui.button(label="Sign Up as Raider", style=ButtonStyle.success, custom_id="sang_signup_raider", emoji="📝")
    async def user_signup_button(self, interaction: discord.Interaction, button: Button):
        # Call the cog's method to get previous data
        previous_data = self.cog.get_previous_signup(str(interaction.user.id))
        # Pass the cog instance to the modal
        await interaction.response.send_modal(UserSignupForm(self.cog, previous_data=previous_data))

    @ui.button(label="Sign Up as Mentor", style=ButtonStyle.danger, custom_id="sang_signup_mentor", emoji="🎓")
    async def mentor_signup_button(self, interaction: discord.Interaction, button: Button):
        user = interaction.user
        member = interaction.guild.get_member(user.id)
        if not member:
             await interaction.response.send_message("⚠️ Could not verify your roles. Please try again.", ephemeral=True)
             return

        has_mentor_role = any(role.id == MENTOR_ROLE_ID for role in member.roles)
        previous_data = self.cog.get_previous_signup(str(user.id))

        if not has_mentor_role:
            # User does NOT have @Mentor role, send them the Mentor form
            await interaction.response.send_modal(MentorSignupForm(self.cog, previous_data=previous_data))
            return
        
        # User HAS @Mentor role, check for auto-signup
        is_auto_signup = previous_data and previous_data.get("KC") == "X"

        if not is_auto_signup:
            # This is a Mentor's first-time click, or they've filled out the form before.
            # Auto-sign them up with default values.
            await interaction.response.defer(ephemeral=True)
            if not self.cog.sang_sheet or not self.cog.history_sheet:
                await interaction.followup.send("⚠️ Error: The Sanguine Sunday signup or history sheet is not connected.", ephemeral=True)
                return

            user_id = str(user.id)
            user_name = member.display_name
            timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
            # Default values for auto-signup
            row_data = [user_id, user_name, "All", "X", True, "Mentor", False, False, timestamp]

            try:
                cell = self.cog.sang_sheet.find(user_id, in_column=1)
                if cell is None: self.cog.sang_sheet.append_row(row_data)
                else: self.cog.sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:I{cell.row}')

                if self.cog.history_sheet:
                    try:
                        history_cell = self.cog.history_sheet.find(user_id, in_column=1)
                        if history_cell is None: self.cog.history_sheet.append_row(row_data)
                        else: self.cog.history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}')
                    except Exception as e: print(f"🔥 GSpread error on HISTORY (Auto-Mentor) write: {e}")
                else: print("🔥 History sheet not available, skipping history append.")
                
                await interaction.followup.send("✅ **Auto-signed up as Mentor!**\nTo edit your KC/Scythe/Roles, click the 'Mentor' button again.", ephemeral=True)
            except gspread.CellNotFound:
                 self.cog.sang_sheet.append_row(row_data)
                 if self.cog.history_sheet:
                    try:
                        history_cell = self.cog.history_sheet.find(user_id, in_column=1)
                        if history_cell is None: self.cog.history_sheet.append_row(row_data)
                        else: self.cog.history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}')
                    except Exception as e: print(f"🔥 GSpread error on HISTORY (Auto-Mentor) write: {e}")
                 else: print("🔥 History sheet not available, skipping history append.")
                 await interaction.followup.send("✅ **Auto-signed up as Mentor!**\nTo edit your KC/Scythe/Roles, click the 'Mentor' button again.", ephemeral=True)
            except Exception as e:
                print(f"🔥 GSpread error on auto mentor signup: {e}")
                await interaction.followup.send("⚠️ An error occurred while auto-signing you up.", ephemeral=True)
        else:
            # Mentor has already auto-signed up (KC="X").
            # Send them the form to let them edit their info.
            previous_data["KC"] = "" # Clear the "X"
            await interaction.response.send_modal(MentorSignupForm(self.cog, previous_data=previous_data))


# ---------------------------
# 🔹 Sanguine Cog Class
# ---------------------------

class SanguineCog(commands.Cog):
    """
    This class holds all the commands, listeners, and tasks
    for the Sanguine Sunday event.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sang_sheet = None
        self.history_sheet = None
        
        # Initialize Google Sheets
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            credentials_dict = {
                "type": os.getenv('GOOGLE_TYPE'),
                "project_id": os.getenv('GOOGLE_PROJECT_ID'),
                "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID'),
                "private_key": os.getenv('GOOGLE_PRIVATE_KEY').replace("\\n", "\n"),
                "client_email": os.getenv('GOOGLE_CLIENT_EMAIL'),
                "client_id": os.getenv('GOOGLE_CLIENT_ID'),
                "auth_uri": os.getenv('GOOGLE_AUTH_URI'),
                "token_uri": os.getenv('GOOGLE_TOKEN_URI'),
                "auth_provider_x509_cert_url": os.getenv('GOOGLE_AUTH_PROVIDER_X509_CERT_URL'),
                "client_x509_cert_url": os.getenv('GOOGLE_CLIENT_X509_CERT_URL'),
                "universe_domain": os.getenv('GOOGLE_UNIVERSE_DOMAIN'),
            }
            creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
            sheet_client = gspread.authorize(creds)
            
            sang_google_sheet = sheet_client.open_by_key(SANG_SHEET_ID)
            
            try:
                self.sang_sheet = sang_google_sheet.worksheet(SANG_SHEET_TAB_NAME)
                # Check header
                header = self.sang_sheet.row_values(1)
                if header != SANG_SHEET_HEADER:
                    print("⚠️ Sanguine sheet header mismatch. Re-writing...")
                    self.sang_sheet.clear()
                    self.sang_sheet.append_row(SANG_SHEET_HEADER)
            except gspread.exceptions.WorksheetNotFound:
                print(f"'{SANG_SHEET_TAB_NAME}' not found. Creating...")
                self.sang_sheet = sang_google_sheet.add_worksheet(title=SANG_SHEET_TAB_NAME, rows="100", cols="20")
                self.sang_sheet.append_row(SANG_SHEET_HEADER)
            
            try:
                self.history_sheet = sang_google_sheet.worksheet(SANG_HISTORY_TAB_NAME)
                # Check header
                header = self.history_sheet.row_values(1)
                if header != SANG_SHEET_HEADER:
                    print("⚠️ Sanguine history sheet header mismatch. Re-writing...")
                    self.history_sheet.clear()
                    self.history_sheet.append_row(SANG_SHEET_HEADER)
            except gspread.exceptions.WorksheetNotFound:
                print(f"'{SANG_HISTORY_TAB_NAME}' not found. Creating...")
                self.history_sheet = sang_google_sheet.add_worksheet(title=SANG_HISTORY_TAB_NAME, rows="1000", cols="20")
                self.history_sheet.append_row(SANG_SHEET_HEADER)
            
            print("✅ Sanguine Cog: Google Sheets initialized successfully.")
        except Exception as e:
            print(f"🔥 CRITICAL ERROR initializing SanguineCog GSheets: {e}")
            # Bot will continue, but commands will fail with checks

        # Add the persistent view
        # We pass `self` (the cog instance) to the view
        self.bot.add_view(SignupView(self))

    @commands.Cog.listener()
    async def on_ready(self):
        """Called when the cog is loaded and the bot is ready."""
        # Start the tasks
        if not self.scheduled_post_signup.is_running():
            self.scheduled_post_signup.start()
            print("✅ Sanguine Cog: Started scheduled signup task.")
        if not self.scheduled_post_reminder.is_running():
            self.scheduled_post_reminder.start()
            print("✅ Sanguine Cog: Started scheduled reminder task.")
        if not self.scheduled_clear_sang_sheet.is_running():
            self.scheduled_clear_sang_sheet.start()
            print("✅ Sanguine Cog: Started scheduled sheet clear task.")
        
        print("Sanguine Cog is ready.")

    # --- Cog Methods (from helper functions) ---

    def get_previous_signup(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Fetches the latest signup data for a user from the HISTORY sheet."""
        if not self.history_sheet:
            print("History sheet not available in get_previous_signup.")
            return None
        try:
            all_records = self.history_sheet.get_all_records()
            if not all_records:
                 print("No records found in history_sheet.")
                 return None
            
            # Iterate in reverse to find the *last* entry for this user
            for record in reversed(all_records):
                sheet_discord_id = record.get("Discord_ID")
                sheet_discord_id_str = str(sheet_discord_id) if sheet_discord_id is not None else None
                
                if sheet_discord_id_str == user_id:
                    # Convert GSheet "TRUE"/"FALSE" strings to real booleans
                    record["Has_Scythe"] = str(record.get("Has_Scythe", "FALSE")).upper() == "TRUE"
                    record["Learning Freeze"] = str(record.get("Learning Freeze", "FALSE")).upper() == "TRUE"
                    return record
            
            print(f"No history match found for user_id: {user_id}")
            return None
        except Exception as e:
            print(f"🔥 GSpread error fetching previous signup for {user_id}: {e}")
            return None

    async def post_signup(self, channel: discord.TextChannel):
        """Posts the main signup message with the signup buttons."""
        await channel.send(SANG_MESSAGE, view=SignupView(self))
        print(f"✅ Posted Sanguine Sunday signup in #{channel.name}")

    async def post_reminder(self, channel: discord.TextChannel):
        """Finds learners and posts a reminder."""
        if not self.sang_sheet:
            print("⚠️ Cannot post reminder, Sang Sheet not connected.")
            return False
        
        # Delete previous reminder messages
        try:
            async for message in channel.history(limit=50):
                if message.author == self.bot.user and LEARNER_REMINDER_IDENTIFIER in message.content:
                    await message.delete()
        except discord.Forbidden:
            print(f"⚠️ Could not delete old reminders in #{channel.name}")
        except Exception as e:
            print(f"🔥 Error cleaning up reminders: {e}")

        learners = []
        try:
            all_signups = self.sang_sheet.get_all_records()
            for signup in all_signups:
                proficiency = str(signup.get("Proficiency", "")).lower()
                if proficiency in ["learner", "new"]:
                    user_id = signup.get('Discord_ID')
                    if user_id:
                        learners.append(f"<@{user_id}>")
            
            if not learners:
                reminder_content = f"{LEARNER_REMINDER_MESSAGE}\n\n_No learners have signed up yet._"
            else:
                learner_pings = " ".join(learners)
                reminder_content = f"{LEARNER_REMINDER_MESSAGE}\n\n**Learners:** {learner_pings}"

            await channel.send(reminder_content, allowed_mentions=discord.AllowedMentions(users=True))
            print(f"✅ Posted Sanguine Sunday learner reminder in #{channel.name}")
            return True
        except Exception as e:
            print(f"🔥 GSpread error fetching/posting reminder: {e}")
            await channel.send("⚠️ Error processing learner list from database.")
            return False

    # --- Slash Commands ---
    
    @app_commands.command(name="sangsignup", description="Manage Sanguine Sunday signups.")
    @app_commands.checks.has_role(STAFF_ROLE_ID)
    @app_commands.describe(variant="Choose the action to perform.", channel="Optional channel to post in (defaults to the configured event channel).")
    @app_commands.choices(variant=[
        app_commands.Choice(name="Post Signup Message", value=1),
        app_commands.Choice(name="Post Learner Reminder", value=2),
    ])
    async def sangsignup(self, interaction: discord.Interaction, variant: int, channel: Optional[discord.TextChannel] = None):
        target_channel = channel or self.bot.get_channel(SANG_CHANNEL_ID)
        if not target_channel:
            await interaction.response.send_message("⚠️ Could not find the target channel.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if variant == 1:
            await self.post_signup(target_channel)
            await interaction.followup.send(f"✅ Signup message posted in {target_channel.mention}.")
        elif variant == 2:
            result = await self.post_reminder(target_channel)
            if result:
                await interaction.followup.send(f"✅ Learner reminder posted in {target_channel.mention}.")
            else:
                await interaction.followup.send("⚠️ Could not post the reminder.")

    @app_commands.command(name="sangmatch", description="Create ToB teams from signups in a voice channel.")
    @app_commands.checks.has_role(STAFF_ROLE_ID)
    @app_commands.describe(voice_channel="Optional: The voice channel to pull users from. If omitted, uses all signups.")
    async def sangmatch(self, interaction: discord.Interaction, voice_channel: Optional[discord.VoiceChannel] = None):
        if not self.sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Sunday sheet is not connected.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)
        
        vc_member_ids = None 
        channel_name = "All Signups" 
        if voice_channel: 
            channel_name = voice_channel.name
            if not voice_channel.members:
                await interaction.followup.send(f"⚠️ No users are in {voice_channel.mention}.")
                return
            vc_member_ids = {str(member.id) for member in voice_channel.members if not member.bot}
            if not vc_member_ids:
                await interaction.followup.send(f"⚠️ No human users are in {voice_channel.mention}.")
                return
        
        try:
            all_signups_records = self.sang_sheet.get_all_records()
            if not all_signups_records:
                await interaction.followup.send("⚠️ There are no signups in the database.")
                return
        except Exception as e:
            print(f"🔥 GSheet error fetching all signups: {e}")
            await interaction.followup.send("⚠️ An error occurred fetching signups from the database.")
            return

        available_raiders = []
        for signup in all_signups_records:
            user_id = str(signup.get("Discord_ID"))
            if vc_member_ids and user_id not in vc_member_ids:
                 continue # User is not in the specified VC
            
            roles_str = signup.get("Favorite Roles", "")
            knows_range, knows_melee = parse_roles(roles_str)
            kc_raw = signup.get("KC", 0)
            
            try:
                kc_val = int(kc_raw)
            except (ValueError, TypeError):
                # Handle "X" KC for mentors or bad data
                kc_val = 9999 if signup.get("Proficiency", "").lower() == 'mentor' else 0
            
            proficiency_val = signup.get("Proficiency", "").lower()
            
            # Re-calculate proficiency based on KC, overriding sheet value
            # (unless they are a mentor)
            if proficiency_val != 'mentor':
                if kc_val <= 10: proficiency_val = "new"
                elif 11 <= kc_val <= 25: proficiency_val = "learner"
                elif 26 <= kc_val <= 100: proficiency_val = "proficient"
                else: proficiency_val = "highly proficient"

            available_raiders.append({
                "user_id": user_id,
                "user_name": sanitize_nickname(signup.get("Discord_Name")),
                "proficiency": proficiency_val,
                "kc": kc_val,
                "has_scythe": str(signup.get("Has_Scythe", "FALSE")).upper() == "TRUE",
                "roles_known": roles_str,
                "learning_freeze": str(signup.get("Learning Freeze", "FALSE")).upper() == "TRUE",
                "knows_range": knows_range,
                "knows_melee": knows_melee,
                "wants_mentor": str(signup.get("Mentor_Request", "FALSE")).upper() == "TRUE"
            })

        if not available_raiders:
            await interaction.followup.send(f"⚠️ None of the users in {voice_channel.mention} have signed up for the event." if voice_channel else "⚠️ No eligible signups.")
            return

        teams, stranded_players = matchmaking_algorithm(available_raiders)
        
        guild = interaction.guild
        category = guild.get_channel(SANG_VC_CATEGORY_ID)
        
        # Create Voice Channels
        if category and hasattr(category, "create_voice_channel"):
            for i in range(len(teams)):
                try:
                    await category.create_voice_channel(name=f"SanguineSunday – Team {i+1}", user_limit=5)
                except Exception as e:
                    print(f"Error creating VC: {e}") 

        post_channel = interaction.channel
        embed = discord.Embed(title=f"Sanguine Sunday Teams - {channel_name}", description=f"Created {len(teams)} valid team(s) from {len(available_raiders)} available signed-up users.", color=discord.Color.red())
        if not teams:
            embed.description = "Could not form any valid teams with the available players."

        for i, team in enumerate(teams, start=1):
            team_sorted = sorted(team, key=prof_rank)
            team_details = []
            for p in team_sorted:
                try:
                    uid = int(p["user_id"])
                    member = guild.get_member(uid)
                    mention = member.mention if member else f"<@{uid}>"
                except Exception:
                    mention = f"@{p.get('user_name', 'Unknown')}"
                
                role_text = p.get("proficiency", "Unknown").replace(" ", "-").capitalize().replace("-", " ")
                kc_raw = p.get("kc", 0)
                kc_text = f"({kc_raw} KC)" if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" and kc_raw != 9999 else ""
                scythe = scythe_icon(p)
                freeze = freeze_icon(p)
                team_details.append(f"{mention} • **{role_text}** {kc_text} • {scythe} Scythe {freeze}")
            
            embed.add_field(name=f"Team {i} (Size: {len(team)})", value="\n".join(team_details) if team_details else "—", inline=False)
        
        global last_generated_teams
        last_generated_teams = teams
        await interaction.followup.send(embed=embed)


    @app_commands.command(name="sangmatchtest", description="Create ToB teams without pinging or creating voice channels; show plain-text nicknames.")
    @app_commands.checks.has_role(STAFF_ROLE_ID)
    @app_commands.describe(voice_channel="Optional: The voice channel to pull users from. If omitted, uses all signups.", channel="(Optional) Override the text channel to post teams (testing).")
    async def sangmatchtest(self, interaction: discord.Interaction, voice_channel: Optional[discord.VoiceChannel] = None, channel: Optional[discord.TextChannel] = None):
        if not self.sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Sunday sheet is not connected.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)

        vc_member_ids = None
        channel_name = "All Signups"
        if voice_channel:
            channel_name = voice_channel.name
            if not voice_channel.members:
                await interaction.followup.send(f"⚠️ No users are in {voice_channel.mention}.")
                return
            vc_member_ids = {str(m.id) for m in voice_channel.members if not m.bot}
            if not vc_member_ids:
                await interaction.followup.send(f"⚠️ No human users are in {voice_channel.mention}.")
                return

        try:
            all_signups_records = self.sang_sheet.get_all_records()
        except Exception as e:
            await interaction.followup.send("⚠️ An error occurred fetching signups from the database.")
            return
        
        available_raiders = []
        for signup in all_signups_records:
            user_id = str(signup.get("Discord_ID"))
            if vc_member_ids and user_id not in vc_member_ids: continue
            
            roles_str = signup.get("Favorite Roles", "")
            knows_range, knows_melee = parse_roles(roles_str)
            kc_raw = signup.get("KC", 0)
            try: kc_val = int(kc_raw)
            except (ValueError, TypeError): kc_val = 9999 if signup.get("Proficiency", "").lower() == 'mentor' else 0
            
            proficiency_val = signup.get("Proficiency", "").lower()
            if proficiency_val != 'mentor':
                if kc_val <= 10: proficiency_val = "new"
                elif 11 <= kc_val <= 25: proficiency_val = "learner"
                elif 26 <= kc_val <= 100: proficiency_val = "proficient"
                else: proficiency_val = "highly proficient"

            available_raiders.append({
                "user_id": user_id, "user_name": sanitize_nickname(signup.get("Discord_Name")),
                "proficiency": proficiency_val, "kc": kc_val,
                "has_scythe": str(signup.get("Has_Scythe", "FALSE")).upper() == "TRUE",
                "roles_known": roles_str, "learning_freeze": str(signup.get("Learning Freeze", "FALSE")).upper() == "TRUE",
                "knows_range": knows_range, "knows_melee": knows_melee,
                "wants_mentor": str(signup.get("Mentor_Request", "FALSE")).upper() == "TRUE"
            })
        
        if not available_raiders:
            await interaction.followup.send("⚠️ No eligible signups.")
            return

        teams, stranded_players = matchmaking_algorithm(available_raiders)
        
        guild = interaction.guild
        post_channel = channel or interaction.channel
        embed = discord.Embed(title=f"Sanguine Sunday Teams (Test, no pings/VC) - {channel_name}", description=f"Created {len(teams)} valid team(s) from {len(available_raiders)} available signed-up users.", color=discord.Color.dark_gray())
        
        for i, team in enumerate(teams, start=1):
            team_sorted = sorted(team, key=prof_rank)
            lines = [format_player_line_plain(guild, p) for p in team_sorted]
            embed.add_field(name=f"Team {i} (Size: {len(team)})", value="\n".join(lines) if lines else "—", inline=False)
        
        global last_generated_teams
        last_generated_teams = teams
        
        if interaction.channel == post_channel:
             await interaction.followup.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        else:
             await post_channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
             await interaction.followup.send("✅ Posted no-ping test teams (no voice channels created).", ephemeral=True)


    @app_commands.command(name="sangexport", description="Export the most recently generated teams to a text file.")
    @app_commands.checks.has_any_role("Administrators", "Clan Staff", "Senior Staff", "Staff", "Trial Staff")
    async def sangexport(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        global last_generated_teams
        teams = last_generated_teams
        if not teams:
            await interaction.followup.send("⚠️ No teams found from this session.", ephemeral=True)
            return

        guild = interaction.guild
        lines = []
        for i, team in enumerate(teams, start=1):
            lines.append(f"Team {i}")
            for p in team:
                sname = sanitize_nickname(p.get("user_name", "Unknown"))
                mid = p.get("user_id")
                id_text = str(mid) if mid is not None else "UnknownID"
                lines.append(f"  - {sname} — ID: {id_text}")
            lines.append("")
        txt = "\n".join(lines)

        # Try to save to a mounted volume, fallback to /tmp
        export_dir = Path(os.getenv("SANG_EXPORT_DIR", "/mnt/data"))
        try:
            export_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            export_dir = Path("/tmp")
            export_dir.mkdir(parents=True, exist_ok=True)
        
        ts = datetime.now(CST).strftime("%Y%m%d_%H%M%S")
        outpath = export_dir / f"sanguine_teams_{ts}.txt"
        
        try:
            with open(outpath, "w", encoding="utf-8") as f:
                f.write(txt)
            
            preview = "\n".join(lines[:min(10, len(lines))])
            await interaction.followup.send(content=f"📄 Exported teams to **{outpath.name}**:\n```\n{preview}\n```", file=discord.File(str(outpath), filename=outpath.name), ephemeral=True)
        except Exception as e:
            print(f"🔥 Failed to write or send export file: {e}")
            await interaction.followup.send(f"⚠️ Failed to write export file: {e}", ephemeral=True)


    @app_commands.command(name="sangcleanup", description="Delete auto-created SanguineSunday voice channels from the last run.")
    @app_commands.checks.has_any_role("Administrators", "Clan Staff", "Senior Staff", "Staff", "Trial Staff")
    async def sangcleanup(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild
        category = guild.get_channel(SANG_VC_CATEGORY_ID)
        if not category:
            await interaction.followup.send("⚠️ Category not found.", ephemeral=True); return
        deleted = 0
        for ch in list(category.channels):
            try:
                if isinstance(ch, discord.VoiceChannel) and ch.name.startswith("SanguineSunday – Team "):
                    await ch.delete(reason="sangcleanup")
                    deleted += 1
            except Exception:
                pass # Ignore errors (e.g., channel deleted by someone else)
        await interaction.followup.send(f"🧹 Deleted {deleted} voice channels.", ephemeral=True)

    @sangsignup.error
    @sangmatch.error
    @sangmatchtest.error
    @sangexport.error
    @sangcleanup.error
    async def sang_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingRole):
            await interaction.response.send_message("❌ You don't have the required role for this command.", ephemeral=True)
        else:
            print(f"Error in a Sanguine command: {error}")
            if interaction.response.is_done():
                await interaction.followup.send(f"An unexpected error occurred. Please contact staff.", ephemeral=True)
            else:
                await interaction.response.send_message(f"An unexpected error occurred. Please contact staff.", ephemeral=True)

    # --- Scheduled Tasks ---

    @tasks.loop(time=dt_time(hour=11, minute=0, tzinfo=CST))
    async def scheduled_post_signup(self):
        if datetime.now(CST).weekday() == 4:  # 4 = Friday
            print("It's Friday at 11:00 AM CST. Posting Sanguine signup...")
            channel = self.bot.get_channel(SANG_CHANNEL_ID)
            if channel:
                await self.post_signup(channel)
            else:
                print(f"🔥 Failed to post signup: Channel {SANG_CHANNEL_ID} not found.")

    @tasks.loop(time=dt_time(hour=14, minute=0, tzinfo=CST))
    async def scheduled_post_reminder(self):
        if datetime.now(CST).weekday() == 5:  # 5 = Saturday
            print("It's Saturday at 2:00 PM CST. Posting Sanguine learner reminder...")
            channel = self.bot.get_channel(SANG_CHANNEL_ID)
            if channel:
                await self.post_reminder(channel)
            else:
                print(f"🔥 Failed to post reminder: Channel {SANG_CHANNEL_ID} not found.")

    @tasks.loop(time=dt_time(hour=4, minute=0, tzinfo=CST)) # 4 AM CST
    async def scheduled_clear_sang_sheet(self):
        if datetime.now(CST).weekday() == 0:  # 0 = Monday
            print("MONDAY DETECTED: Clearing SangSignups sheet...")
            if self.sang_sheet:
                try:
                    self.sang_sheet.clear()
                    self.sang_sheet.append_row(SANG_SHEET_HEADER)
                    print("✅ SangSignups sheet cleared and headers added.")
                except Exception as e:
                    print(f"🔥 Failed to clear SangSignups sheet: {e}")
            else:
                print("⚠️ Cannot clear SangSignups sheet, not connected.")

    @scheduled_post_signup.before_loop
    @scheduled_post_reminder.before_loop
    @scheduled_clear_sang_sheet.before_loop
    async def before_scheduled_tasks(self):
        await self.bot.wait_until_ready()

# This setup function is required for the bot to load the Cog
async def setup(bot: commands.Bot):
    await bot.add_cog(SanguineCog(bot))

