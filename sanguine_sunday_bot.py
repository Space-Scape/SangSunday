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
from zoneinfo import ZoneInfo
CST = ZoneInfo('America/Chicago')

# ---------------------------
# 🔹 Google Sheets Setup
# ---------------------------

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

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

# ---------------------------
# 🔹 Sang Sheet Setup
# ---------------------------
SANG_SHEET_ID = "1CCpDAJO7Cq581yF_-rz3vx7L_BTettVaKglSvOmvTOE" # <-- Specific ID for Sang Signups
SANG_SHEET_TAB_NAME = "SangSignups"
SANG_HISTORY_TAB_NAME = "History" # <-- ADDED

try:
    # Use the specific SANG_SHEET_ID and the main sheet_client
    sang_google_sheet = sheet_client.open_by_key(SANG_SHEET_ID) # <-- Get the spreadsheet
    
    # Try to get SangSignups sheet
    try:
        sang_sheet = sang_google_sheet.worksheet(SANG_SHEET_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"'{SANG_SHEET_TAB_NAME}' not found. Creating...")
        sang_sheet = sang_google_sheet.add_worksheet(title=SANG_SHEET_TAB_NAME, rows="100", cols="20")
        sang_sheet.append_row(["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Timestamp"])

    # Try to get History sheet
    try:
        history_sheet = sang_google_sheet.worksheet(SANG_HISTORY_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"'{SANG_HISTORY_TAB_NAME}' not found. Creating...")
        history_sheet = sang_google_sheet.add_worksheet(title=SANG_HISTORY_TAB_NAME, rows="1000", cols="20")
        history_sheet.append_row(["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Timestamp"])

except (PermissionError, gspread.exceptions.APIError) as e: # <-- Use fully qualified name
    # This block runs if the bot doesn't have permission to access the file at all.
    print(f"🔥 CRITICAL ERROR: Bot does not have permission for Sang Sheet (ID: {SANG_SHEET_ID}).")
    print(f"🔥 Please ensure the service account email ({os.getenv('GOOGLE_CLIENT_EMAIL')}) has 'Editor' permissions on this Google Sheet.")
    print(f"🔥 Error details: {e}")
    sang_sheet = None
    history_sheet = None # <-- ADDED
except Exception as e:
    print(f"Error initializing Sang Sheet: {e}")
    sang_sheet = None
    history_sheet = None # <-- ADDED


# ---------------------------
# 🔹 Discord Bot Setup
# ---------------------------

intents = discord.Intents.default()
intents.members = True
intents.message_content = True # Needed for on_message
intents.reactions = True # Needed for reaction tasks
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# ---------------------------
# 🔹 Main Configuration
# ---------------------------
GUILD_ID = 1272629330115297330

SANG_CHANNEL_ID = 1338295765759688767
STAFF_ROLE_ID = 1272635396991221824
MEMBER_ROLE_ID = 1272633036814946324
MENTOR_ROLE_ID = 1306021911830073414
SANG_ROLE_ID = 1387153629072592916
TOB_ROLE_ID = 1272694636921753701
SENIOR_STAFF_CHANNEL_ID = 1336473990302142484  # Channel for approval notifications.
ADMINISTRATOR_ROLE_ID = 1272961765034164318   # Role that can approve actions.
SENIOR_STAFF_ROLE_ID = 1336473488159936512    # Role that can approve actions.

# --------------------------------------------------
# 🔹 Sanguine Sunday Signup System (REFACTORED)
# --------------------------------------------------

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

class UserSignupForm(Modal, title="Sanguine Sunday Signup"):
    roles_known = TextInput(
        label="Favorite Roles (Leave blank if None)",
        placeholder="Inputs: All, Nfrz, Sfrz, Mdps, Rdps",
        style=discord.TextStyle.short,
        max_length=4,
        required=False
    )
    
    kc = TextInput(
        label="What is your N͟o͟r͟m͟a͟l͟ Mode ToB KC?",
        placeholder="0-10 = New, 11-25 = Learner, 26-100 = Proficient, 100+ = Highly Proficient",
        style=discord.TextStyle.short,
        max_length=5,
        required=True
    )

    has_scythe = TextInput(
        label="Do you have a Scythe? (Yes/No)",
        placeholder="Yes or No O͟N͟L͟Y͟", 
        style=discord.TextStyle.short,
        max_length=3,
        required=True
    )
    
    learning_freeze = TextInput(
        label="Learn freeze role? (Yes or leave blank)", # <-- Shortened this label
        placeholder="Yes or blank/No O͟N͟L͟Y͟",
        style=discord.TextStyle.short,
        max_length=3,
        required=False
    )

    def __init__(self, previous_data: dict = None):
        super().__init__(title="Sanguine Sunday Signup")
        if previous_data:
            self.roles_known.default = previous_data.get("Favorite Roles", "")
            kc_val = previous_data.get("KC", "")
            self.kc.default = str(kc_val) if kc_val not in ["", None, "X"] else ""
            self.has_scythe.default = "Yes" if previous_data.get("Has_Scythe", False) else "No"
            self.learning_freeze.default = "Yes" if previous_data.get("Learning Freeze", False) else ""

    async def on_submit(self, interaction: discord.Interaction):
        if not sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Sunday signup sheet is not connected. Please contact staff.", ephemeral=True)
            return

        try:
            kc_value = int(str(self.kc))
            if kc_value < 0:
                raise ValueError("KC cannot be negative.")
        except ValueError:
            await interaction.response.send_message("⚠️ Error: Kill Count must be a valid number.", ephemeral=True)
            return
            
        scythe_value = str(self.has_scythe).strip().lower()
        if scythe_value not in ["yes", "no", "y", "n"]:
            await interaction.response.send_message("⚠️ Error: Scythe must be 'Yes' or 'No'.", ephemeral=True)
            return
        has_scythe_bool = scythe_value in ["yes", "y"]

        proficiency_value = ""
        if kc_value <= 1:
            proficiency_value = "New"
        elif 1 < kc_value < 50:
            proficiency_value = "Learner"
        elif 50 <= kc_value < 150:
            proficiency_value = "Proficient"
        else:
            proficiency_value = "Highly Proficient"

        roles_known_value = str(self.roles_known).strip() or "None"
        learning_freeze_value = str(self.learning_freeze).strip().lower()
        learning_freeze_bool = learning_freeze_value in ["yes", "y"]

        user_id = str(interaction.user.id)
        user_name = sanitize_nickname(interaction.user.display_name)
        timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
        
        row_data = [
            user_id, user_name, roles_known_value, kc_value, 
            has_scythe_bool, proficiency_value, learning_freeze_bool, timestamp
        ]
        
        try:
            cell = sang_sheet.find(user_id, in_column=1)

            # --- UPDATED CHECK ---
            if cell is None:
                sang_sheet.append_row(row_data)
            else:
                sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:H{cell.row}') # <-- FIXED

            # --- MODIFIED HISTORY WRITE ---
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:H{history_cell.row}') # <-- FIXED
                except Exception as e:
                    print(f"🔥 GSpread error on HISTORY (User Form) write: {e}")
            else:
                print("🔥 History sheet not available, skipping history append.")
            # --- END MODIFIED ---

        except gspread.CellNotFound:
             sang_sheet.append_row(row_data)

             # --- MODIFIED HISTORY WRITE ---
             if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:H{history_cell.row}') # <-- FIXED
                except Exception as e:
                    print(f"🔥 GSpread error on HISTORY (User Form) write: {e}")
             else:
                 print("🔥 History sheet not available, skipping history append.")
             # --- END MODIFIED ---

        except Exception as e:
            print(f"🔥 GSpread error on signup: {e}")
            await interaction.response.send_message("⚠️ An error occurred while saving your signup.", ephemeral=True)
            return

        # --- Success message ---
        await interaction.response.send_message(
            f"✅ **You are signed up as {proficiency_value}!**\n"
            f"**KC:** {kc_value}\n"
            f"**Scythe:** {'Yes' if has_scythe_bool else 'No'}\n"
            f"**Favorite Roles:** {roles_known_value}\n"
            f"**Learn Freeze:** {'Yes' if learning_freeze_bool else 'No'}",
            ephemeral=True
        )

class MentorSignupForm(Modal, title="Sanguine Sunday Mentor Signup"):
    roles_known = TextInput(
        label="Favorite Roles (Leave blank if None)",
        placeholder="Inputs: All, Nfrz, Sfrz, Mdps, Rdps",
        style=discord.TextStyle.short,
        max_length=4,
        required=True
    )
    
    kc = TextInput(
        label="What is your N͟o͟r͟m͟a͟l͟ Mode ToB KC?",
        placeholder="150+",
        style=discord.TextStyle.short,
        max_length=5,
        required=True
    )

    has_scythe = TextInput(
        label="Do you have a Scythe? (Yes/No)",
        placeholder="Yes or No",
        style=discord.TextStyle.short,
        max_length=3,
        required=True
    )

    # --- Add __init__ for prefilling ---
    def __init__(self, previous_data: dict = None):
        super().__init__(title="Sanguine Sunday Mentor Signup")
        if previous_data:
             self.roles_known.default = previous_data.get("Favorite Roles", "")
             kc_val = previous_data.get("KC", "")
             self.kc.default = str(kc_val) if kc_val not in ["", None, "X"] else ""
             self.has_scythe.default = "Yes" if previous_data.get("Has_Scythe", False) else "No"

    async def on_submit(self, interaction: discord.Interaction):
        if not sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Sunday signup sheet is not connected.", ephemeral=True)
            return

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

        proficiency_value = "Mentor"
        roles_known_value = str(self.roles_known).strip()
        learning_freeze_bool = False

        user_id = str(interaction.user.id)
        user_name = sanitize_nickname(interaction.user.display_name)
        timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
        
        row_data = [
            user_id, user_name, roles_known_value, kc_value, 
            has_scythe_bool, proficiency_value, learning_freeze_bool, timestamp
        ]
        
        try:
            cell = sang_sheet.find(user_id, in_column=1)
            if cell is None:
                 sang_sheet.append_row(row_data)
            else:
                 sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:H{cell.row}') # <-- FIXED

            # --- MODIFIED HISTORY WRITE ---
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:H{history_cell.row}') # <-- FIXED
                except Exception as e:
                    print(f"🔥 GSpread error on HISTORY (Mentor Form) write: {e}")
            else:
                print("🔥 History sheet not available, skipping history append.")
            # --- END MODIFIED ---
        except gspread.CellNotFound:
            sang_sheet.append_row(row_data)

            # --- MODIFIED HISTORY WRITE ---
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:H{history_cell.row}') # <-- FIXED
                except Exception as e:
                    print(f"🔥 GSpread error on HISTORY (Mentor Form) write: {e}")
            else:
                print("🔥 History sheet not available, skipping history append.")
            # --- END MODIFIED ---
        except Exception as e:
            print(f"🔥 GSpread error on mentor signup: {e}")
            await interaction.response.send_message("⚠️ An error occurred while saving your signup.", ephemeral=True)
            return

        # --- Success message ---
        await interaction.response.send_message(
            f"✅ **You are signed up as a Mentor!**\n"
            f"**KC:** {kc_value}\n"
            f"**Scythe:** {'Yes' if has_scythe_bool else 'No'}\n"
            f"**Favorite Roles:** {roles_known_value}",
            ephemeral=True
        )
        
def get_previous_signup(user_id: str) -> Optional[Dict[str, Any]]:
    """Fetches the latest signup data for a user from the HISTORY sheet."""
    if not history_sheet: # <-- MODIFIED
        print("History sheet not available in get_previous_signup.") # <-- MODIFIED & DEBUG removed
        return None
    try:
        all_records = history_sheet.get_all_records() # <-- MODIFIED
        if not all_records:
             print("No records found in history_sheet.") # <-- MODIFIED & DEBUG removed
             return None

        for record in reversed(all_records):
            sheet_discord_id = record.get("Discord_ID")
            sheet_discord_id_str = str(sheet_discord_id) if sheet_discord_id is not None else None

            if sheet_discord_id_str == user_id:
                record["Has_Scythe"] = str(record.get("Has_Scythe", "FALSE")).upper() == "TRUE"
                record["Learning Freeze"] = str(record.get("Learning Freeze", "FALSE")).upper() == "TRUE"
                return record
        print(f"No history match found for user_id: {user_id}") # <-- MODIFIED & DEBUG removed
        return None
    except Exception as e:
        print(f"🔥 GSpread error fetching previous signup for {user_id}: {e}")
        return None

class SignupView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="Sign Up as Raider", style=ButtonStyle.success, custom_id="sang_signup_raider", emoji="📝")
    async def user_signup_button(self, interaction: discord.Interaction, button: Button):
        previous_data = get_previous_signup(str(interaction.user.id))
        await interaction.response.send_modal(UserSignupForm(previous_data=previous_data))

    @ui.button(label="Sign Up as Mentor", style=ButtonStyle.danger, custom_id="sang_signup_mentor", emoji="🎓")
    async def mentor_signup_button(self, interaction: discord.Interaction, button: Button):
        user = interaction.user
        member = interaction.guild.get_member(user.id)
        if not member:
             await interaction.response.send_message("⚠️ Could not verify your roles. Please try again.", ephemeral=True)
             return

        has_mentor_role = any(role.id == MENTOR_ROLE_ID for role in member.roles)
        previous_data = get_previous_signup(str(user.id))

        if not has_mentor_role:
            await interaction.response.send_modal(MentorSignupForm(previous_data=previous_data))
            return

        is_auto_signup = previous_data and previous_data.get("KC") == "X"

        if not is_auto_signup:
            await interaction.response.defer(ephemeral=True)
            if not sang_sheet or not history_sheet: # <-- MODIFIED
                await interaction.followup.send("⚠️ Error: The Sanguine Sunday signup or history sheet is not connected.", ephemeral=True) # <-- MODIFIED
                return

            user_id = str(user.id)
            user_name = member.display_name
            timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")

            row_data = [
                user_id, user_name, "All", "X",
                True, "Mentor", False, # <-- Changed from "Highly Proficient"
                timestamp
            ]

            try:
                cell = sang_sheet.find(user_id, in_column=1)
                if cell is None:
                    sang_sheet.append_row(row_data)
                else:
                    sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:H{cell.row}') # <-- FIXED

                # --- MODIFIED HISTORY WRITE ---
                if history_sheet:
                    try:
                        history_cell = history_sheet.find(user_id, in_column=1)
                        if history_cell is None:
                            history_sheet.append_row(row_data)
                        else:
                            history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:H{history_cell.row}') # <-- FIXED
                    except Exception as e:
                        print(f"🔥 GSpread error on HISTORY (Auto-Mentor) write: {e}")
                else:
                    print("🔥 History sheet not available, skipping history append.")
                # --- END MODIFIED ---

                await interaction.followup.send(
                    "✅ **Auto-signed up as Mentor!** (Detected Mentor role).\n"
                    "Your proficiency is set to Highly Proficient, Favorite Roles to All, and Scythe to Yes.\n"
                    "**If this is incorrect, click the button again to fill out the form.**",
                    ephemeral=True
                )
            except gspread.CellNotFound:
                 sang_sheet.append_row(row_data)

                 # --- MODIFIED HISTORY WRITE ---
                 if history_sheet:
                    try:
                        history_cell = history_sheet.find(user_id, in_column=1)
                        if history_cell is None:
                            history_sheet.append_row(row_data)
                        else:
                            history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:H{history_cell.row}') # <-- FIXED
                    except Exception as e:
                        print(f"🔥 GSpread error on HISTORY (Auto-Mentor) write: {e}")
                 else:
                     print("🔥 History sheet not available, skipping history append.")
                 # --- END MODIFIED ---

                 await interaction.followup.send(
                    "✅ **Auto-signed up as Mentor!** (Detected Mentor role).\n"
                    "Your proficiency is set to Highly Proficient, Favorite Roles to All, and Scythe to Yes.\n"
                    "**If this is incorrect, click the button again to fill out the form.**",
                    ephemeral=True
                )
            except Exception as e:
                print(f"🔥 GSpread error on auto mentor signup: {e}")
                await interaction.followup.send("⚠️ An error occurred while auto-signing you up.", ephemeral=True)

        else:
            previous_data["KC"] = ""
            await interaction.response.send_modal(MentorSignupForm(previous_data=previous_data))

# --- Helper Functions ---

# --- Sanguine Sunday VC/Channel Config ---
SANG_VC_CATEGORY_ID = 1376645103803830322  # Category for auto-created team voice channels
SANG_POST_CHANNEL_ID = 1338295765759688767  # Default text channel to post teams

def normalize_role(p: dict) -> str:
    # Honor explicit Mentor flag if present
    prof = str(p.get("proficiency","")).strip().lower()
    if prof == "mentor":
        return "mentor"
    # Otherwise infer from KC ranges (event rules)
    try:
        kc = int(p.get("kc") or p.get("KC") or 0)
    except Exception:
        kc = 0
    if kc <= 10:
        return "new"
    if 11 <= kc <= 25:
        return "learner"
    if 26 <= kc <= 100:
        return "proficient"
    return "highly proficient"

def prof_rank(p: dict) -> int:
    role = normalize_role(p)
    order = {"mentor":0, "highly proficient":1, "proficient":2, "learner":3, "new":4}
    return order.get(role, 5)

PROF_ORDER = {"mentor": 0, "highly proficient": 1, "proficient": 2, "learner": 3, "new": 4}

def prof_rank(p: dict) -> int:
    return PROF_ORDER.get(p.get("proficiency", "").lower(), 99)

def scythe_icon(p: dict) -> str:
    return "✅" if p.get("has_scythe") else "❌"

async def find_latest_signup_message(channel: discord.TextChannel) -> Optional[discord.Message]:
    """Finds the most recent Sanguine Sunday signup message in a channel."""
    async for message in channel.history(limit=100):
        if message.author == bot.user and SANG_MESSAGE_IDENTIFIER in message.content:
            return message
    return None

# --- Core Functions ---
async def post_signup(channel: discord.TextChannel):
    """Posts the main signup message with the signup buttons."""
    await channel.send(SANG_MESSAGE, view=SignupView())
    print(f"✅ Posted Sanguine Sunday signup in #{channel.name}")

async def post_reminder(channel: discord.TextChannel):
    """Finds learners (New or Learner proficiency) from GSheet and posts a reminder."""
    if not sang_sheet:
        print("⚠️ Cannot post reminder, Sang Sheet not connected.")
        return False # Indicate failure

    # Delete previous reminders from the bot
    try:
        async for message in channel.history(limit=50):
            if message.author == bot.user and LEARNER_REMINDER_IDENTIFIER in message.content:
                await message.delete()
    except discord.Forbidden:
        print(f"⚠️ Could not delete old reminders in #{channel.name} (Missing Permissions)")
    except Exception as e:
        print(f"🔥 Error cleaning up reminders: {e}")

    learners = []
    try:
        all_signups = sang_sheet.get_all_records() # Fetch all signups
        for signup in all_signups:
            # Check the Proficiency column (case-insensitive)
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
        return True # Indicate success
    except Exception as e:
        print(f"🔥 GSpread error fetching/posting reminder: {e}")
        await channel.send("⚠️ Error processing learner list from database.")
        return False # Indicate failure

# --- Slash Command Group ---
@bot.tree.command(name="sangsignup", description="Manage Sanguine Sunday signups.")
@app_commands.checks.has_role(STAFF_ROLE_ID)
@app_commands.describe(
    variant="Choose the action to perform.",
    channel="Optional channel to post in (defaults to the configured event channel)."
)
@app_commands.choices(variant=[
    app_commands.Choice(name="Post Signup Message", value=1),
    app_commands.Choice(name="Post Learner Reminder", value=2),
])
async def sangsignup(interaction: discord.Interaction, variant: int, channel: Optional[discord.TextChannel] = None):
    target_channel = channel or bot.get_channel(SANG_CHANNEL_ID)
    if not target_channel:
        await interaction.response.send_message("⚠️ Could not find the target channel.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    if variant == 1:
        await post_signup(target_channel)
        await interaction.followup.send(f"✅ Signup message posted in {target_channel.mention}.")
    elif variant == 2:
        result = await post_reminder(target_channel)
        if result:
            await interaction.followup.send(f"✅ Learner reminder posted in {target_channel.mention}.")
        else:
            await interaction.followup.send("⚠️ Could not post the reminder.")

@sangsignup.error
async def sangsignup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("❌ You don't have permission.", ephemeral=True)
    else:
        print(f"Error in sangsignup command: {error}")
        # Use followup if response already sent (due to defer)
        if interaction.response.is_done():
             await interaction.followup.send(f"An unexpected error occurred.", ephemeral=True)
        else:
             await interaction.response.send_message(f"An unexpected error occurred.", ephemeral=True)


# --- Helper function for role parsing ---
def parse_roles(roles_str: str) -> (bool, bool):
    """Parses a roles string to check for range and melee keywords."""
    if not roles_str or roles_str == "N/A":
        return False, False

    roles_str = roles_str.lower()
    knows_range = any(s in roles_str for s in ["range", "ranger", "rdps"])
    knows_melee = any(s in roles_str for s in ["melee", "mdps", "meleer"])
    return knows_range, knows_melee

# --- Helper function to get complementary learners ---
def pop_complementary_learners(learners_list: list) -> (dict, dict):
    """
    Pops the first learner and tries to find a complementary
    (range/melee) learner to pop and return as a pair.
    """
    if not learners_list or len(learners_list) < 2:
        return None, None

    l1 = learners_list.pop(0)
    l1_range, l1_melee = l1.get('knows_range', False), l1.get('knows_melee', False)

    # If l1 doesn't know a specific role, no complement is needed
    if not (l1_range or l1_melee):
        return l1, learners_list.pop(0)

    # Try to find a complement
    for i, l2 in enumerate(learners_list):
        l2_range, l2_melee = l2.get('knows_range', False), l2.get('knows_melee', False)
        # Check for complementary roles
        if (l1_range and l2_melee) or (l1_melee and l2_range):
            return l1, learners_list.pop(i)

    # No complement found, just return the next in line
    return l1, learners_list.pop(0)


# --- Matchmaking Slash Command (REWORKED + Highly Proficient) ---
@bot.tree.command(name="sangmatch", description="Create ToB teams from signups in a voice channel.")
@app_commands.checks.has_role(STAFF_ROLE_ID)
@app_commands.describe(voice_channel="Optional: The voice channel to pull users from. If omitted, uses all signups.")
async def sangmatch(interaction: discord.Interaction, voice_channel: Optional[discord.VoiceChannel] = None):
    # --- SANG_POSTPROCESS_RULES: enforce hard constraints ---
    def role_of(p): return normalize_role(p)
    def is_new(p): return role_of(p) == "new"
    def is_learner(p): return role_of(p) == "learner"
    def is_pro(p): return role_of(p) in ("proficient","highly proficient")
    def is_hp(p): return role_of(p) == "highly proficient"
    def is_mentor(p): return role_of(p) == "mentor"
    def has_scythe(p): return bool(p.get("has_scythe"))
    def is_freeze_learner(p): 
        return str(p.get("learning_freeze")).lower() in ("true","1","yes")

    def count(predicate, team): return sum(1 for x in team if predicate(x))
    def has(predicate, team): return any(predicate(x) for x in team)

    # Ensure every team with Mentor has at least one Pro/HP
    def ensure_mentor_with_pro(teams):
        for i, t in enumerate(teams):
            if has(is_mentor, t) and not has(is_pro, t):
                # borrow from a team with >1 pro/hp
                for j, u in enumerate(teams):
                    if i==j: continue
                    if sum(1 for x in u if is_pro(x)) > 1:
                        idx = next(k for k,x in enumerate(u) if is_pro(x))
                        t.append(u.pop(idx)); break

    # New must be only on 4-man teams, with Mentor and Pro/HP
    def enforce_new_rules(teams):
        for i, t in enumerate(list(teams)):
            if count(is_new, t) > 0:
                # remove from size 3/5
                while len(t) in (3,5) and count(is_new, t) > 0:
                    idx = next(k for k,x in enumerate(t) if is_new(x))
                    moved = t.pop(idx)
                    # place into a team with size 4, mentor+pro and <2 news
                    placed = False
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if len(u) == 4 and has(is_mentor,u) and has(is_pro,u) and count(is_new,u) < 2:
                            u.append(moved); placed=True; break
                    if not placed:
                        teams.append([moved]); t = teams[i]
                # ensure mentor present
                if not has(is_mentor, t):
                    # borrow a mentor
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if count(is_mentor, u) > 1:
                            idx = next(k for k,x in enumerate(u) if is_mentor(x))
                            t.append(u.pop(idx)); break
                # ensure pro present
                if not has(is_pro, t):
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if sum(1 for x in u if is_pro(x)) > 1:
                            idx = next(k for k,x in enumerate(u) if is_pro(x))
                            t.append(u.pop(idx)); break
                # ensure size is 4: move non-new out or relocate new
                while len(t) != 4 and count(is_new, t) > 0:
                    if len(t) > 4:
                        # move a non-new out
                        idx = next((k for k,x in enumerate(t) if not is_new(x)), None)
                        if idx is not None:
                            # move into team with <5
                            for j,u in enumerate(teams):
                                if i==j: continue
                                if len(u) < 5:
                                    u.append(t.pop(idx)); break
                        else:
                            break
                    elif len(t) < 4:
                        # pull a pro/hp from other
                        pulled=False
                        for j,u in enumerate(teams):
                            if i==j: continue
                            idx = next((k for k,x in enumerate(u) if is_pro(x)), None)
                            if idx is not None:
                                t.append(u.pop(idx)); pulled=True; break
                        if not pulled:
                            break

    # Learners: 3 only if scythe; 5 only if no New
    def enforce_learner_3_and_5(teams):
        for i, t in enumerate(teams):
            if len(t) == 3:
                if any(is_learner(x) and not has_scythe(x) for x in t):
                    # move such learner to a 4/no-new team
                    idx = next(k for k,x in enumerate(t) if is_learner(x) and not has_scythe(x))
                    moved = t.pop(idx)
                    placed=False
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if len(u) == 4 and count(is_new,u) == 0:
                            u.append(moved); placed=True; break
                    if not placed: teams.append([moved])
            if len(t) == 5 and count(is_new,t) > 0:
                # prefer moving new out; fallback move a learner out
                if count(is_new,t) > 0:
                    idx = next(k for k,x in enumerate(t) if is_new(x))
                    moved = t.pop(idx)
                else:
                    idx = next((k for k,x in enumerate(t) if is_learner(x)), 0)
                    moved = t.pop(idx)
                placed=False
                for j,u in enumerate(teams):
                    if i==j: continue
                    if len(u) < 5 and count(is_new,u) == 0:
                        u.append(moved); placed=True; break
                if not placed: teams.append([moved])

    # Freeze learners: never together
    def split_freeze_learners(teams):
        for i,t in enumerate(teams):
            while sum(1 for x in t if is_freeze_learner(x)) > 1:
                idx = next(k for k,x in enumerate(t) if is_freeze_learner(x))
                moved = t.pop(idx)
                placed=False
                for j,u in enumerate(teams):
                    if i==j: continue
                    if sum(1 for x in u if is_freeze_learner(x)) == 0 and len(u) < 5:
                        u.append(moved); placed=True; break
                if not placed: teams.append([moved])

    def rebalance(teams):
        ensure_mentor_with_pro(teams)
        enforce_new_rules(teams)
        enforce_learner_3_and_5(teams)
        split_freeze_learners(teams)



def enforce_min_team_size_and_new_support(teams):
    """Ensure no team < 4, and every team with a New has Mentor + Pro/HP."""
    def role_of(p): return normalize_role(p)
    def is_new(p): return role_of(p) == "new"
    def is_pro_or_hp(p): return role_of(p) in ("proficient","highly proficient")
    def is_mentor(p): return role_of(p) == "mentor"

    # First, try to merge undersized teams (<4) into others
    changed = True
    while changed:
        changed = False
        for i, t in enumerate(list(teams)):
            if len(t) > 0 and len(t) < 4:
                # Prefer to move members (non-new if possible) to the smallest teams <5
                movables = [idx for idx, x in enumerate(t) if not is_new(x)] or list(range(len(t)))
                while movables and len(t) < 4:
                    m_idx = movables.pop(0)
                    member = t.pop(m_idx)
                    placed = False
                    # place into team with size <5
                    for j, u in enumerate(teams):
                        if i == j: continue
                        if len(u) < 5:
                            u.append(member); placed = True; break
                    if placed:
                        changed = True
                    else:
                        # couldn't place; put back and stop
                        t.insert(m_idx, member)
                        break
                if len(t) == 0:
                    try:
                        teams.remove(t)
                    except ValueError:
                        pass

    # Now ensure New support: Mentor + Pro/HP in any team that contains a New
    for i, t in enumerate(teams):
        if any(is_new(x) for x in t):
            if not any(is_mentor(x) for x in t):
                # borrow a Mentor from a team that has >1 Mentors or can spare
                for j, u in enumerate(teams):
                    if i == j: continue
                    if sum(1 for x in u if is_mentor(x)) > 1:
                        idxm = next(k for k,x in enumerate(u) if is_mentor(x))
                        t.append(u.pop(idxm)); break
            if not any(is_pro_or_hp(x) for x in t):
                for j, u in enumerate(teams):
                    if i == j: continue
                    if sum(1 for x in u if is_pro_or_hp(x)) > 1:
                        idxs = next(k for k,x in enumerate(u) if is_pro_or_hp(x))
                        t.append(u.pop(idxs)); break
    if not sang_sheet:
        await interaction.response.send_message("⚠️ Error: The Sanguine Sunday sheet is not connected.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False) # Send to channel

    # --- 1. Get users in the specified voice channel ---
    vc_member_ids = None # <-- ADDED
    channel_name = "All Signups" # <-- ADDED

    if voice_channel: # <-- ADDED IF BLOCK
        channel_name = voice_channel.name
        if not voice_channel.members:
            await interaction.followup.send(f"⚠️ No users are in {voice_channel.mention}.")
            return

        vc_member_ids = {str(member.id) for member in voice_channel.members if not member.bot}
        if not vc_member_ids:
            await interaction.followup.send(f"⚠️ No human users are in {voice_channel.mention}.")
            return

    # --- 2. Get all signups from GSheet ---
    try:
        all_signups_records = sang_sheet.get_all_records()
        if not all_signups_records:
            await interaction.followup.send("⚠️ There are no signups in the database.")
            return
    except Exception as e:
        print(f"🔥 GSheet error fetching all signups: {e}")
        await interaction.followup.send("⚠️ An error occurred fetching signups from the database.")
        return

    # --- 3. Filter signups to only users in the VC and parse roles ---
    available_raiders = []
    for signup in all_signups_records:
        user_id = str(signup.get("Discord_ID"))
        
        # --- MODIFIED VC CHECK ---
        # If vc_member_ids is set (a VC was provided), filter by it.
        if vc_member_ids and user_id not in vc_member_ids:
             continue # Skip this user, not in the specified VC
        
        roles_str = signup.get("Favorite Roles", "")
        knows_range, knows_melee = parse_roles(roles_str)
        kc_raw = signup.get("KC", 0) # Get KC value, default to 0
        try:
            # Convert KC to int, handle potential non-numeric values (like 'N/A' or 'X' for mentors)
            kc_val = int(kc_raw)
        except (ValueError, TypeError):
            # For Mentors with 'X' or other non-numbers, treat KC as very high for sorting purposes
            kc_val = 9999 if signup.get("Proficiency", "").lower() == 'mentor' else 0


        # --- Determine Proficiency including Highly Proficient ---
        # Use the value from the sheet if it's 'Mentor', otherwise calculate based on KC
        proficiency_val = signup.get("Proficiency", "").lower()
        if proficiency_val != 'mentor': # Recalculate if not mentor (in case KC changed)
            if kc_val <= 1:
                proficiency_val = "new"
            elif 1 < kc_val < 50:
                proficiency_val = "learner"
            elif 50 <= kc_val < 150:
                proficiency_val = "proficient"
            else: # 150+ KC
                proficiency_val = "highly proficient"

        available_raiders.append({
            "user_id": user_id,
                "user_name": sanitize_nickname(signup.get("Discord_Name")),
                "proficiency": proficiency_val, # Use calculated/sheet proficiency
                "kc": kc_val, # Use the integer KC value (or default)
                "has_scythe": str(signup.get("Has_Scythe", "FALSE")).upper() == "TRUE",
                "roles_known": roles_str,
                "learning_freeze": str(signup.get("Learning Freeze", "FALSE")).upper() == "TRUE",
                "knows_range": knows_range,
                "knows_melee": knows_melee
            })

    if not available_raiders:
        await interaction.followup.send(f"⚠️ None of the users in {voice_channel.mention} have signed up for the event.")
        return

    
    # --- 4. Matchmaking Logic (mentor-first, prefer teams of 5) ---
    guild = interaction.guild

    # 4a) Sort globally: Mentor → HP → Pro → Learner → New, then scythe, then KC desc
    available_raiders.sort(
        key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0)))
    )

    mentors = [p for p in available_raiders if p["proficiency"] == "mentor"]
    high_pro = [p for p in available_raiders if p["proficiency"] == "highly proficient"]
    pro      = [p for p in available_raiders if p["proficiency"] == "proficient"]
    learners = [p for p in available_raiders if p["proficiency"] == "learner"]
    news     = [p for p in available_raiders if p["proficiency"] == "new"]

    # prefer 5-man teams; number of teams bounded by mentors (one per team if possible)
    total_players = len(available_raiders)
    ideal_size = 5
    min_teams_by_size = math.ceil(total_players / ideal_size) if total_players else 0
    num_teams = max(1, min(len(mentors) if mentors else min_teams_by_size, min_teams_by_size))

    # If not enough mentors for the minimum by size, ensure at least 1 team (or as many mentors as we have)
    if len(mentors) < num_teams:
        num_teams = max(len(mentors), 1)

    # Create empty teams and put **one** mentor per team (no double mentor while another team has none)
    teams = [[] for _ in range(num_teams)]
    for i, mtr in enumerate(mentors[:num_teams]):
        teams[i].append(mtr)

    # Pool of “strong” (HP first, then Pro) for backfilling
    strong = high_pro + pro

    # 4b) Give each team at least 2 strong players if possible (balance scythes round-robin)
    def pop_with_scythe(prefer_scythe=True):
        idx = next((i for i, p in enumerate(strong) if p.get("has_scythe")), None) if prefer_scythe else None
        if idx is None:
            return strong.pop(0) if strong else None
        return strong.pop(idx)

    # First pass: 1 strong per team
    for i in range(num_teams):
        if not strong:
            break
        prefer_s = not any(member.get("has_scythe") for member in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick:
            teams[i].append(pick)

    # Second pass: try to get each team to 2 strong
    for i in range(num_teams):
        if not strong:
            break
        prefer_s = not any(member.get("has_scythe") for member in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick:
            teams[i].append(pick)

    # 4c) Add learners next (place Learner before New; avoid making New-only teams)
    def add_role_bucket(bucket):
        i = 0
        while bucket:
            placed = False
            for _ in range(num_teams):
                t = teams[i]
                if len(t) < ideal_size:
                    has_strong = any(prof_rank(p) <= PROF_ORDER["proficient"] for p in t)
                    if has_strong or bucket is learners:
                        t.append(bucket.pop(0))
                        placed = True
                        break
                i = (i + 1) % num_teams
            if not placed:
                break

    add_role_bucket(learners)
    add_role_bucket(news)

    # 4d) Fill remaining seats with leftover strong players
    while strong:
        placed = False
        for i in range(num_teams):
            if len(teams[i]) < ideal_size:
                teams[i].append(strong.pop(0))
                placed = True
                if not strong:
                    break
        if not placed:
            break

    # 4e) If people still left (rare), spill into 4-man teams
    leftover_pool = [p for p in available_raiders if p not in [m for team in teams for m in team]]
    while leftover_pool:
        spill = leftover_pool[:4]
        leftover_pool = leftover_pool[4:]
        teams.append(spill)

    # --- 4f) Post-process to enforce New-player constraints ---
    def is_new(p): 
        return p.get("proficiency") == "new"
    def count_new(team): 
        return sum(1 for p in team if is_new(p))

    # Helper: try to swap a 'new' from team A with a non-new from team B
    def swap_new_out(team_a_idx, team_b_idx):
        A = teams[team_a_idx]; B = teams[team_b_idx]
        a_new_idx = next((i for i,p in enumerate(A) if is_new(p)), None)
        b_non_idx = next((i for i,p in enumerate(B) if not is_new(p)), None)
        if a_new_idx is None or b_non_idx is None:
            return False
        A[a_new_idx], B[b_non_idx] = B[b_non_idx], A[a_new_idx]
        return True

    # Pass 1: No team of size 3 or 5 may include 'new'
    for idx, t in enumerate(teams):
        if len(t) in (3, 5) and count_new(t) > 0:
            # find a team we can trade with (prefer size 4)
            candidates = sorted(
                [j for j in range(len(teams)) if j != idx],
                key=lambda j: (abs(len(teams[j]) - 4), count_new(teams[j]))
            )
            for j in candidates:
                if swap_new_out(idx, j) and not (len(teams[idx]) in (3,5) and count_new(teams[idx])>0):
                    break  # fixed this team
            # If still not fixed, just remove extra 'new' into a new 4-man spill team
            while len(t) in (3,5) and count_new(t) > 0:
                # move one 'new' to a new temporary bucket, will be appended later
                rem_idx = next(i for i,p in enumerate(t) if is_new(p))
                moved = t.pop(rem_idx)
                placed = False
                for j in range(len(teams)):
                    if j == idx: 
                        continue
                    # place into a team that's not size 3/5, with <=1 new already
                    if len(teams[j]) == 4 and count_new(teams[j]) <= 1:
                        teams[j].append(moved); placed=True; break
                if not placed:
                    # create a new bucket of 4, will fill later if needed
                    teams.append([moved])
                    t = teams[idx]

    # Pass 2: Cap 'new' per team to 2
    overfull = True
    while overfull:
        overfull = False
        for i in range(len(teams)):
            while count_new(teams[i]) > 2:
                overfull = True
                # move one 'new' to a team with <2 new and not size 3/5
                n_idx = next(k for k,p in enumerate(teams[i]) if is_new(p))
                moved = teams[i].pop(n_idx)
                placed = False
                for j in range(len(teams)):
                    if i==j: 
                        continue
                    if count_new(teams[j]) < 2 and len(teams[j]) == 4:
                        teams[j].append(moved); placed=True; break
                if not placed:
                    # as a last resort, create a new team bucket (will become 4 later if possible)
                    teams.append([moved])
                    placed = True

    # Optional tidy-up: try to merge tiny buckets into 4s without violating rules
    # Fill any singleton/doubleton buckets by stealing non-new from large buckets
    for i in range(len(teams)):
        if len(teams[i]) < 4:
            for j in range(len(teams)):
                if i==j: continue
                while len(teams[i]) < 4 and len(teams[j]) > 4:
                    # move a non-new from j to i
                    idx_non = next((k for k,p in enumerate(teams[j]) if not is_new(p)), None)
                    if idx_non is None: break
                    teams[i].append(teams[j].pop(idx_non))


    # --- 5. Output (mention + nickname + scythe icon) ---

    # --- Build teams here (existing logic assumed) then enforce rules ---
    try:
        rebalance(teams)
        enforce_min_team_size_and_new_support(teams)
    except Exception as e:
        # If teams cannot be legally arranged, warn and abort
        warn = discord.Embed(title="❌ Team Generation Failed", description=f"Could not satisfy rules: {e}", color=discord.Color.red())
        await interaction.response.send_message(embed=warn, ephemeral=True)
        return

    # --- Create voice channels under SanguineSunday – Team X pattern ---
    guild = interaction.guild
    category = guild.get_channel(SANG_VC_CATEGORY_ID)
    created_voice_channel_ids = []
    if category and hasattr(category, "create_voice_channel"):
        for i in range(len(teams)):
            try:
                vc = await category.create_voice_channel(name=f"SanguineSunday – Team {i+1}")
                created_voice_channel_ids.append(vc.id)
            except Exception:
                pass  # non-fatal

    # Determine post channel (testing override allowed)
    if 'channel' in locals() and isinstance(channel, discord.TextChannel):
        post_channel = channel
    else:
        post_channel = guild.get_channel(SANG_POST_CHANNEL_ID)
    if post_channel is None:
        post_channel = interaction.channel

    embed = discord.Embed(
        title=f"Sanguine Sunday Teams - {channel_name}",
        description=f"Created {len(teams)} team(s) from {len(available_raiders)} available signed-up users.",
        color=discord.Color.red()
    )

    def user_line(p: dict) -> str:
        uid = int(p["user_id"])
        member = guild.get_member(uid)
        mention = member.mention if member else f"<@{uid}>"
        nickname = member.display_name if member else p.get("user_name", f"User {uid}")
        role_text = p.get("proficiency", "Unknown").replace(" ", "-").capitalize().replace("-", " ")
        kc_raw = p.get("kc", 0)
        kc_text = f"({kc_raw} KC)" if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" else ""
        return f"{mention} • **{role_text}** {kc_text} • {scythe_icon(p)} Scythe •{freeze_icon(p)}"

    # Sort each team's display Mentor → HP → Pro → Learner → New
    for i, team in enumerate(teams, start=1):
        team_sorted = sorted(team, key=prof_rank)
        lines = [user_line(p) for p in team_sorted]
        embed.add_field(name=f"Team {i}", value="\n".join(lines) if lines else "—", inline=False)

    # If a VC was provided, show unassigned
    unassigned_users = set()
    if vc_member_ids:
        assigned_ids = {p["user_id"] for t in teams for p in t}
        unassigned_users = vc_member_ids - assigned_ids
    
    if unassigned_users:
        mentions = []
        for uid in unassigned_users:
            m = guild.get_member(int(uid))
            mentions.append(m.mention if m else f"<@{uid}>")
        embed.add_field(
            name="Unassigned Users in VC",
            value=" ".join(mentions),
            inline=False
        )


    await interaction.followup.send(embed=embed)


# --- Plain-text formatter (no mentions) ---
def format_player_line_plain(guild: discord.Guild, p: dict) -> str:
    nickname = p.get("user_name") or "Unknown"
    role_text = p.get("proficiency", "Unknown").replace(" ", "-").capitalize().replace("-", " ")
    kc_raw = p.get("kc", 0)
    kc_text = f"({kc_raw} KC)" if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" else ""
    return f"{nickname} • **{role_text}** {kc_text} • {scythe_icon(p)} Scythe •{freeze_icon(p)}"

@bot.tree.command(name="sangmatchtest", description="Create ToB teams without pinging or creating voice channels; show plain-text nicknames.")
@app_commands.checks.has_role(STAFF_ROLE_ID)
@app_commands.describe(
    voice_channel="Optional: The voice channel to pull users from. If omitted, uses all signups.",
    channel="(Optional) Override the text channel to post teams (testing)."
)
async def sangmatchtest(
    interaction: discord.Interaction,
    voice_channel: Optional[discord.VoiceChannel] = None,
    channel: Optional[discord.TextChannel] = None
):
    def role_of(p): return normalize_role(p)
    def is_new(p): return role_of(p) == "new"
    def is_learner(p): return role_of(p) == "learner"
    def is_pro(p): return role_of(p) in ("proficient","highly proficient")
    def is_hp(p): return role_of(p) == "highly proficient"
    def is_mentor(p): return role_of(p) == "mentor"
    def has_scythe(p): return bool(p.get("has_scythe"))
    def is_freeze_learner(p): return str(p.get("learning_freeze")).lower() in ("true","1","yes")

    def count(predicate, team): return sum(1 for x in team if predicate(x))
    def has(predicate, team): return any(predicate(x) for x in team)

    def ensure_mentor_with_pro(teams):
        for i, t in enumerate(teams):
            if has(is_mentor, t) and not has(is_pro, t):
                for j, u in enumerate(teams):
                    if i==j: continue
                    if sum(1 for x in u if is_pro(x)) > 1:
                        idx = next(k for k,x in enumerate(u) if is_pro(x))
                        t.append(u.pop(idx)); break

    def enforce_new_rules(teams):
        for i, t in enumerate(list(teams)):
            if count(is_new, t) > 0:
                while len(t) in (3,5) and count(is_new, t) > 0:
                    idx = next(k for k,x in enumerate(t) if is_new(x))
                    moved = t.pop(idx)
                    placed = False
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if len(u) == 4 and has(is_mentor,u) and has(is_pro,u) and count(is_new,u) < 2:
                            u.append(moved); placed=True; break
                    if not placed:
                        teams.append([moved]); t = teams[i]
                if not has(is_mentor, t):
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if count(is_mentor, u) > 1:
                            idx = next(k for k,x in enumerate(u) if is_mentor(x))
                            t.append(u.pop(idx)); break
                if not has(is_pro, t):
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if sum(1 for x in u if is_pro(x)) > 1:
                            idx = next(k for k,x in enumerate(u) if is_pro(x))
                            t.append(u.pop(idx)); break
                while len(t) != 4 and count(is_new, t) > 0:
                    if len(t) > 4:
                        idx = next((k for k,x in enumerate(t) if not is_new(x)), None)
                        if idx is not None:
                            for j,u in enumerate(teams):
                                if i==j: continue
                                if len(u) < 5:
                                    u.append(t.pop(idx)); break
                        else:
                            break
                    elif len(t) < 4:
                        pulled=False
                        for j,u in enumerate(teams):
                            if i==j: continue
                            idx = next((k for k,x in enumerate(u) if is_pro(x)), None)
                            if idx is not None:
                                t.append(u.pop(idx)); pulled=True; break
                        if not pulled:
                            break

    def enforce_learner_3_and_5(teams):
        for i, t in enumerate(teams):
            if len(t) == 3:
                if any(is_learner(x) and not has_scythe(x) for x in t):
                    idx = next(k for k,x in enumerate(t) if is_learner(x) and not has_scythe(x))
                    moved = t.pop(idx)
                    placed=False
                    for j,u in enumerate(teams):
                        if i==j: continue
                        if len(u) == 4 and count(is_new,u) == 0:
                            u.append(moved); placed=True; break
                    if not placed: teams.append([moved])
            if len(t) == 5 and count(is_new,t) > 0:
                if count(is_new,t) > 0:
                    idx = next(k for k,x in enumerate(t) if is_new(x))
                    moved = t.pop(idx)
                else:
                    idx = next((k for k,x in enumerate(t) if is_learner(x)), 0)
                    moved = t.pop(idx)
                placed=False
                for j,u in enumerate(teams):
                    if i==j: continue
                    if len(u) < 5 and count(is_new,u) == 0:
                        u.append(moved); placed=True; break
                if not placed: teams.append([moved])

    def split_freeze_learners(teams):
        for i,t in enumerate(teams):
            while sum(1 for x in t if is_freeze_learner(x)) > 1:
                idx = next(k for k,x in enumerate(t) if is_freeze_learner(x))
                moved = t.pop(idx)
                placed=False
                for j,u in enumerate(teams):
                    if i==j: continue
                    if sum(1 for x in u if is_freeze_learner(x)) == 0 and len(u) < 5:
                        u.append(moved); placed=True; break
                if not placed: teams.append([moved])

    def rebalance(teams):
        ensure_mentor_with_pro(teams)
        enforce_new_rules(teams)
        enforce_learner_3_and_5(teams)
        split_freeze_learners(teams)

    if not sang_sheet:
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
        all_signups_records = sang_sheet.get_all_records()
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
            continue
        roles_str = signup.get("Favorite Roles", "")
        knows_range, knows_melee = parse_roles(roles_str)
        kc_raw = signup.get("KC", 0)
        try:
            kc_val = int(kc_raw)
        except (ValueError, TypeError):
            kc_val = 9999 if signup.get("Proficiency", "").lower() == 'mentor' else 0

        proficiency_val = signup.get("Proficiency", "").lower()
        if proficiency_val != 'mentor':
            if kc_val <= 1:
                proficiency_val = "new"
            elif 2 <= kc_val <= 25:
                proficiency_val = "learner"
            elif 26 <= kc_val <= 149:
                proficiency_val = "proficient"
            else:
                proficiency_val = "highly proficient"

        available_raiders.append({
            "user_id": user_id,
            "user_name": sanitize_nickname(signup.get("Discord_Name")),
            "proficiency": proficiency_val,
            "kc": kc_val,
            "has_scythe": str(signup.get("Has_Scythe", "FALSE")).upper() == "TRUE",
            "roles_known": roles_str,
            "learning_freeze": str(signup.get("Learning Freeze", "FALSE")).upper() == "TRUE",
            "knows_range": knows_range,
            "knows_melee": knows_melee
        })

    if not available_raiders:
        await interaction.followup.send(f"⚠️ None of the users in {voice_channel.mention} have signed up for the event." if voice_channel else "⚠️ No eligible signups.")
        return

    available_raiders.sort(key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0))))
    mentors   = [p for p in available_raiders if p["proficiency"] == "mentor"]
    high_pro  = [p for p in available_raiders if p["proficiency"] == "highly proficient"]
    pro       = [p for p in available_raiders if p["proficiency"] == "proficient"]
    learners  = [p for p in available_raiders if p["proficiency"] == "learner"]
    news      = [p for p in available_raiders if p["proficiency"] == "new"]

    total_players = len(available_raiders)
    ideal_size = 5
    min_teams_by_size = math.ceil(total_players / ideal_size) if total_players else 0
    num_teams = max(1, min(len(mentors) if mentors else min_teams_by_size, min_teams_by_size))
    if len(mentors) < num_teams:
        num_teams = max(len(mentors), 1)

    teams = [[] for _ in range(num_teams)]
    for i, mtr in enumerate(mentors[:num_teams]): teams[i].append(mtr)
    strong = high_pro + pro

    def pop_with_scythe(prefer_scythe=True):
        idx = next((i for i, p in enumerate(strong) if p.get("has_scythe")), None) if prefer_scythe else None
        if idx is None: return strong.pop(0) if strong else None
        return strong.pop(idx)

    for i in range(num_teams):
        if not strong: break
        prefer_s = not any(member.get("has_scythe") for member in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick: teams[i].append(pick)

    for i in range(num_teams):
        if not strong: break
        prefer_s = not any(member.get("has_scythe") for member in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick: teams[i].append(pick)

    def add_role_bucket(bucket):
        i = 0
        while bucket:
            placed = False
            for _ in range(num_teams):
                t = teams[i]
                if len(t) < ideal_size:
                    has_strong = any(prof_rank(p) <= PROF_ORDER["proficient"] for p in t)
                    if has_strong or bucket is learners:
                        t.append(bucket.pop(0)); placed = True; break
                i = (i + 1) % num_teams
            if not placed: break

    add_role_bucket(learners)
    add_role_bucket(news)

    while strong:
        placed = False
        for i in range(num_teams):
            if len(teams[i]) < ideal_size:
                teams[i].append(strong.pop(0)); placed = True
                if not strong: break
        if not placed: break

    leftover_pool = [p for p in available_raiders if p not in [m for team in teams for m in team]]
    while leftover_pool:
        spill = leftover_pool[:4]; leftover_pool = leftover_pool[4:]; teams.append(spill)

    try:
        rebalance(teams)
        enforce_min_team_size_and_new_support(teams)
    except Exception as e:
        warn = discord.Embed(title="❌ Team Generation Failed", description=f"Could not satisfy rules: {e}", color=discord.Color.red())
        await interaction.response.send_message(embed=warn, ephemeral=True)
        return

    guild = interaction.guild
    post_channel = channel or guild.get_channel(SANG_POST_CHANNEL_ID) or interaction.channel
    embed = discord.Embed(
        title=f"Sanguine Sunday Teams (Test, no pings/VC) - {channel_name}",
        description=f"Created {len(teams)} team(s) from {len(available_raiders)} available signed-up users.",
        color=discord.Color.dark_gray()
    )
    for i, team in enumerate(teams, start=1):
        team_sorted = sorted(team, key=prof_rank)
        lines = [format_player_line_plain(guild, p) for p in team_sorted]
        embed.add_field(name=f"Team {i}", value="\n".join(lines) if lines else "—", inline=False)

    if voice_channel:
        assigned_ids = {p["user_id"] for t in teams for p in t}
        unassigned_ids = [uid for uid in vc_member_ids or [] if uid not in assigned_ids]
        if unassigned_ids:
            names = []
            for uid in unassigned_ids:
                m = guild.get_member(int(uid))
                names.append(m.display_name if m else f"User {uid}")
            embed.add_field(name="Unassigned Users in VC", value=", ".join(names), inline=False)

    await post_channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    await interaction.followup.send("✅ Posted no-ping test teams (no voice channels created).", ephemeral=True)




from pathlib import Path

from pathlib import Path

@bot.tree.command(name="sangexport", description="Export the most recently generated teams to a text file.")
@app_commands.checks.has_any_role("Administrators", "Clan Staff", "Senior Staff", "Staff", "Trial Staff")
async def sangexport(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    global last_generated_teams
    teams = last_generated_teams if 'last_generated_teams' in globals() else None
    if not teams:
        await interaction.followup.send("⚠️ No teams found from this session.", ephemeral=True)
        return

    guild = interaction.guild

    def resolve_discord_id(p: dict):
        sname = sanitize_nickname(p.get("user_name", ""))
        mid = find_member_id_by_sanitized_nickname(guild, sname)
        if mid:
            return mid
        uid_str = str(p.get("user_id") or p.get("Discord_ID") or "")
        return int(uid_str) if uid_str.isdigit() else None

    lines = []
    for i, team in enumerate(teams, start=1):
        lines.append(f"Team {i}")
        for p in team:
            sname = sanitize_nickname(p.get("user_name", "Unknown"))
            mid = resolve_discord_id(p)
            id_text = str(mid) if mid is not None else "UnknownID"
            lines.append(f"  - {sname} — ID: {id_text}")
        lines.append("")

    txt = "".join(lines)

    export_dir = Path(os.getenv("SANG_EXPORT_DIR", "/mnt/data"))
    try:
        export_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        export_dir = Path("/tmp")
        export_dir.mkdir(parents=True, exist_ok=True)

    from datetime import datetime
    from zoneinfo import ZoneInfo
    CST = ZoneInfo("America/Chicago")
    ts = datetime.now(CST).strftime("%Y%m%d_%H%M%S")
    outpath = export_dir / f"sanguine_teams_{ts}.txt"
    with open(outpath, "w", encoding="utf-8") as f:
        f.write(txt)

    preview = "
".join(lines[:min(12, len(lines))])
    await interaction.followup.send(
        content=f"📄 Exported teams to **{outpath.name}**:
```
{preview}
```",
        file=discord.File(str(outpath), filename=outpath.name),
        ephemeral=True
    )
return

    guild = interaction.guild

    def resolve_discord_id(p: dict):
        sname = sanitize_nickname(p.get("user_name", ""))
        mid = find_member_id_by_sanitized_nickname(guild, sname)
        if mid:
            return mid
        uid_str = str(p.get("user_id") or p.get("Discord_ID") or "")
        return int(uid_str) if uid_str.isdigit() else None

    lines = []
    for i, team in enumerate(teams, start=1):
        lines.append(f"Team {i}")
        for p in team:
            sname = sanitize_nickname(p.get("user_name", "Unknown"))
            mid = resolve_discord_id(p)
            id_text = str(mid) if mid is not None else "UnknownID"
            lines.append(f"  - {sname} — ID: {id_text}")
        lines.append("")

    txt = "
".join(lines)

    export_dir = Path(os.getenv("SANG_EXPORT_DIR", "/mnt/data"))
    try:
        export_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        export_dir = Path("/tmp")
        export_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.datetime.now(CST).strftime("%Y%m%d_%H%M%S")
    outpath = export_dir / f"sanguine_teams_{ts}.txt"
    with open(outpath, "w", encoding="utf-8") as f:
        f.write(txt)

    # send a short message preview and attach the file
    preview = "
".join(lines[:min(10, len(lines))])
    await interaction.followup.send(
        content=f"📄 Exported teams to **{outpath.name}**:
```
{preview}
```",
        file=discord.File(str(outpath), filename=outpath.name),
        ephemeral=True
    )

@bot.tree.command(name="sangcleanup", description="Delete auto-created SanguineSunday voice channels from the last run.")
@app_commands.checks.has_any_role("Administrators", "Clan Staff", "Senior Staff", "Staff", "Trial Staff")
async def sangcleanup(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    guild = interaction.guild
    # The IDs we created are not persisted across restarts; this cleans up by name under category.
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
            pass
    await interaction.followup.send(f"🧹 Deleted {deleted} voice channels.", ephemeral=True)

@sangmatch.error
async def sangmatch_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("❌ You don't have permission.", ephemeral=True)
    else:
        print(f"Error in sangmatch command: {error}")
        if not interaction.response.is_done():
            await interaction.response.send_message(f"An unexpected error occurred.", ephemeral=True)
        else:
            await interaction.followup.send(f"An unexpected error occurred.", ephemeral=True)


# --- Scheduled Tasks ---
SANG_SHEET_HEADER = ["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Timestamp"]

@tasks.loop(time=dt_time(hour=11, minute=0, tzinfo=CST))
async def scheduled_post_signup():
    """Posts the signup message every Friday at 11:00 AM CST."""
    if datetime.now(CST).weekday() == 4:  # 4 = Friday
        channel = bot.get_channel(SANG_CHANNEL_ID)
        if channel:
            await post_signup(channel)

@tasks.loop(time=dt_time(hour=14, minute=0, tzinfo=CST))
async def scheduled_post_reminder():
    """Posts the learner reminder every Saturday at 2:00 PM CST."""
    if datetime.now(CST).weekday() == 5:  # 5 = Saturday
        channel = bot.get_channel(SANG_CHANNEL_ID)
        if channel:
            await post_reminder(channel)

@tasks.loop(time=dt_time(hour=4, minute=0, tzinfo=CST)) # 4 AM CST
async def scheduled_clear_sang_sheet():
    """Clears the SangSignups sheet every Monday at 4:00 AM CST."""
    if datetime.now(CST).weekday() == 0:  # 0 = Monday
        print("MONDAY DETECTED: Clearing SangSignups sheet...")
        if sang_sheet:
            try:
                sang_sheet.clear()
                sang_sheet.append_row(SANG_SHEET_HEADER)
                print("✅ SangSignups sheet cleared and headers added.")
            except Exception as e:
                print(f"🔥 Failed to clear SangSignups sheet: {e}")
        else:
            print("⚠️ Cannot clear SangSignups sheet, not connected.")

@scheduled_post_signup.before_loop
@scheduled_post_reminder.before_loop
@scheduled_clear_sang_sheet.before_loop # <-- ADDED
async def before_scheduled_tasks():
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")

    bot.add_view(SignupView()) # <-- Added for Sanguine Sunday

    # Start the Sanguine Sunday tasks
    if not scheduled_post_signup.is_running():
        scheduled_post_signup.start()
        print("✅ Started scheduled signup task.")
    if not scheduled_post_reminder.is_running():
        scheduled_post_reminder.start()
        print("✅ Started scheduled reminder task.")
    
    if not scheduled_clear_sang_sheet.is_running(): # <-- ADDED
        scheduled_clear_sang_sheet.start() # <-- ADDED
        print("✅ Started scheduled sang sheet clear task.") # <-- ADDED
    
    try:
        synced = await bot.tree.sync()
        print(f"✅ Synced {len(synced)} commands.")
    except Exception as e:
        print(f"❌ Command sync failed: {e}")

# ---------------------------
# 🔹 Run Bot
# ---------------------------
bot.run(os.getenv('DISCORD_BOT_TOKEN'))
