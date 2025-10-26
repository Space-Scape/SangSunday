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
        sang_sheet.append_row(SANG_SHEET_HEADER)

    # Try to get History sheet
    try:
        history_sheet = sang_google_sheet.worksheet(SANG_HISTORY_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"'{SANG_HISTORY_TAB_NAME}' not found. Creating...")
        history_sheet = sang_google_sheet.add_worksheet(title=SANG_HISTORY_TAB_NAME, rows="1000", cols="20")
        history_sheet.append_row(SANG_SHEET_HEADER)

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
SENIOR_STAFF_CHANNEL_ID = 1336473990302142484 # Channel for approval notifications.
ADMINISTRATOR_ROLE_ID = 1272961765034164318  # Role that can approve actions.
SENIOR_STAFF_ROLE_ID = 1336473488159936512   # Role that can approve actions.

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

# --- Utility to sanitize nickname (remove discord user tag for display) ---
def sanitize_nickname(name: str) -> str:
    # Removes common Discord tag patterns like @User#1234 or (User#1234)
    if not name:
        return ""
    # Remove everything in parentheses that might contain user tags
    name = re.sub(r'\s*\([^)]*#\d{4}\)', '', name)
    # Remove everything in brackets that might contain user tags
    name = re.sub(r'\s*\[[^\]]*#\d{4}\]', '', name)
    # Remove @mention at the start
    name = re.sub(r'^@', '', name)
    return name.strip()


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

        # --- Re-aligning KC based on new ranges ---
        proficiency_value = ""
        if kc_value <= 10:
            proficiency_value = "New"
        elif 11 <= kc_value <= 25:
            proficiency_value = "Learner"
        elif 26 <= kc_value <= 100:
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
            has_scythe_bool, proficiency_value, learning_freeze_bool, False, timestamp
        ]
        
        try:
            cell = sang_sheet.find(user_id, in_column=1)

            # --- UPDATED CHECK ---
            if cell is None:
                sang_sheet.append_row(row_data)
            else:
                sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:I{cell.row}') # <-- FIXED

            # --- MODIFIED HISTORY WRITE ---
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}') # <-- FIXED
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
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}') # <-- FIXED
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
            has_scythe_bool, proficiency_value, learning_freeze_bool, False, timestamp
        ]
        
        try:
            cell = sang_sheet.find(user_id, in_column=1)
            if cell is None:
                 sang_sheet.append_row(row_data)
            else:
                 sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:I{cell.row}') # <-- FIXED

            # --- MODIFIED HISTORY WRITE ---
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}') # <-- FIXED
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
                        history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}') # <-- FIXED
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

# --- New Withdrawal Button ---
class WithdrawalButton(ui.Button):
    def __init__(self):
        super().__init__(label="Withdraw", style=ButtonStyle.secondary, custom_id="sang_withdraw", emoji="❌")

    async def callback(self, interaction: discord.Interaction):
        user_id = str(interaction.user.id)
        user_name = interaction.user.display_name

        if not sang_sheet:
            await interaction.response.send_message("⚠️ Error: The Sanguine Signup sheet is not connected.", ephemeral=True)
            return

        try:
            cell = sang_sheet.find(user_id, in_column=1)
            
            if cell is None:
                await interaction.response.send_message(f"ℹ️ {user_name}, you are not currently signed up for this week's event.", ephemeral=True)
                return

            # Delete the row from SangSignups sheet (but leave History alone)
            sang_sheet.delete_rows(cell.row)
            
            await interaction.response.send_message(f"✅ **{user_name}**, you have been successfully withdrawn from this week's Sanguine Sunday signups.", ephemeral=True)
            print(f"✅ User {user_id} ({user_name}) withdrew from SangSignups.")

        except Exception as e:
            print(f"🔥 GSpread error on withdrawal: {e}")
            await interaction.response.send_message("⚠️ An error occurred while processing your withdrawal.", ephemeral=True)


class SignupView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(WithdrawalButton()) # Add the withdrawal button

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
                True, "Mentor", False, False,
                timestamp
            ]

            try:
                cell = sang_sheet.find(user_id, in_column=1)
                if cell is None:
                    sang_sheet.append_row(row_data)
                else:
                    sang_sheet.update(values=[row_data], range_name=f'A{cell.row}:I{cell.row}') # <-- FIXED

                # --- MODIFIED HISTORY WRITE ---
                if history_sheet:
                    try:
                        history_cell = history_sheet.find(user_id, in_column=1)
                        if history_cell is None:
                            history_sheet.append_row(row_data)
                        else:
                            history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}') # <-- FIXED
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
                            history_sheet.update(values=[row_data], range_name=f'A{history_cell.row}:I{history_cell.row}') # <-- FIXED
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
SANG_VC_CATEGORY_ID = 1376645103803830322 # Category for auto-created team voice channels
SANG_POST_CHANNEL_ID = 1338295765759688767 # Default text channel to post teams

def normalize_role(p: dict) -> str:
    """Returns the normalized proficiency string."""
    prof = str(p.get("proficiency","")).strip().lower()
    if prof == "mentor":
        return "mentor"
    try:
        # KC value from the dictionary might be a string (e.g., 'X') or int
        kc = int(p.get("kc") or p.get("KC") or 0)
    except Exception:
        # If it's the 'X' placeholder for auto-mentors, their proficiency is already set above.
        return prof # returns 'highly proficient', 'proficient', etc. if it was calculated before

    # KC ranges from UserSignupForm
    if kc <= 10:
        return "new"
    if 11 <= kc <= 25:
        return "learner"
    if 26 <= kc <= 100:
        return "proficient"
    return "highly proficient"

PROF_ORDER = {"mentor": 0, "highly proficient": 1, "proficient": 2, "learner": 3, "new": 4}

def prof_rank(p: dict) -> int:
    """Returns a numerical rank for sorting."""
    return PROF_ORDER.get(normalize_role(p), 99)

def scythe_icon(p: dict) -> str:
    """Returns Scythe checkmark or cross."""
    return "✅" if p.get("has_scythe") else "❌"

def freeze_icon(p: dict) -> str:
    """Returns Freeze Learner icon or empty string."""
    return "❄️ Learn Freeze" if p.get("learning_freeze") else ""


def is_proficient_plus(p: dict) -> bool:
    """Allowed for trios: Mentor / Highly Proficient / Proficient."""
    role = normalize_role(p)
    return role in ("mentor", "highly proficient", "proficient")

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


# --- New Matchmaking Logic following strict requirements ---

def matchmaking_algorithm(available_raiders: List[Dict[str, Any]]):
    """
    Guarantees:
      - No leftovers (everyone gets placed).
      - Team sizes are 4–5 whenever mathematically possible.
      - Never forms a 6.
      - If N in {6, 7, 11}, allows 3-man team(s) as the only feasible fallback:
          6  -> 3+3
          7  -> 4+3
          11 -> 4+4+3
      - Mentees (wants_mentor=True) are placed on Mentor teams first (if any).
      - Returns (teams, []) for compatibility (no stranded list).
    """
    # ---------- Sort and segment ----------
    available_raiders.sort(
        key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0)))
    )

    mentors = [p for p in available_raiders if normalize_role(p) == "mentor"]
    non_mentors = [p for p in available_raiders if normalize_role(p) != "mentor"]

    strong_pool = [p for p in non_mentors if prof_rank(p) <= PROF_ORDER["proficient"]]   # HP/Pro
    learners    = [p for p in non_mentors if normalize_role(p) == "learner"]
    news        = [p for p in non_mentors if normalize_role(p) == "new"]

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
        # Find nonnegative a,b with 4a + 5b = n
        for b in range(n // 5, -1, -1):
            rem = n - 5*b
            if rem % 4 == 0:
                a = rem // 4
                return [5]*b + [4]*a  # prefer 5s first
        return None

    sizes = split_into_4_5(N)
    if sizes is None:
        if N == 6:
            sizes = [3,3]
        elif N == 7:
            sizes = [4,3]
        elif N == 11:
            sizes = [4,4,3]
        else:
            # Fallback guard (shouldn't trigger for N>=12)
            q, r = divmod(N, 4)
            sizes = [4]*q + ([3] if r == 3 else ([] if r == 0 else [4]))

    T = len(sizes)

    # ---------- Build anchors (Mentors first, then strongest HP/Pro) ----------
    anchors: List[Dict[str, Any]] = []
    if len(mentors) >= T:
        anchors = mentors[:T]
        extra_mentors = mentors[T:]
    else:
        anchors = mentors[:] + strong_pool[: (T - len(mentors))]
        strong_pool = strong_pool[(T - len(mentors)):]
        extra_mentors = []

    # If still short (edge case), backfill from any pool
    for pool in (strong_pool, learners, news, mentees):
        while len(anchors) < T and pool:
            anchors.append(pool.pop(0))

    teams: List[List[Dict[str, Any]]] = [[a] for a in anchors]

    # ---------- Helper for safe placement ----------
    def can_add(player, team, max_size) -> bool:
        if len(team) >= max_size:
            return False

        future_size = len(team) + 1

        # Trio rule: only Proficient+ may be on a 3-man team
        if max_size == 3:
            if not is_proficient_plus(player):
                return False
            if not all(is_proficient_plus(p) for p in team):
                return False

        # Freeze rule: no two freeze learners on same team
        if player.get('learning_freeze') and any(p.get('learning_freeze') for p in team):
            return False

        # Optional guard: avoid placing a New to make a 5 if you want
        if normalize_role(player) == 'new' and future_size == 5:
            return False

        return True

    # Cap per team from sizes plan
    max_sizes = list(sizes)

    # ---------- Place mentees onto Mentor teams first ----------
    mentor_idxs = [i for i, t in enumerate(teams) if normalize_role(t[0]) == "mentor"]
    mentees.sort(key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0))))
    if mentor_idxs and mentees:
        forward = True
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
                break  # mentor teams are full; remaining mentees placed later

    # ---------- One-pass seeding: give each team a New/Learner then a Strong ----------
    for i in range(T):
        if news and can_add(news[0], teams[i], max_sizes[i]):
            teams[i].append(news.pop(0))
        elif learners and can_add(learners[0], teams[i], max_sizes[i]):
            teams[i].append(learners.pop(0))

    for i in range(T):
        if strong_pool and can_add(strong_pool[0], teams[i], max_sizes[i]):
            teams[i].append(strong_pool.pop(0))

    # ---------- Distribute leftovers in snake pattern until all placed ----------
    leftovers = strong_pool + learners + news + mentees + extra_mentors
    forward = True
    safety = 0
    while leftovers and safety < 10000:
        safety += 1
        placed_any = False
        idxs = list(range(T)) if forward else list(range(T-1, -1, -1))
        forward = not forward

        for i in idxs:
            if not leftovers:
                break
            if can_add(leftovers[0], teams[i], max_sizes[i]):
                teams[i].append(leftovers.pop(0))
                placed_any = True

        if not placed_any:
            # Try a simple borrow: move a Proficient+ from a fuller non-trio team to finish a trio
            need_idxs = [i for i in range(T) if max_sizes[i] == 3 and len(teams[i]) < 3]
            borrowed = False
            for ti in need_idxs:
                for dj in range(T):
                    if dj == ti:
                        continue
                    donor_min_keep = 5 if max_sizes[dj] == 5 else (4 if max_sizes[dj] == 4 else 3)
                    if len(teams[dj]) <= donor_min_keep:
                        continue
                    donor = next((p for p in teams[dj] if is_proficient_plus(p)), None)
                    if donor and can_add(donor, teams[ti], max_sizes[ti]):
                        teams[ti].append(donor)
                        teams[dj].remove(donor)
                        borrowed = True
                        placed_any = True
                        break
                if borrowed:
                    break

            if not placed_any:
                # Rotate candidate and continue
                leftovers.append(leftovers.pop(0))

    return teams, []  # no stranded list
    return [], available_raiders
        
    num_teams = len(mentors)
    teams = [[] for _ in range(num_teams)]
    
    # 3. Anchor Assignment: Mentor + New/Learner + HP/Pro Support
    
    # 3a. Assign Mentors (Pass 1)
    # Use a copy of mentors to initialize teams, then remove from source list
    for i in range(num_teams):
        teams[i].append(mentors[i])
    mentors_source = mentors[:] # Keep a clean list of all mentors
    mentors = [] # Empty the source list

    # 3b. Assign New players (Pass 2) - Prioritize placing 1 New player per team
    for i in range(num_teams):
        if news:
            teams[i].append(news.pop(0))
        elif learners:
             # If no new, give the team a learner instead
            teams[i].append(learners.pop(0))

    # 3c. Assign 1 Pro/HP to every Mentor team (Pass 3) - Provide support layer
    # Distribute strongest first (strong_pool is already sorted)
    for i in range(num_teams):
        if strong_pool:
            teams[i].append(strong_pool.pop(0))
    
    # 4. Fill remaining spots with leftovers (HP/Pro, Learners, New)
    
    # Consolidated pool of remaining players, prioritized for filling.
    leftovers = strong_pool + learners + news
    
    # Distribute remaining players round-robin, prioritizing size 4 teams
    current_team_idx = 0
    while leftovers:
        player = leftovers.pop(0)
        
        # Find the smallest team that is under size 5
        # Sort by current size (ascending) to promote balance
        target_indices = sorted(range(num_teams), key=lambda idx: len(teams[idx]))
        placed = False

        for target_idx in target_indices:
            target_team = teams[target_idx]
            if len(target_team) < 5:
                
                # Check 1: No New in 5-man team (Must be size 4 or less)
                is_new_player = normalize_role(player) == 'new'
                if is_new_player and len(target_team) == 4:
                    continue # Skip this team, cannot make size 5 with a New player
                
                # Check 2: No two Freeze Learners together
                freeze_conflict = player.get('learning_freeze') and any(p.get('learning_freeze') for p in target_team)
                if freeze_conflict:
                    continue
                
                # Place player
                target_team.append(player)
                placed = True
                break
        
        if not placed:
            # If player couldn't be placed, put them back and stop filling (will become stranded)
            leftovers.insert(0, player)
            break 
            
    # 5. Final Cleanup and Stranded Logic
    
    # Remaining players are the final leftovers (stranded)
    stranded = leftovers 
    
    # Try to form a single last team of 3 or 4 from stranded players
    if len(stranded) >= 3 and len(stranded) <= 4:
        # Check 3-man team rule (no New or Learner w/out Scythe)
        if len(stranded) == 3:
            has_new = any(normalize_role(p) == 'new' for p in stranded)
            has_unsupported_learner = any(normalize_role(p) == 'learner' and not p.get('has_scythe') for p in stranded)
            
            if not has_new and not has_unsupported_learner:
                teams.append(stranded)
                stranded = []
        # If 4 leftovers, just form a team (as long as it doesn't violate the 3-man rules)
        elif len(stranded) == 4:
             teams.append(stranded)
             stranded = []

    # 6. Final Validation Pass (Filter teams that violate hard rules)
    final_teams = []
    
    for team in teams:
        team_size = len(team)
        
        # Must be size 3 or more to be a valid raiding team
        if team_size < 3:
            # Add small teams to stranded pool for reporting
            stranded.extend(team)
            continue 

        # Rule Check 1: No New in 3s or 5s.
        has_new = any(normalize_role(p) == 'new' for p in team)
        if has_new and team_size in (3, 5):
            stranded.extend(team)
            continue

        # Rule Check 2: No Learner in 3s without Scythe
        has_unsupported_learner = any(normalize_role(p) == 'learner' and not p.get('has_scythe') for p in team)
        if has_unsupported_learner and team_size == 3:
            # This is a bit aggressive but ensures safety: if a learner is in a 3-man without scythe, the team is invalid
            stranded.extend(team)
            continue
            
        # Rule Check 3: No two Freeze Learners together.
        freeze_count = sum(1 for p in team if p.get('learning_freeze'))
        if freeze_count > 1:
            stranded.extend(team)
            continue
            
        final_teams.append(team)
        
    return final_teams, stranded

# --- Matchmaking Slash Command (REWORKED + Highly Proficient) ---
@bot.tree.command(name="sangmatch", description="Create ToB teams from signups in a voice channel.")
@app_commands.checks.has_role(STAFF_ROLE_ID)
@app_commands.describe(voice_channel="Optional: The voice channel to pull users from. If omitted, uses all signups.")
async def sangmatch(interaction: discord.Interaction, voice_channel: Optional[discord.VoiceChannel] = None):
    if not sang_sheet:
        await interaction.response.send_message("⚠️ Error: The Sanguine Sunday sheet is not connected.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False) # Send to channel

    # --- 1. Get users in the specified voice channel ---
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
        
        # If vc_member_ids is set (a VC was provided), filter by it.
        if vc_member_ids and user_id not in vc_member_ids:
             continue # Skip this user, not in the specified VC
        
        roles_str = signup.get("Favorite Roles", "")
        knows_range, knows_melee = parse_roles(roles_str)
        kc_raw = signup.get("KC", 0) # Get KC value, default to 0
        
        try:
            kc_val = int(kc_raw)
        except (ValueError, TypeError):
            kc_val = 9999 if signup.get("Proficiency", "").lower() == 'mentor' else 0

        proficiency_val = signup.get("Proficiency", "").lower()
        if proficiency_val != 'mentor': # Recalculate if not mentor (in case KC changed)
            if kc_val <= 10:
                proficiency_val = "new"
            elif 11 <= kc_val <= 25:
                proficiency_val = "learner"
            elif 26 <= kc_val <= 100:
                proficiency_val = "proficient"
            else: # 100+ KC
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
            "knows_melee": knows_melee,
            "wants_mentor": str(signup.get("Mentor_Request", "FALSE")).upper() == "TRUE"
        })

    if not available_raiders:
        await interaction.followup.send(f"⚠️ None of the users in {voice_channel.mention} have signed up for the event." if voice_channel else "⚠️ No eligible signups.")
        return

    # --- 4. RUN MATCHMAKING ALGORITHM ---
    teams, stranded_players = matchmaking_algorithm(available_raiders)
    
    # --- 5. Format and send output ---
    guild = interaction.guild
    
    # --- Create voice channels under SanguineSunday – Team X pattern ---
    category = guild.get_channel(SANG_VC_CATEGORY_ID)
    if category and hasattr(category, "create_voice_channel"):
        for i in range(len(teams)):
            try:
                await category.create_voice_channel(name=f"SanguineSunday – Team {i+1}", user_limit=5)
            except Exception as e:
                print(f"Error creating VC: {e}") 

    # Determine post channel
    post_channel = interaction.channel

    embed = discord.Embed(
        title=f"Sanguine Sunday Teams - {channel_name}",
        description=f"Created {len(teams)} valid team(s) from {len(available_raiders)} available signed-up users.",
        color=discord.Color.red()
    )

    if not teams:
        embed.description = "Could not form any valid teams with the available players."

    # Sort each team's display Mentor → HP → Pro → Learner → New
    for i, team in enumerate(teams, start=1):
        team_sorted = sorted(team, key=prof_rank)
        
        # Resolve nickname/mention and format
        team_details = []
        for p in team_sorted:
            uid = int(p["user_id"])
            member = guild.get_member(uid)
            mention = member.mention if member else f"<@{uid}>"
            
            role_text = p.get("proficiency", "Unknown").replace(" ", "-").capitalize().replace("-", " ")
            kc_raw = p.get("kc", 0)
            
            # Hide the fake KC for auto-signed mentors (kc=9999)
            kc_text = f"({kc_raw} KC)" if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" and kc_raw != 9999 else ""
            
            scythe = scythe_icon(p)
            freeze = freeze_icon(p)
            
            team_details.append(
                f"{mention} • **{role_text}** {kc_text} • {scythe} Scythe {freeze}"
            )
            
        embed.add_field(name=f"Team {i} (Size: {len(team)})", value="\n".join(team_details) if team_details else "—", inline=False)

    # If a VC was provided, show unassigned
    unassigned_users_in_vc = set()
    if vc_member_ids:
        assigned_ids = {p["user_id"] for t in teams for p in t}
        unassigned_users_in_vc = vc_member_ids - assigned_ids
    
    # Store teams globally for /sangexport
    global last_generated_teams
    last_generated_teams = teams

    await interaction.followup.send(embed=embed)


# --- Plain-text formatter (no mentions) ---
def format_player_line_plain(guild: discord.Guild, p: dict) -> str:
    nickname = p.get("user_name") or "Unknown"
    role_text = p.get("proficiency", "Unknown").replace(" ", "-").capitalize().replace("-", " ")
    kc_raw = p.get("kc", 0)
    
    # Hide the fake KC for auto-signed mentors (kc=9999)
    kc_text = f"({kc_raw} KC)" if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" and kc_raw != 9999 else ""
    
    scythe = scythe_icon(p)
    freeze = freeze_icon(p)
    
    return f"{nickname} • **{role_text}** {kc_text} • {scythe} Scythe {freeze}"

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
            if kc_val <= 10:
                proficiency_val = "new"
            elif 11 <= kc_val <= 25:
                proficiency_val = "learner"
            elif 26 <= kc_val <= 100:
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
            "knows_melee": knows_melee,
            "wants_mentor": str(signup.get("Mentor_Request", "FALSE")).upper() == "TRUE"
        })

    if not available_raiders:
        await interaction.followup.send(f"⚠️ None of the users in {voice_channel.mention} have signed up for the event." if voice_channel else "⚠️ No eligible signups.")
        return

    # --- RUN MATCHMAKING ALGORITHM ---
    teams, stranded_players = matchmaking_algorithm(available_raiders)
    
    # --- Format Output ---
    guild = interaction.guild
    post_channel = channel or interaction.channel
    
    embed = discord.Embed(
        title=f"Sanguine Sunday Teams (Test, no pings/VC) - {channel_name}",
        description=f"Created {len(teams)} valid team(s) from {len(available_raiders)} available signed-up users.",
        color=discord.Color.dark_gray()
    )
    
    for i, team in enumerate(teams, start=1):
        team_sorted = sorted(team, key=prof_rank)
        lines = [format_player_line_plain(guild, p) for p in team_sorted]
        embed.add_field(name=f"Team {i} (Size: {len(team)})", value="\n".join(lines) if lines else "—", inline=False)

    if vc_member_ids:
        assigned_ids = {p["user_id"] for t in teams for p in t}
        unassigned_ids = [uid for uid in vc_member_ids or [] if uid not in assigned_ids]
        if unassigned_ids:
            names = []
            for uid in unassigned_ids:
                m = guild.get_member(int(uid))
                names.append(f"{m.display_name} (VC/No Signup)" if m else f"User {uid} (VC/No Signup)")
            embed.add_field(name="Users in VC but not Signed Up", value="
".join(names), inline=False)

    # Store teams globally for /sangexport
    global last_generated_teams
    last_generated_teams = teams

    await post_channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    await interaction.followup.send("✅ Posted no-ping test teams (no voice channels created).", ephemeral=True)


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
            mid = p.get("user_id") # Use the ID directly from the dict
            id_text = str(mid) if mid is not None else "UnknownID"
            lines.append(f"  - {sname} — ID: {id_text}")
        lines.append("")

    txt = "\n".join(lines)

    export_dir = Path(os.getenv("SANG_EXPORT_DIR", "/mnt/data"))
    try:
        export_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        export_dir = Path("/tmp")
        export_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(CST).strftime("%Y%m%d_%H%M%S")
    outpath = export_dir / f"sanguine_teams_{ts}.txt"
    with open(outpath, "w", encoding="utf-8") as f:
        f.write(txt)

    # send a short message preview and attach the file
    preview = "\n".join(lines[:min(10, len(lines))])
    await interaction.followup.send(
        content=f"📄 Exported teams to **{outpath.name}**:\n```\n{preview}\n```",
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
SANG_SHEET_HEADER = ["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Mentor_Request", "Timestamp"]

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
