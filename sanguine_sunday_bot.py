
import os
import re
import math
import asyncio
from datetime import datetime, time as dt_time
from typing import Optional, List, Dict, Any

import discord
from discord.ext import commands, tasks
from discord import app_commands, ui, ButtonStyle
from discord.ui import View, Button, Modal, TextInput

from oauth2client.service_account import ServiceAccountCredentials
import gspread
import gspread.exceptions
from zoneinfo import ZoneInfo

# ---------------------------
# 🔹 Timezone
# ---------------------------
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
    "private_key": (os.getenv('GOOGLE_PRIVATE_KEY') or "").replace("\\n", "\n"),
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
SANG_SHEET_ID = "1CCpDAJO7Cq581yF_-rz3vx7L_BTettVaKglSvOmvTOE"
SANG_SHEET_TAB_NAME = "SangSignups"
SANG_HISTORY_TAB_NAME = "History"

try:
    sang_google_sheet = sheet_client.open_by_key(SANG_SHEET_ID)
    try:
        sang_sheet = sang_google_sheet.worksheet(SANG_SHEET_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        sang_sheet = sang_google_sheet.add_worksheet(title=SANG_SHEET_TAB_NAME, rows="200", cols="20")
        sang_sheet.append_row(["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Timestamp"])

    try:
        history_sheet = sang_google_sheet.worksheet(SANG_HISTORY_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        history_sheet = sang_google_sheet.add_worksheet(title=SANG_HISTORY_TAB_NAME, rows="1000", cols="20")
        history_sheet.append_row(["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Timestamp"])

except (PermissionError, gspread.exceptions.APIError) as e:
    print("CRITICAL: Missing permission to access Sang Sheet:", e)
    sang_sheet = None
    history_sheet = None
except Exception as e:
    print("Error initializing Sang Sheet:", e)
    sang_sheet = None
    history_sheet = None

# ---------------------------
# 🔹 Discord Bot Setup
# ---------------------------
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.reactions = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# Cache for export
last_generated_teams: List[List[Dict[str, Any]]] = []

# ---------------------------
# 🔹 IDs & Config
# ---------------------------
GUILD_ID = 1272629330115297330
SANG_CHANNEL_ID = 1338295765759688767
STAFF_ROLE_ID = 1272635396991221824
MEMBER_ROLE_ID = 1272633036814946324
MENTOR_ROLE_ID = 1306021911830073414
SANG_ROLE_ID = 1387153629072592916
TOB_ROLE_ID = 1272694636921753701

# VC / posting
SANG_VC_CATEGORY_ID = 1376645103803830322  # Category for auto-created team voice channels
SANG_POST_CHANNEL_ID = 1338295765759688767  # Default text channel to post teams

# ---------------------------
# 🔹 Messages / Copy
# ---------------------------
SANG_MESSAGE_IDENTIFIER = "Sanguine Sunday Sign Up"
SANG_MESSAGE = (
    "# {0} – Hosted by Macflag <:sanguine_sunday:1388100187985154130>\n\n"
    "Looking for a fun Sunday activity? Look no farther than **Sanguine Sunday!**\n"
    "Spend an afternoon or evening sending **Theatre of Blood** runs with clan members.\n"
    "The focus on this event is on **Learners** and general KC.\n\n"
    "We plan to have mentors on hand to help out with the learners.\n"
    "A learner is someone who needs the mechanics explained for each room.\n\n"
    "───────────────────────────────\n"
    "**ToB Learner Resource Hub**\n\n"
    "All Theatre of Blood guides, setups, and related resources are organized here:\n"
    "➤ [**ToB Resource Hub**](https://discord.com/channels/1272629330115297330/1426262876699496598)\n\n"
    "───────────────────────────────\n\n"
    "LEARNERS – please review this thread, watch the xzact guides, and get your plugins set up before Sunday:\n"
    "➤ [**Guides & Plugins**](https://discord.com/channels/1272629330115297330/1388887895837773895)\n\n"
    "No matter if you're a learner or an experienced raider, we strongly encourage you to use one of the setups in this thread:\n\n"
    "⚪ [**Learner Setups**](https://discord.com/channels/1272629330115297330/1426263868950450257)\n"
    "🔵 [**Rancour Meta Setups**](https://discord.com/channels/1272629330115297330/1426272592452391012)\n\n"
    "───────────────────────────────\n"
    "**Sign-Up Here!**\n\n"
    "Click a button below to sign up for the event.\n"
    "- **Raider:** Fill out the form with your KC and gear.\n"
    "- **Mentor:** Fill out the form to sign up as a mentor.\n\n"
    "The form will remember your answers from past events! \n"
    "You only need to edit Kc's and Roles.\n\n"
    "Event link: <https://discord.com/events/1272629330115297330/1386302870646816788>\n\n"
    "||<@&{1}> <@&{2}> <@&{3}>||"
).format(SANG_MESSAGE_IDENTIFIER, MENTOR_ROLE_ID, SANG_ROLE_ID, TOB_ROLE_ID)

LEARNER_REMINDER_IDENTIFIER = "Sanguine Sunday Learner Reminder"
LEARNER_REMINDER_MESSAGE = (
    "# {0} ⏰ <:sanguine_sunday:1388100187985154130>\n\n"
    "This is a reminder for all learners who signed up for Sanguine Sunday!\n\n"
    "Please make sure you have reviewed the following guides and have your gear and plugins ready to go:\n"
    "• **[ToB Resource Hub](https://discord.com/channels/1272629330115297330/1426262876699496598)**\n"
    "• **[Learner Setups](https://discord.com/channels/1272629330115297330/1426263868950450257)**\n"
    "• **[Rancour Meta Setups](https://discord.com/channels/1272629330115297330/1426272592452391012)**\n"
    "• **[Guides & Plugins](https://discord.com/channels/1272629330115297330/1426263621440372768)**\n\n"
    "We look forward to seeing you there!"
).format(LEARNER_REMINDER_IDENTIFIER)

# ---------------------------
# 🔹 Helpers
# ---------------------------
KC_TIERS = {
    "new_max": 10,
    "learner_min": 11, "learner_max": 25,
    "pro_min": 26, "pro_max": 100,
    "hp_min": 101
}

def sanitize_nickname(name: str) -> str:
    """Sanitize spreadsheet nickname: strip things in ( ), drop text after '/',
    and remove special characters !@#$%^&*()/?><'\";:[]{}\\|=+-"""
    if not name:
        return ""
    # Remove text in parentheses (and the parentheses)
    name = re.sub(r"\([^)]*\)", "", str(name))
    # Cut anything after a slash
    name = name.split("/", 1)[0]
    # Remove disallowed special chars
    name = re.sub(r"[!@#$%^&*()\?/><'\";:\[\]{}\|=+\-]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name

def find_member_id_by_sanitized_nickname(guild: discord.Guild, sanitized: str) -> Optional[int]:
    if not guild or not sanitized:
        return None
    s_lower = sanitized.lower()
    for m in guild.members:
        disp = sanitize_nickname(m.display_name).lower()
        if disp == s_lower:
            return m.id
    return None

def normalize_role(p: dict) -> str:
    prof = str(p.get("proficiency", "")).strip().lower()
    if prof == "mentor":
        return "mentor"
    try:
        kc = int(p.get("kc") or p.get("KC") or 0)
    except Exception:
        kc = 0
    if kc <= KC_TIERS["new_max"]:
        return "new"
    if KC_TIERS["learner_min"] <= kc <= KC_TIERS["learner_max"]:
        return "learner"
    if KC_TIERS["pro_min"] <= kc <= KC_TIERS["pro_max"]:
        return "proficient"
    return "highly proficient"

PROF_ORDER = {"mentor": 0, "highly proficient": 1, "proficient": 2, "learner": 3, "new": 4}

def prof_rank(p: dict) -> int:
    return PROF_ORDER.get(normalize_role(p), 99)

def scythe_icon(p: dict) -> str:
    return "✅" if p.get("has_scythe") else "❌"

def freeze_icon(p: dict) -> str:
    return " 🧊" if str(p.get("learning_freeze", "")).lower() in ("true","1","yes") else ""

def format_player_line(guild: discord.Guild, p: dict) -> str:
    """@Nickname • **Role** (KC) • ✅/❌ Scythe • 🧊"""
    uid = int(p.get("user_id", 0)) if str(p.get("user_id","0")).isdigit() else None
    member = guild.get_member(uid) if uid else None
    mention = member.mention if member else ("<@{}>".format(uid) if uid else p.get("user_name","Unknown"))
    role_text = normalize_role(p).replace(" ", "-").capitalize().replace("-", " ")
    kc_raw = p.get("kc", 0)
    kc_text = "({} KC)".format(kc_raw) if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" else ""
    return "{} • **{}** {} • {} Scythe •{}".format(mention, role_text, kc_text, scythe_icon(p), freeze_icon(p))

def format_player_line_plain(guild: discord.Guild, p: dict) -> str:
    """No pings, plain nicknames."""
    uid = int(p.get("user_id", 0)) if str(p.get("user_id","0")).isdigit() else None
    member = guild.get_member(uid) if uid else None
    nickname = sanitize_nickname(p.get("user_name") or (member.display_name if member else "Unknown"))
    role_text = normalize_role(p).replace(" ", "-").capitalize().replace("-", " ")
    kc_raw = p.get("kc", 0)
    kc_text = "({} KC)".format(kc_raw) if isinstance(kc_raw, int) and kc_raw > 0 and role_text != "Mentor" else ""
    return "@{} • **{}** {} • {} Scythe •{}".format(nickname, role_text, kc_text, scythe_icon(p), freeze_icon(p))

def parse_roles(roles_str: str) -> (bool, bool):
    if not roles_str or roles_str == "N/A":
        return (False, False)
    s = roles_str.lower()
    return (any(t in s for t in ["range", "ranger", "rdps"]),
            any(t in s for t in ["melee", "mdps", "meleer"]))

def get_previous_signup(user_id: str) -> Optional[Dict[str, Any]]:
    if not history_sheet:
        return None
    try:
        all_records = history_sheet.get_all_records()
        for record in reversed(all_records):
            if str(record.get("Discord_ID")) == user_id:
                record["Has_Scythe"] = str(record.get("Has_Scythe", "FALSE")).upper() == "TRUE"
                record["Learning Freeze"] = str(record.get("Learning Freeze", "FALSE")).upper() == "TRUE"
                return record
        return None
    except Exception as e:
        print("GS error fetching previous signup:", e)
        return None

# ---------------------------
# 🔹 Views / Forms
# ---------------------------
class UserSignupForm(Modal, title="Sanguine Sunday Signup"):
    roles_known = TextInput(
        label="Favorite Roles (Leave blank if None)",
        placeholder="Inputs: All, Nfrz, Sfrz, Mdps, Rdps",
        style=discord.TextStyle.short,
        max_length=8,
        required=False
    )
    kc = TextInput(
        label="What is your Normal Mode ToB KC?",
        placeholder="0–10 = New, 11–25 = Learner, 26–100 = Proficient, 101+ = Highly Proficient",
        style=discord.TextStyle.short,
        max_length=5,
        required=True
    )
    has_scythe = TextInput(
        label="Do you have a Scythe? (Yes/No)",
        placeholder="Yes or No ONLY",
        style=discord.TextStyle.short,
        max_length=3,
        required=True
    )
    learning_freeze = TextInput(
        label="Do you want to learn freeze role?",
        placeholder="Yes or leave blank",
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
            await interaction.response.send_message("⚠️ Sanguine Sunday sheet is not connected.", ephemeral=True)
            return
        try:
            kc_value = int(str(self.kc))
            if kc_value < 0:
                raise ValueError("KC cannot be negative.")
        except ValueError:
            await interaction.response.send_message("⚠️ Kill Count must be a valid number.", ephemeral=True)
            return

        scythe_value = str(self.has_scythe).strip().lower()
        if scythe_value not in ["yes", "no", "y", "n"]:
            await interaction.response.send_message("⚠️ Scythe must be 'Yes' or 'No'.", ephemeral=True)
            return
        has_scythe_bool = scythe_value in ["yes", "y"]

        if kc_value <= KC_TIERS["new_max"]:
            proficiency_value = "New"
        elif KC_TIERS["learner_min"] <= kc_value <= KC_TIERS["learner_max"]:
            proficiency_value = "Learner"
        elif KC_TIERS["pro_min"] <= kc_value <= KC_TIERS["pro_max"]:
            proficiency_value = "Proficient"
        else:
            proficiency_value = "Highly Proficient"

        roles_known_value = str(self.roles_known).strip() or "None"
        learning_freeze_bool = str(self.learning_freeze).strip().lower() in ["yes", "y"]

        user_id = str(interaction.user.id)
        user_name = interaction.user.display_name
        timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")

        row_data = [user_id, user_name, roles_known_value, kc_value,
                    has_scythe_bool, proficiency_value, learning_freeze_bool, timestamp]

        try:
            cell = sang_sheet.find(user_id, in_column=1)
            if cell is None:
                sang_sheet.append_row(row_data)
            else:
                sang_sheet.update(values=[row_data], range_name='A{}:H{}'.format(cell.row, cell.row))
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name='A{}:H{}'.format(history_cell.row, history_cell.row))
                except Exception as e:
                    print("GS history write error:", e)
        except gspread.CellNotFound:
            sang_sheet.append_row(row_data)
        except Exception as e:
            print("GS signup error:", e)
            await interaction.response.send_message("⚠️ Error saving your signup.", ephemeral=True)
            return

        await interaction.response.send_message(
            "✅ You are signed up as {}!\n**KC:** {}\n**Scythe:** {}\n**Favorite Roles:** {}\n**Learn Freeze:** {}".format(
                proficiency_value, kc_value, "Yes" if has_scythe_bool else "No", roles_known_value,
                "Yes" if learning_freeze_bool else "No"
            ),
            ephemeral=True
        )

class MentorSignupForm(Modal, title="Sanguine Sunday Mentor Signup"):
    roles_known = TextInput(
        label="Favorite Roles (Leave blank if None)",
        placeholder="Inputs: All, Nfrz, Sfrz, Mdps, Rdps",
        style=discord.TextStyle.short,
        max_length=8,
        required=True
    )
    kc = TextInput(
        label="What is your Normal Mode ToB KC?",
        placeholder="101+ recommended",
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

    def __init__(self, previous_data: dict = None):
        super().__init__(title="Sanguine Sunday Mentor Signup")
        if previous_data:
            self.roles_known.default = previous_data.get("Favorite Roles", "")
            kc_val = previous_data.get("KC", "")
            self.kc.default = str(kc_val) if kc_val not in ["", None, "X"] else ""
            self.has_scythe.default = "Yes" if previous_data.get("Has_Scythe", False) else "No"

    async def on_submit(self, interaction: discord.Interaction):
        if not sang_sheet:
            await interaction.response.send_message("⚠️ Sanguine Sunday sheet is not connected.", ephemeral=True); return
        try:
            kc_value = int(str(self.kc))
        except ValueError:
            await interaction.response.send_message("⚠️ Kill Count must be a valid number.", ephemeral=True); return

        scythe_value = str(self.has_scythe).strip().lower()
        if scythe_value not in ["yes", "no", "y", "n"]:
            await interaction.response.send_message("⚠️ Scythe must be 'Yes' or 'No'.", ephemeral=True); return
        has_scythe_bool = scythe_value in ["yes", "y"]

        proficiency_value = "Mentor"
        roles_known_value = str(self.roles_known).strip()
        learning_freeze_bool = False

        user_id = str(interaction.user.id)
        user_name = interaction.user.display_name
        timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")

        row_data = [user_id, user_name, roles_known_value, kc_value,
                    has_scythe_bool, proficiency_value, learning_freeze_bool, timestamp]

        try:
            cell = sang_sheet.find(user_id, in_column=1)
            if cell is None:
                sang_sheet.append_row(row_data)
            else:
                sang_sheet.update(values=[row_data], range_name='A{}:H{}'.format(cell.row, cell.row))
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name='A{}:H{}'.format(history_cell.row, history_cell.row))
                except Exception as e:
                    print("GS history write error:", e)
        except gspread.CellNotFound:
            sang_sheet.append_row(row_data)
        except Exception as e:
            print("GS mentor signup error:", e)
            await interaction.response.send_message("⚠️ Error saving your signup.", ephemeral=True); return

        await interaction.response.send_message(
            "✅ You are signed up as a Mentor!\n**KC:** {}\n**Scythe:** {}\n**Favorite Roles:** {}".format(
                kc_value, "Yes" if has_scythe_bool else "No", roles_known_value
            ),
            ephemeral=True
        )

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

        # Auto-mentor quick add
        await interaction.response.defer(ephemeral=True)
        if not sang_sheet or not history_sheet:
            await interaction.followup.send("⚠️ The Sanguine Sunday signup or history sheet is not connected.", ephemeral=True)
            return

        user_id = str(user.id)
        user_name = member.display_name
        timestamp = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
        row_data = [user_id, user_name, "All", "X", True, "Mentor", False, timestamp]

        try:
            cell = sang_sheet.find(user_id, in_column=1)
            if cell is None:
                sang_sheet.append_row(row_data)
            else:
                sang_sheet.update(values=[row_data], range_name='A{}:H{}'.format(cell.row, cell.row))
            if history_sheet:
                try:
                    history_cell = history_sheet.find(user_id, in_column=1)
                    if history_cell is None:
                        history_sheet.append_row(row_data)
                    else:
                        history_sheet.update(values=[row_data], range_name='A{}:H{}'.format(history_cell.row, history_cell.row))
                except Exception as e:
                    print("GS history write error:", e)
            await interaction.followup.send("✅ Auto-signed up as Mentor! If this is incorrect, click the button again to fill out the form.", ephemeral=True)
        except gspread.CellNotFound:
            sang_sheet.append_row(row_data)
            await interaction.followup.send("✅ Auto-signed up as Mentor! If this is incorrect, click the button again to fill out the form.", ephemeral=True)
        except Exception as e:
            print("GS auto mentor error:", e)
            await interaction.followup.send("⚠️ An error occurred while auto-signing you up.", ephemeral=True)

# ---------------------------
# 🔹 Posting helpers
# ---------------------------
async def post_signup(channel: discord.TextChannel):
    await channel.send(SANG_MESSAGE, view=SignupView())

async def post_reminder(channel: discord.TextChannel):
    if not sang_sheet:
        return False
    try:
        # cleanup old reminders
        async for message in channel.history(limit=50):
            if message.author == bot.user and LEARNER_REMINDER_IDENTIFIER in message.content:
                await message.delete()
    except Exception:
        pass

    learners = []
    try:
        for signup in sang_sheet.get_all_records():
            prof = str(signup.get("Proficiency", "")).lower()
            if prof in ["learner", "new"]:
                uid = signup.get("Discord_ID")
                if uid:
                    learners.append("<@{}>".format(uid))
        content = "{}\n\n{}".format(LEARNER_REMINDER_MESSAGE, "**Learners:** " + " ".join(learners) if learners else "_No learners have signed up yet._")
        await channel.send(content, allowed_mentions=discord.AllowedMentions(users=True))
        return True
    except Exception as e:
        print("GS reminder error:", e)
        await channel.send("⚠️ Error processing learner list from database.")
        return False

# ---------------------------
# 🔹 Slash commands: /sangsignup
# ---------------------------
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
        await interaction.response.send_message("⚠️ Could not find the target channel.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    if variant == 1:
        await post_signup(target_channel)
        await interaction.followup.send("✅ Signup message posted in {}.".format(target_channel.mention))
    elif variant == 2:
        ok = await post_reminder(target_channel)
        await interaction.followup.send("✅ Learner reminder posted in {}.".format(target_channel.mention) if ok else "⚠️ Could not post the reminder.")

# ---------------------------
# 🔹 Matchmaking Core & Constraints
# ---------------------------
def enforce_constraints(teams: List[List[Dict[str, Any]]]):
    """Apply post-process rules:
       - New only in teams of 4, with Mentor + Pro/HP
       - No New in 3 or 5
       - Learner in 3 only if scythe
       - Learner in 5 only if no New
       - Freeze learners separated
       - Minimum team size 4 (merge/split to reach 4 where possible)
    """
    def role_of(p): return normalize_role(p)
    def is_new(p): return role_of(p) == "new"
    def is_learner(p): return role_of(p) == "learner"
    def is_pro(p): return role_of(p) in ("proficient", "highly proficient")
    def is_hp(p): return role_of(p) == "highly proficient"
    def is_mentor(p): return role_of(p) == "mentor"
    def has_scythe(p): return bool(p.get("has_scythe"))
    def is_freeze_learner(p): return str(p.get("learning_freeze")).lower() in ("true","1","yes")

    def count(pred, team): return sum(1 for x in team if pred(x))
    def has(pred, team): return any(pred(x) for x in team)

    # Ensure each Mentor team has at least one Pro/HP
    for i, t in enumerate(teams):
        if has(is_mentor, t) and not has(is_pro, t):
            for j, u in enumerate(teams):
                if i == j: continue
                idx = next((k for k, x in enumerate(u) if is_pro(x)), None)
                if idx is not None:
                    t.append(u.pop(idx)); break

    # New-player rules
    for i, t in enumerate(list(teams)):
        if count(is_new, t) > 0:
            # remove from 3/5
            while len(t) in (3, 5) and count(is_new, t) > 0:
                idx = next(k for k, x in enumerate(t) if is_new(x))
                moved = t.pop(idx)
                placed = False
                for j, u in enumerate(teams):
                    if i == j: continue
                    if len(u) == 4 and has(is_mentor, u) and has(is_pro, u) and count(is_new, u) < 2:
                        u.append(moved); placed = True; break
                if not placed:
                    teams.append([moved]); t = teams[i]
            # ensure mentor + pro present
            if not has(is_mentor, t):
                for j, u in enumerate(teams):
                    if i == j: continue
                    midx = next((k for k, x in enumerate(u) if is_mentor(x)), None)
                    if midx is not None:
                        t.append(u.pop(midx)); break
            if not has(is_pro, t):
                for j, u in enumerate(teams):
                    if i == j: continue
                    prx = next((k for k, x in enumerate(u) if is_pro(x)), None)
                    if prx is not None:
                        t.append(u.pop(prx)); break
            # size exactly 4 if contains new
            while count(is_new, t) > 0 and len(t) != 4:
                if len(t) > 4:
                    idx = next((k for k, x in enumerate(t) if not is_new(x)), None)
                    if idx is not None:
                        # move out to other team <5 without new
                        moved = t.pop(idx)
                        placed = False
                        for j, u in enumerate(teams):
                            if i == j: continue
                            if len(u) < 5 and count(is_new, u) == 0:
                                u.append(moved); placed = True; break
                        if not placed:
                            teams.append([moved])
                    else:
                        break
                else:  # len < 4
                    pulled = False
                    for j, u in enumerate(teams):
                        if i == j: continue
                        prx = next((k for k, x in enumerate(u) if is_pro(x)), None)
                        if prx is not None:
                            t.append(u.pop(prx)); pulled = True; break
                    if not pulled:
                        break

    # Learner constraints
    for i, t in enumerate(teams):
        if len(t) == 3 and any(is_learner(x) and not has_scythe(x) for x in t):
            idx = next(k for k, x in enumerate(t) if is_learner(x) and not has_scythe(x))
            moved = t.pop(idx)
            placed = False
            for j, u in enumerate(teams):
                if i == j: continue
                if len(u) == 4 and count(is_new, u) == 0:
                    u.append(moved); placed = True; break
            if not placed:
                teams.append([moved])
        if len(t) == 5 and count(is_new, t) > 0 and any(is_learner(x) for x in t):
            # move a new out first
            idx = next((k for k, x in enumerate(t) if is_new(x)), None)
            if idx is None:
                idx = next((k for k, x in enumerate(t) if is_learner(x)), 0)
            moved = t.pop(idx)
            placed = False
            for j, u in enumerate(teams):
                if i == j: continue
                if len(u) < 5 and count(is_new, u) == 0:
                    u.append(moved); placed = True; break
            if not placed:
                teams.append([moved])

    # Freeze learners separated
    for i, t in enumerate(teams):
        while sum(1 for x in t if is_freeze_learner(x)) > 1:
            idx = next(k for k, x in enumerate(t) if is_freeze_learner(x))
            moved = t.pop(idx)
            placed = False
            for j, u in enumerate(teams):
                if i == j: continue
                if sum(1 for x in u if is_freeze_learner(x)) == 0 and len(u) < 5:
                    u.append(moved); placed = True; break
            if not placed:
                teams.append([moved])

    # Minimum team size 4: try to rebalance
    changed = True
    while changed:
        changed = False
        small_idxs = [i for i, t in enumerate(teams) if len(t) < 4 and len(t) > 0]
        big_idxs = [i for i, t in enumerate(teams) if len(t) > 4]
        for si in small_idxs:
            for bi in big_idxs:
                if len(teams[si]) >= 4:
                    break
                # move a non-new from big to small
                idx = next((k for k, x in enumerate(teams[bi]) if normalize_role(x) != "new"), None)
                if idx is not None:
                    teams[si].append(teams[bi].pop(idx)); changed = True

# ---------------------------
# 🔹 /sangmatch (creates VCs, pings)
# ---------------------------
@bot.tree.command(name="sangmatch", description="Create ToB teams from signups in a voice channel.")
@app_commands.checks.has_role(STAFF_ROLE_ID)
@app_commands.describe(voice_channel="Optional: The voice channel to pull users from. If omitted, uses all signups.")
async def sangmatch(interaction: discord.Interaction, voice_channel: Optional[discord.VoiceChannel] = None):
    if not sang_sheet:
        await interaction.response.send_message("⚠️ The Sanguine Sunday sheet is not connected.", ephemeral=True); return

    await interaction.response.defer(ephemeral=False)
    vc_member_ids = None
    channel_name = "All Signups"

    if voice_channel:
        channel_name = voice_channel.name
        vc_member_ids = {str(m.id) for m in voice_channel.members if not m.bot}
        if not vc_member_ids:
            await interaction.followup.send("⚠️ No human users are in {}.".format(voice_channel.mention)); return

    # Fetch signups
    try:
        all_signups_records = sang_sheet.get_all_records()
        if not all_signups_records:
            await interaction.followup.send("⚠️ There are no signups in the database."); return
    except Exception as e:
        print("GSheet fetch error:", e)
        await interaction.followup.send("⚠️ An error occurred fetching signups from the database."); return

    available = []
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
            kc_val = 9999 if str(signup.get("Proficiency","")).lower() == "mentor" else 0

        prof_val = str(signup.get("Proficiency","")).lower()
        if prof_val != "mentor":
            if kc_val <= KC_TIERS["new_max"]:
                prof_val = "new"
            elif KC_TIERS["learner_min"] <= kc_val <= KC_TIERS["learner_max"]:
                prof_val = "learner"
            elif KC_TIERS["pro_min"] <= kc_val <= KC_TIERS["pro_max"]:
                prof_val = "proficient"
            else:
                prof_val = "highly proficient"

        available.append({
            "user_id": user_id,
            "user_name": signup.get("Discord_Name"),
            "proficiency": prof_val,
            "kc": kc_val,
            "has_scythe": str(signup.get("Has_Scythe","FALSE")).upper() == "TRUE",
            "roles_known": roles_str,
            "learning_freeze": str(signup.get("Learning Freeze","FALSE")).upper() == "TRUE",
            "knows_range": knows_range,
            "knows_melee": knows_melee
        })

    if not available:
        await interaction.followup.send("⚠️ None of the users in the selected VC have signed up." if voice_channel else "⚠️ No eligible signups."); return

    # Seed teams based on mentors / strong
    available.sort(key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0))))
    mentors = [p for p in available if normalize_role(p) == "mentor"]
    high_pro = [p for p in available if normalize_role(p) == "highly proficient"]
    pro = [p for p in available if normalize_role(p) == "proficient"]
    learners = [p for p in available if normalize_role(p) == "learner"]
    news = [p for p in available if normalize_role(p) == "new"]

    total = len(available)
    ideal_size = 5
    min_teams_by_size = math.ceil(total / ideal_size) if total else 0
    num_teams = max(1, min(len(mentors) if mentors else min_teams_by_size, min_teams_by_size))
    if len(mentors) < num_teams:
        num_teams = max(len(mentors), 1)

    teams = [[] for _ in range(num_teams)]
    for i, mtr in enumerate(mentors[:num_teams]):
        teams[i].append(mtr)

    strong = high_pro + pro

    def pop_with_scythe(prefer_scythe=True):
        idx = next((i for i, p in enumerate(strong) if p.get("has_scythe")), None) if prefer_scythe else None
        if idx is None: return strong.pop(0) if strong else None
        return strong.pop(idx)

    for i in range(num_teams):
        if not strong: break
        prefer_s = not any(m.get("has_scythe") for m in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick: teams[i].append(pick)

    for i in range(num_teams):
        if not strong: break
        prefer_s = not any(m.get("has_scythe") for m in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick: teams[i].append(pick)

    def add_bucket(bucket):
        i = 0
        while bucket:
            placed = False
            for _ in range(num_teams):
                t = teams[i]
                if len(t) < ideal_size:
                    has_str = any(prof_rank(p) <= PROF_ORDER["proficient"] for p in t)
                    if has_str or bucket is learners:
                        t.append(bucket.pop(0)); placed = True; break
                i = (i + 1) % num_teams
            if not placed: break

    add_bucket(learners)
    add_bucket(news)

    while strong:
        placed = False
        for i in range(num_teams):
            if len(teams[i]) < ideal_size:
                teams[i].append(strong.pop(0)); placed = True
                if not strong: break
        if not placed: break

    leftover_pool = [p for p in available if p not in [m for tm in teams for m in tm]]
    while leftover_pool:
        spill = leftover_pool[:4]
        leftover_pool = leftover_pool[4:]
        teams.append(spill)

    enforce_constraints(teams)

    # Save for export
    global last_generated_teams
    last_generated_teams = teams

    guild = interaction.guild
    # Create voice channels
    category = guild.get_channel(SANG_VC_CATEGORY_ID)
    if category and hasattr(category, "create_voice_channel"):
        for i in range(len(teams)):
            try:
                await category.create_voice_channel(name="SanguineSunday – Team {}".format(i+1))
            except Exception:
                pass

    post_channel = guild.get_channel(SANG_POST_CHANNEL_ID) or interaction.channel
    embed = discord.Embed(
        title="Sanguine Sunday Teams - {}".format(channel_name),
        description="Created {} team(s) from {} available signed-up users.".format(len(teams), len(available)),
        color=discord.Color.red()
    )

    for i, team in enumerate(teams, start=1):
        team_sorted = sorted(team, key=prof_rank)
        lines = [format_player_line(guild, p) for p in team_sorted]
        embed.add_field(name="Team {}".format(i), value="\n".join(lines) if lines else "—", inline=False)

    await post_channel.send(embed=embed, allowed_mentions=discord.AllowedMentions(users=True))

# ---------------------------
# 🔹 /sangmatchtest (no pings, no VCs)
# ---------------------------
@bot.tree.command(name="sangmatchtest", description="Create ToB teams without pinging or creating voice channels; plain-text nicknames.")
@app_commands.checks.has_role(STAFF_ROLE_ID)
@app_commands.describe(
    voice_channel="Optional: The voice channel to pull users from. If omitted, uses all signups.",
    channel="(Optional) Override the text channel to post teams (testing)."
)
async def sangmatchtest(interaction: discord.Interaction, voice_channel: Optional[discord.VoiceChannel] = None, channel: Optional[discord.TextChannel] = None):
    if not sang_sheet:
        await interaction.response.send_message("⚠️ The Sanguine Sunday sheet is not connected.", ephemeral=True); return

    await interaction.response.defer(ephemeral=False)
    vc_member_ids = None
    channel_name = "All Signups"

    if voice_channel:
        channel_name = voice_channel.name
        vc_member_ids = {str(m.id) for m in voice_channel.members if not m.bot}
        if not vc_member_ids:
            await interaction.followup.send("⚠️ No human users are in {}.".format(voice_channel.mention)); return

    try:
        all_signups_records = sang_sheet.get_all_records()
        if not all_signups_records:
            await interaction.followup.send("⚠️ There are no signups in the database."); return
    except Exception as e:
        print("GSheet fetch error:", e)
        await interaction.followup.send("⚠️ An error occurred fetching signups from the database."); return

    available = []
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
            kc_val = 9999 if str(signup.get("Proficiency","")).lower() == "mentor" else 0

        prof_val = str(signup.get("Proficiency","")).lower()
        if prof_val != "mentor":
            if kc_val <= KC_TIERS["new_max"]:
                prof_val = "new"
            elif KC_TIERS["learner_min"] <= kc_val <= KC_TIERS["learner_max"]:
                prof_val = "learner"
            elif KC_TIERS["pro_min"] <= kc_val <= KC_TIERS["pro_max"]:
                prof_val = "proficient"
            else:
                prof_val = "highly proficient"

        available.append({
            "user_id": user_id,
            "user_name": sanitize_nickname(signup.get("Discord_Name")),
            "proficiency": prof_val,
            "kc": kc_val,
            "has_scythe": str(signup.get("Has_Scythe","FALSE")).upper() == "TRUE",
            "roles_known": roles_str,
            "learning_freeze": str(signup.get("Learning Freeze","FALSE")).upper() == "TRUE",
            "knows_range": knows_range,
            "knows_melee": knows_melee
        })

    if not available:
        await interaction.followup.send("⚠️ None of the users in the selected VC have signed up." if voice_channel else "⚠️ No eligible signups."); return

    available.sort(key=lambda p: (prof_rank(p), not p.get("has_scythe"), -int(p.get("kc", 0))))
    mentors = [p for p in available if normalize_role(p) == "mentor"]
    high_pro = [p for p in available if normalize_role(p) == "highly proficient"]
    pro = [p for p in available if normalize_role(p) == "proficient"]
    learners = [p for p in available if normalize_role(p) == "learner"]
    news = [p for p in available if normalize_role(p) == "new"]

    total = len(available)
    ideal_size = 5
    min_teams_by_size = math.ceil(total / ideal_size) if total else 0
    num_teams = max(1, min(len(mentors) if mentors else min_teams_by_size, min_teams_by_size))
    if len(mentors) < num_teams:
        num_teams = max(len(mentors), 1)

    teams = [[] for _ in range(num_teams)]
    for i, mtr in enumerate(mentors[:num_teams]):
        teams[i].append(mtr)

    strong = high_pro + pro

    def pop_with_scythe(prefer_scythe=True):
        idx = next((i for i, p in enumerate(strong) if p.get("has_scythe")), None) if prefer_scythe else None
        if idx is None: return strong.pop(0) if strong else None
        return strong.pop(idx)

    for i in range(num_teams):
        if not strong: break
        prefer_s = not any(m.get("has_scythe") for m in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick: teams[i].append(pick)

    for i in range(num_teams):
        if not strong: break
        prefer_s = not any(m.get("has_scythe") for m in teams[i])
        pick = pop_with_scythe(prefer_s)
        if pick: teams[i].append(pick)

    def add_bucket(bucket):
        i = 0
        while bucket:
            placed = False
            for _ in range(num_teams):
                t = teams[i]
                if len(t) < ideal_size:
                    has_str = any(prof_rank(p) <= PROF_ORDER["proficient"] for p in t)
                    if has_str or bucket is learners:
                        t.append(bucket.pop(0)); placed = True; break
                i = (i + 1) % num_teams
            if not placed: break

    add_bucket(learners)
    add_bucket(news)

    while strong:
        placed = False
        for i in range(num_teams):
            if len(teams[i]) < ideal_size:
                teams[i].append(strong.pop(0)); placed = True
                if not strong: break
        if not placed: break

    leftover_pool = [p for p in available if p not in [m for tm in teams for m in tm]]
    while leftover_pool:
        spill = leftover_pool[:4]
        leftover_pool = leftover_pool[4:]
        teams.append(spill)

    enforce_constraints(teams)

    global last_generated_teams
    last_generated_teams = teams

    guild = interaction.guild
    post_channel = channel or guild.get_channel(SANG_POST_CHANNEL_ID) or interaction.channel
    embed = discord.Embed(
        title="Sanguine Sunday Teams (Test, no pings/VC) - {}".format(channel_name),
        description="Created {} team(s) from {} available signed-up users.".format(len(teams), len(available)),
        color=discord.Color.dark_gray()
    )
    for i, team in enumerate(teams, start=1):
        team_sorted = sorted(team, key=prof_rank)
        lines = [format_player_line_plain(guild, p) for p in team_sorted]
        embed.add_field(name="Team {}".format(i), value="\n".join(lines) if lines else "—", inline=False)

    await post_channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

# ---------------------------
# 🔹 /sangexport (preview + attachment)
# ---------------------------
from pathlib import Path
@bot.tree.command(name="sangexport", description="Export the most recently generated teams to a text file.")
@app_commands.checks.has_any_role("Administrators", "Clan Staff", "Senior Staff", "Staff", "Trial Staff")
async def sangexport(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    teams = last_generated_teams if last_generated_teams else None
    if not teams:
        await interaction.followup.send("⚠️ No teams found from this session.", ephemeral=True); return

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
        lines.append("Team {}".format(i))
        for p in team:
            sname = sanitize_nickname(p.get("user_name", "Unknown"))
            mid = resolve_discord_id(p)
            id_text = str(mid) if mid is not None else "UnknownID"
            lines.append("  - {} — ID: {}".format(sname, id_text))
        lines.append("")

    txt = "\n".join(lines)

    export_dir = Path(os.getenv("SANG_EXPORT_DIR", "/mnt/data"))
    try:
        export_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        export_dir = Path("/tmp")
        export_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(CST).strftime("%Y%m%d_%H%M%S")
    outpath = export_dir / "sanguine_teams_{}.txt".format(ts)
    with open(outpath, "w", encoding="utf-8") as f:
        f.write(txt)

    preview = "\n".join(lines[:min(12, len(lines))])
    await interaction.followup.send(
        content="📄 Exported teams to **{}**:\n```\n{}\n```".format(outpath.name, preview),
        file=discord.File(str(outpath), filename=outpath.name),
        ephemeral=True
    )

# ---------------------------
# 🔹 /sangcleanup (delete created VCs)
# ---------------------------
@bot.tree.command(name="sangcleanup", description="Delete auto-created SanguineSunday voice channels from the last run.")
@app_commands.checks.has_any_role("Administrators", "Clan Staff", "Senior Staff", "Staff", "Trial Staff")
async def sangcleanup(interaction: discord.Interaction):
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
            pass
    await interaction.followup.send("🧹 Deleted {} voice channels.".format(deleted), ephemeral=True)

# ---------------------------
# 🔹 Scheduled Tasks
# ---------------------------
SANG_SHEET_HEADER = ["Discord_ID", "Discord_Name", "Favorite Roles", "KC", "Has_Scythe", "Proficiency", "Learning Freeze", "Timestamp"]

@tasks.loop(time=dt_time(hour=11, minute=0, tzinfo=CST))
async def scheduled_post_signup():
    if datetime.now(CST).weekday() == 4:  # Friday
        channel = bot.get_channel(SANG_CHANNEL_ID)
        if channel:
            await post_signup(channel)

@tasks.loop(time=dt_time(hour=14, minute=0, tzinfo=CST))
async def scheduled_post_reminder():
    if datetime.now(CST).weekday() == 5:  # Saturday
        channel = bot.get_channel(SANG_CHANNEL_ID)
        if channel:
            await post_reminder(channel)

@tasks.loop(time=dt_time(hour=4, minute=0, tzinfo=CST))
async def scheduled_clear_sang_sheet():
    if datetime.now(CST).weekday() == 0:  # Monday
        if sang_sheet:
            try:
                sang_sheet.clear()
                sang_sheet.append_row(SANG_SHEET_HEADER)
                print("SangSignups sheet cleared and headers added.")
            except Exception as e:
                print("Failed to clear SangSignups sheet:", e)

@scheduled_post_signup.before_loop
@scheduled_post_reminder.before_loop
@scheduled_clear_sang_sheet.before_loop
async def before_scheduled_tasks():
    await bot.wait_until_ready()

# ---------------------------
# 🔹 on_ready
# ---------------------------
@bot.event
async def on_ready():
    print("✅ Logged in as {} (ID: {})".format(bot.user, bot.user.id))
    bot.add_view(SignupView())
    if not scheduled_post_signup.is_running():
        scheduled_post_signup.start()
    if not scheduled_post_reminder.is_running():
        scheduled_post_reminder.start()
    if not scheduled_clear_sang_sheet.is_running():
        scheduled_clear_sang_sheet.start()
    try:
        synced = await bot.tree.sync()
        print("✅ Synced {} commands.".format(len(synced)))
    except Exception as e:
        print("❌ Command sync failed:", e)

# ---------------------------
# 🔹 Run
# ---------------------------
bot.run(os.getenv('DISCORD_BOT_TOKEN'))
