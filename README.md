This is a vibe coded ham clock.  Pretty much nothing in here was coded by me.  I do want to give a shout out to https://uninformativ.de/git/asciiworld as I had OpenAI convert his C code to python and integrated it in to my program.  I'm just using the shapefile that he provided in his git.

You wil need a couple of python libraries, pillow, pyshp and ncurses for sure.  I think modern 3+ python comes with the rest already installed.  It will warn you if you are missing anything.

Settings live in hamclock_settings.json next to hamclock.py.  That file is not in git because it holds your passwords.  On the first run with no settings file the Settings screen opens so you can fill it in, or you can copy hamclock_settings.example.json to hamclock_settings.json and edit it.  The main ones:

grid_square: This is your grid square.  With out it (or latitude/longitude), hamclock can't work out where you are and get weather.
latitude_decimal/longitude_decimal: Optional, in decimal notation.  It will convert your grid square if these are blank.
wx_location_name: It will display this in the local weather panel if it can't figure out where you are.
time_zone: IANA name like America/Edmonton.  The "Local" clock uses this; leave it blank to use the Pi's own time zone.
space_weather_refresh_sec / local_weather_refresh_sec: How often to fetch space weather and local weather.  Default 1800 (30 minutes).
world_map_refresh_sec: How often to update the world map.
time_refresh_seconds: Default is one second so it makes the clock tick.
dx_cluster_host/port/username/password, dx_filter: Your DX cluster login.  Set from Esc > DX Cluster.
enable_logging: Debug log to hamclock.log.  Off by default because it gets huge.

Press ? (or F1) for a help screen listing every key.  The hint is shown in the bottom border of the map panel.  Pressing ESC will bring up the menu (Settings, Rig Control, DX Cluster, Local Database, Shortwave, Online lookup, Look up call sign, Quit).  You can also just press "q" while it's running to quit.

If HTTPS fetches fail with certificate errors, fix the Pi's certificates with "sudo apt install --reinstall ca-certificates".  As a last resort MYHAMCLOCK_INSECURE_SSL=1 turns off certificate checks.

Have fun with it.

April 2/2025 changes

Remember this thing is 99% vibe coded using ChatGPT.  While I can program python, it's not my strongest language.  I've been using this to help me see how python works along with ncurses.  I'll be uploading a sample hamlib.sqlite database that has about 100 rows in it.

- Added the ability to look up callsigns from hamdb.org, hamqth.com (with login) and qrz.com (with login & membership).  This will update the hamcall.sqlite if it's not already in there.
- Added a menu item to select which online lookup to use with user name and password.
- Added menu item to do a one off lookup.  It checks the hamcall.sqlite database first then will check online (and update database if needed).
- Shrunk the world map panel and used the left side to show the DE and DX info that is coming in through the DX Cluster.  The DE/DX will show what database it found the callsign data in.
- Added status bar for online lookups.  Needed this for debugging mostly.
- Added some debug logging.  This can be turned on or off in the settings menu.  Leaving it on will create a huge text file as it dumps a lot of info.
- Removed the markers for DE/DX on the world map.  I'm not sure how I want to implement this as it could get very cluttered and it doesn't always render the X and Y characters properly.

April 5/2026 changes
- Added simple log book.  It will create the tables if missing from mysql or sqlite.  Pretty simple.  When you type in the call sign it will grab their information from the tables/qrz/hamdb/hamlib as needed.  If the call sign is missing from the local lookup table it will insert it from the online one.  Pressing "L" will replace the world map with the log book.  It shows the last 10 logged contacts.
- Added radio control.  This uses Rigctld only at this time, so you will need to have that working first.  It gets info from the radio and displays it.  It will also populate the frequency in the log book and mode (USB, LSB, CW, RTTY)
- When world map is showing you can use the arrow keys to go up and down the DX cluster and when you hit Enter it will change the radio to that frequency and mode.  It defaults band width to 3000khz for SSB, 1200khz for CW.
- Added F1 help that shows what all the keys do.
- Remember this is 99.99% vibe coded so if things don't make sense in the code it's not my fault :)

April 6/2025 changes
- Added shortwave.  When at the main screen if you hit "S" it changes the world map to the shortwave database.  You will need to download the latest csv file from http://www.eibispace.de/.  You can filter the list by language in the json file.  See the README.txt file at the same website to see the abbrviations for languages.  You can set the highlight colour for the target location to make it easier to find stations broadcasting to your location.  Since the csv file has a lot more then just shortwave you can toggle on/off to show only those in the shortwave bands.
  "shortwave_languages": "E",
  "shortwave_highlight_target": "North America",
  "shortwave_highlight_color": "cyan",
  "shortwave_broadcast_bands_only": true
- You will also need the two new .json files that convert languages and locations.

November 24/2026 changes (bug fixes and cleanup)
- HamQTH lookups work now.  Two helper functions they needed were missing.
- Fixed a crash when opening the Station Callsigns menu.
- Saving the main Settings no longer resets your shortwave languages to "E".
- The time_zone setting is used for the Local clock.
- Space weather, local weather, radio polling and DE/DX callsign lookups run in the background, so the clock keeps ticking if a website or rigctld is slow.  Closing a menu no longer re-downloads everything.
- Local weather refreshes on its own timer instead of waiting for space weather.
- Radio panel reads COMP, NB, NR, pre-amp and AGC from rigctld.  Shows "-" for anything your radio doesn't report.
- DX cluster reconnects by itself after you save new DX Cluster settings, and says "not configured" when there's no host.
- DX cluster login is now stored once (dx_cluster_*).  Old dx_host/dx_user/dx_pass/dx_port values are moved over automatically.
- HTTPS no longer silently turns off certificate checks when verification fails.
- Debug logging is off by default.
- hamclock_settings.json is no longer tracked in git.  Use hamclock_settings.example.json as a starting point.
- Removed duplicate and dead code.
- "? or F1 = Help  Esc = Menu" hint on the map panel border.  ? opens help too, and F1 now works from PuTTY's default keyboard setting.
- When rig control is off the Radio box is hidden and the DE / DX panel uses its space.  The DE / DX panel switches to a compact layout when it's short on room, so both DE and DX fit on a 24-line terminal.
- Esc in the Rig Control dialog now cancels changes instead of keeping them.
- Station Callsigns menu (C in Logbook mode) has Edit and Delete.  Editing a callsign also updates the QSOs logged under it.  Deleting one with QSOs hides it from the list and keeps the QSOs.
- New callsigns must look like a callsign (letters, numbers and /).
- Text boxes: Esc cancels instead of typing ^[, and the current value is filled in so you can edit it.  Pressing Enter on a setting no longer blanks it.
- Re-selecting the default callsign no longer wipes its display name.
- ADIF import/export.  In Logbook mode press I to import or X to export (also in the Esc menu).  Import reads .adi/.adif files, or a .zip with one inside (eQSL, LoTW, QRZ, WSJT-X etc.), from the hamclock folder, your home folder or ~/Downloads, or a path you type.  QSOs already in the log are skipped, so importing the same file twice is safe.  Fields the logbook has no column for (QSL status, contest info...) are kept and written back out on export.  Exports go to the hamclock folder unless you type another path.  adif.py needs to be in the same folder as hamclock.py.
- Online lookups (QRZ etc.) are saved to the local database as normal columns only.  The whole XML reply and the QRZ session key are no longer stored, and QRZ's serial number and bio size no longer land in the FCC ID / interest profile columns.  A few QRZ extras with no column (nickname, CQ/ITU zone, LoTW/eQSL) are kept in data_json.  Rows saved by older versions are cleaned up automatically the next time hamclock starts.
