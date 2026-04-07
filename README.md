This is a vibe coded ham clock.  Pretty much nothing in here was coded by me.  I do want to give a shout out to https://uninformativ.de/git/asciiworld as I had OpenAI convert his C code to python and integrated it in to my program.  I'm just using the shapefile that he provided in his git.

You wil need a couple of python libraries, pillow, pyshp and ncurses for sure.  I think modern 3+ python comes with the rest already installed.  It will warn you if you are missing anything.

There are a few settings you need to declare near the top of hamclock.py before it will work properly.

wx_location_name: It will display this in the local weather panel if it can't figure out where you are from lat/long/grid square
wx_lat/wx_long: These are in decimal notation and are optional.  It will convert your grid square if given.
wx_grid: This is your grid square.  With out it, hamclock can't calculate your lat/long and get weather.
wx_update_seconds: Defaulted to 1800 (30 minutes)
map_image_path: Not used.  I should get rid of it.
refresh_seconds: This needs a better name.  But it's for how often it refreshes the space weather.  Should be set much higher then default 10 seconds.
map_refresh_seconds: How often to update the world map.
time_refresh_seconds: Default is one second so it makes the clock tick

Pressing ESC will bring up a menu with "Settings" and "Quit".  Only Quit works at the moment.  I will have to vibe code in the settings at some point.  You can also just press "q" while it's running to quit.

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
