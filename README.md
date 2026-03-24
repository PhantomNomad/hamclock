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

Have fun with it.4
