#!/bin/bash
python realtimeresource.py & # Real time dashboaed of currently using resource
python resourceinfo.py & # Real time dashboard of monthly user's byte resource using 
python virusinfo.py & # Real time dashboard of monthly virus overview
python realtimevirus.py & # Real time dashboard of virus monitor
