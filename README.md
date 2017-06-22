# read_tor_aigk

Read and collect https://www.reformagkh.ru data
Read using TOR, TOR must be in system, must be working and configured.
For config TOR (you can see https://www.torproject.org/docs/tor-relay-debian.html.en) edit file /etc/tor/torrc - for Ubuntu, 
or ..\Tor Browser\Browser\TorBrowser\Data\Tor\torrc for Windows
You can't open relay, but you must set HashedControlPassword to hashed password 'cmasf' - it used in this script. 
Or hash and use your own.
So you must set up TOR control port: ControlPort 9151

This script read house passports from https://www.reformagkh.ru and make from its useful DataFrame, than save it to CSV-file.
Site https://www.reformagkh.ru bad organized, for making house passports list you must open web-page of every passport - it can take a very long time.
Site unstable, uses the protection of multiple requests and work slow - so script use TOR.

Script's idea taken from this https://github.com/nextgis/reformagkh (thanks)

You must have installed BeautifulSoup, pandas, numpy, requests and stem libiary
