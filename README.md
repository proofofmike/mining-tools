# mining-tools

Bitcoin mining tools by @proofofmike.

## str_race.py

Multi-pool stratum prevhash race tool.

Connects to multiple pools and watches live `mining.notify` traffic to compare when pools deliver new jobs / prevhash updates.

## ph_timing.py

Single-pool initial work timing helper.

Connects to one pool and measures how long it takes to receive the first usable `mining.notify` job after connection.

## Notes

Results depend on your location, network path, DNS, and pool backend behavior.
