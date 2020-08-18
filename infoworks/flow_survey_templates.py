DEPTH = """UserSettings,U_LEVEL,U_CONDHEIGHT,U_VALUES,U_DATETIME,
UserSettingsValues,ft AD,in,ft,mm-dd-yyyy hh:mm,
G_START,G_TS,G_NPROFILES,,,
{start_dt},{time_step},{count},,,
#profile,,,,,
L_LINKID,L_INVERTLEVEL,L_CONDHEIGHT,L_GROUNDLEVEL,L_PTITLE,
{profile}
P_DATETIME,{ts_header}
{ts_data}
"""

FLOW = """UserSettings,U_VALUES,U_DATETIME,
UserSettingsValues,MGD,mm-dd-yyyy hh:mm,
G_START,G_TS,G_NPROFILES,,,
{start_dt},{time_step},{count},,,
#profile,,,,,
L_LINKID,L_CONDCAPACITY,L_PTITLE,
{profile}
P_DATETIME,{ts_header}
{ts_data}
"""

VELOCITY = """UserSettings,U_VALUES,U_DATETIME
UserSettingsValues,ft/s,mm-dd-yyyy hh:mm
G_START,G_TS,G_NPROFILES,,,
{start_dt},{time_step},{count},,,
#profile,,,,,
L_LINKID,L_PTITLE,
{profile}
P_DATETIME,{ts_header}
{ts_data}
"""

RAINFALL = """FILECONT, TITLE,,,,,,
0,   RAINFALL,,,,,,
UserSettings,U_ARD,U_EVAP,U_VALUES,U_DATETIME,,,
UserSettingsValues,in,in/day,in/hr,mm-dd-yyyy hh:mm,,,
G_START,G_TS,G_NPROFILES,G_UCWI,G_ARD,G_EVAP,G_WI,
{start_dt},{time_step},{count},0,0,0,2,
#profile,,,,,
L_UCWI,L_ARD,L_ARF,L_EVAP,L_WI,L_PTITLE,L_ADD,
{profile}
P_DATETIME,{ts_header}
{ts_data}
"""