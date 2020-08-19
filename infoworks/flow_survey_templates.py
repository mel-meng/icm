

DEPTH = """UserSettings,U_LEVEL,U_CONDHEIGHT,U_VALUES,U_DATETIME
UserSettingsValues,ft AD,in,ft,mm-dd-yyyy hh:mm
G_START,G_TS,G_NPROFILES
{start_dt},{time_step},{count}
L_LINKID,L_INVERTLEVEL,L_CONDHEIGHT,L_GROUNDLEVEL,L_PTITLE
{profile}
P_DATETIME,{ts_header}
{ts_data}
"""

FLOW = """UserSettings,U_VALUES,U_DATETIME
UserSettingsValues,MGD,mm-dd-yyyy hh:mm
G_START,G_TS,G_NPROFILES
{start_dt},{time_step},{count}
L_LINKID,L_CONDCAPACITY,L_PTITLE
{profile}
P_DATETIME,{ts_header}
{ts_data}
"""

VELOCITY = """UserSettings,U_VALUES,U_DATETIME
UserSettingsValues,ft/s,mm-dd-yyyy hh:mm
G_START,G_TS,G_NPROFILES
{start_dt},{time_step},{count}
L_LINKID,L_PTITLE
{profile}
P_DATETIME,{ts_header}
{ts_data}
"""

RAINFALL = """!Version=2,type=RED,encoding=MBCS
FILECONT, TITLE
0,1
UserSettings,U_RD,U_EVAP,U_FLOW,U_VALUES,U_DATETIME
UserSettingsValues,in,in/day,MGD,in/hr,mm-dd-yyyy hh:mm
G_START,G_TS,G_NPROFILES,G_ARD,G_EVAP,G_UCWI,G_API30,G_SMS,G_SMD,G_WI,G_CINI,G_BF0,G_RP,G_DUR,G_RPT
{start_dt},{time_step},{count},         0,    0,     0.000,         0,         0,     0.000,    2,         0,         0,    0.0000,       0.0,POT  
L_ARF,L_ARD,L_EVAP,L_UCWI,L_API30,L_SMS,L_SMD,L_WI,L_CINI,L_BF0
{profile1}
L_PTITLE,L_PDESC,L_GAUGE_DATA
{profile}
L_ACF,L_SCF
{profile2}
P_DATETIME,{ts_header}
{ts_data}
"""

TMP = {'flow': FLOW, 'depth': DEPTH, 'velocity': VELOCITY, 'rainfall': RAINFALL}