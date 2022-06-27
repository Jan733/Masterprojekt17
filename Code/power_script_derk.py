import geopandas as gpd
import pandas as pd
power_circ_members = pd.DataFrame
# voltage_select ist vorgegebene SPannung

# 806-823:
for i in range(0, len(power_circ_members)):
    if id in power_circ_members.iloc['relation_id', i]:
        if f_bus in bus_data.iloc['id', i] or t_bus in bus_data.iloc['id', i]:
            if bus_data.iloc['origin', i] == 'rel' and bus_data.iloc['cnt', i] == 1 and bus_data.iloc['substation_id', i] is NULL:
                # delete row

# 825-826
for i in range(0, len(power_circ_members)):
    if power_circ_members.iloc['relation_id', i] != power_circuits['id', i]:
        # delete row

# 829-830
for i in range(0, len(bus_data)):
    if bus_data.iloc['origin', i] == 'rel' and bus_data.iloc['cnt', i] == 0:
        # delete row

# 840
power_line['frequency_from_relation'] = ''

# 879
power_line_sep = pd.DataFrame
power_line_sep = branch_data
power_line_sep['relation_id'] = 0

# 884-885
power_line_sep['startpoint_geometry'] = ''
power_line_sep['endpoint_geometry'] = ''

# 893-908 + 911-927
for i in range(0, len(power_line_sep)):
    if power_line_sep.iloc['voltage', i] <= voltage_select or power_line_sep.iloc['cables', i] = NULL or power_line_sep.iloc['cables', i] == 0:
        # delete row

# 935-938
for i in range(0, len(power_line_sep)):
    if power_line_sep.iloc['frequency', i] == NULL:
        if power_line_sep.iloc['voltage', i] == 220 or power_line_sep.iloc['voltage', i] == 380:
            power_line_sep.iloc['frequency', i] = 50
    elif power_line_sep.iloc['voltage', i] == 110:
        if power_line_sep.iloc['cables', i] == 3 or power_line_sep.iloc['cables', i] == 6 or power_line_sep.iloc['cables', i] == 9 or power_line_sep.iloc['cables', i] == 12:
            power_line_sep.iloc['frequency', i] = 50
        elif power_line_sep.iloc['cables', i] == 2 or power_line_sep.iloc['cables', i] == 4:
            power_line_sep.iloc['frequency', i] = 16.7
    else:
        power_line_sep.iloc['frequency', i] = 50

# 1002-1004
for i in range(0, len(bus_data)):
    if bus_data.iloc['origin', i] == 'lin' and bus_data.iloc['cnt', i] == 0:
        # löschen der Knoten

# 1010-1022 + 1024-1036
branch_data['relation_id'] = power_circ_members['relation_id'] + power_line_sep['relation_id']
branch_data['line_id'] = power_circ_members['line_id'] + power_line_sep['line_id']
branch_data['f_bus'] = power_circ_members['f_bus'] + power_line_sep['f_bus']
branch_data['t_bus'] = power_circ_members['t_bus'] + power_line_sep['t_bus']
branch_data['length'] = power_circ_members['length'] + power_line_sep['length']
branch_data['way'] = power_circ_members['way'] + power_line_sep['way']
branch_data['voltage'] = power_circ_members['voltage'] + power_line_sep['voltage']
branch_data['cables'] = power_circ_members['cables'] + power_line_sep['cables']
branch_data['wires'] = power_circ_members['wires'] + power_line_sep['wires']
branch_data['frequency'] = power_circ_members['frequency'] + power_line_sep['frequency']
branch_data['power'] = power_circ_members['power'] + power_line_sep['power']

# 1045-1046
del branch_data['buffered']
del branch_data['origin']

# 1045-1046
for i in range(0, len(branch_data)):
    if branch_data.iloc['frequency', i] != 50 or branch_data.iloc['frequency', i] = NULL
        del branch_data.iloc[:, i]

# 1069
for i in range(0, len(branch_data)):
    if branch_data.iloc['cables', i] <= 1:
        del branch_data.iloc[:, i]

# 1072-1073
for i in range(0, len(branch_data)):
    if not id in branch_data.iloc['fbus', i] and id in branch_data.iloc['tbus', i]:
        del branch_data.iloc[:, i]

# 1084-1089
for i in range(0, len(branch_data)):
    if branch_data.iloc['f_bus', i] == id or branch_data.iloc['f_bus', i] == id:
        branch_data.iloc['way', i] = ST_AddPoint(way, bus_data.iloc['the_geom', i])

# 1112-1120
for i in range(0, len(branch_data)):
    branch_data.iloc['line_id', i] = array(branch_data.iloc['line_id', i])
    branch_data.iloc['way', i] = array(branch_data.iloc['way', i])

# 1130-1133
# branch_data['multiline'] = ST_Multi(ST_union(ways))

# 1139
branch_data['discovered'] = False
