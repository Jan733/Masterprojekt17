import geopandas as gpd
import pandas as pd
from math import pi as pi
import numpy as np

power_circ_members = pd.DataFrame
branch_data = pd.DataFrame
bus_data = pd.DataFrame
branch_data = pd.DataFrame
dcline_data = pd.DataFrame
dcline_specifications = pd.DataFrame
power_substation = pd.DataFrame
dcline_specifications_specs = pd.DataFrame
power_circuits = pd.DataFrame
# voltage_select ist vorgegebene SPannung

# Berechnet due genauen Leitungsspezifikationen
def otg_calc_branch_specifications(branch_data, branch_specifications, bus_data, abstr_values):
    dict_v_branch = {
        "branch_id": [],
        "spec_id": [],
        "length": [],
        "f_bus": [],
        "t_bus": [],
        "cables": [],
        "wires": [],
        "power": [],
    }
    v_branch = pd.DataFrame(dict_v_branch)

    v_branch.loc[:, ['branch_id', 'spec_id', 'length', 'f_bus', 't_bus', 'cables', 'wires', 'power']] = branch_data.loc[
        (branch_data['power'] == 'line') or (branch_data['power'] == 'cable'), ['branch_id', 'spec_id', 'length', 'f_bus', 't_bus', 'cables', 'wires', 'power']]

    v_spec = branch_specifications.loc[(branch_specifications['spec_id'] == v_branch['spec_id'])]

    v_numb_syst = round(v_branch["cables"]/3)

    if bus_data.loc['id'] == v_branch.loc['t_bus'] and abstr_values.loc['val_description'] == 'base_MVA':
        v_Z_base = bus_data.loc['voltage'] / (abstr_values.loc['val_int'] * 10^6)

    v_R = (v_branch['length'] * v_spec['R_Ohm_per_km'] * 10 ^ (-3)) / v_numb_syst
    v_X = (v_branch['length'] * 2 * pi * 50 * v_spec['L_mH_per_km'] * 10 ^ (-6)) / v_numb_syst
    v_B = v_branch['length'] * 2 * pi * 50 * v_spec['C_nF_per_km'] * 10 ^ (-12) * v_numb_syst
    v_S_long = v_numb_syst * v_spec['Stherm_MVA'] * (10 ^ 6)

    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "br_r"] = v_R / v_Z_base
    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "br_x"] = v_X / v_Z_base
    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "S_long"] = v_S_long

    return branch_data

"""
    for i in range(0, len(branch_data)):
        if branch_data.iloc['power', i] == 'line' or branch_data.iloc['power', i] == 'cable':
            # write all relevant data from branch_dat to v_spec dictionary
            v_branch["branch_id"]   =   branch_data.iloc['branch_id', i]
            v_branch["spec_id"]     =   branch_data.iloc['spec_id', i]
            v_branch["length"]      =   branch_data.iloc['length', i]
            v_branch["f_bus"]       =   branch_data.iloc['f_bus', i]
            v_branch["t_bus"]       =   branch_data.iloc['t_bus', i]
            v_branch["cables"]      =   branch_data.iloc['cables', i]
            v_branch["wires"]       =   branch_data.iloc['wires', i]
            v_branch["power"]       =   branch_data.iloc['power', i]

            for j in range(0, len(branch_specifications)):
                # write all relevant data from specifications to v_spec
                if branch_specifications.iloc['spec_id', j] == v_branch["spec_id"]:
                    v_spec = dict(branch_specifications.iloc[j])

                # Declare the number of systems v_numb_syst and the basic resistance v_Z_base
                v_numb_syst = round(v_branch["cables"]/3)
                if bus_data.iloc['id', j] == v_branch["t_bus"] * v_branch["t_bus"]:
                    if abstr_values.iloc['val_description', j] == 'base_MVA':
                        v_Z_base = bus_data.iloc['voltage', j] / (abstr_values.iloc['val_int', j] * 10^6)

                v_R = (v_branch["length"] * v_spec["R_Ohm_per_km"] * 10^(-3)) / v_numb_syst
                v_X = (v_branch["length"] * 2 * pi * 50 * v_spec["L_mH_per_km"] * 10^(-6)) / v_numb_syst
                v_B = v_branch["length"] * 2 * pi * 50 * v_spec["C_nF_per_km"] * 10 ^ (-12) * v_numb_syst
                v_S_long = v_numb_syst * v_spec["Stherm_MVA"] * (10^6)

                if branch_data.iloc["branch_id", i] == v_branch["branch_id"]:
                    branch_data.iloc["br_r", i] = v_R / v_Z_base
                    branch_data.iloc["br_x", i] = v_X / v_Z_base
                    branch_data.iloc["S_long", i] = v_S_long
    return branch_data
"""


def otg_calc_dcline_specifications(dcline_data, dcline_specifications):
    dict_v_dcline = {
        "branch_id": [],
        "spec_id": [],
        "length": [],
        "voltage": [],
        "cables": [],
    }
    v_dcline = pd.DataFrame(dict_v_dcline)

    dict_v_spec = {
        "r_ohm_per_km": [],
        "i_a_therm": []
    }
    v_spec = pd.DataFrame(dict_v_spec)

    v_dcline["branch_id"] = dcline_data["branch_id"]
    v_dcline["spec_id"] = dcline_data["spec_id"]
    v_dcline["length"] = dcline_data["length"]
    v_dcline["voltage"] = dcline_data["voltage"]
    v_dcline["cables"] = dcline_data["cables"]

    v_spec["r_ohm_per_km"] = dcline_specifications["r_ohm_per_km"]
    v_spec["i_a_therm"] = dcline_specifications["i_a_therm"]

    v_numb_syst = round(v_dcline["cables"] / 2)
    v_pmax = 2 * v_dcline["voltage"] * v_spec["i_a_therm"]
    v_R = (v_spec["r_ohm_per_km"] / 1000) * v_dcline["length"]

    v_loss1_max = v_spec["i_a_therm"] * v_R / (2 * v_dcline["voltage"])

    dcline_data.loc[(dcline_data["branch_id"] == v_dcline["branch_id"]), "loss1"] = v_loss1_max
    dcline_data.loc[(dcline_data["branch_id"] == v_dcline["branch_id"]), "pmax"] = v_pmax

    return dcline_data

"""
    for i in range(0, len(dcline_data)):
        v_dcline["branch_id"]   =   dcline_data.iloc["branch_id", i]
        v_dcline["spec_id"]     =   dcline_data.iloc["spec_id", i]
        v_dcline["length"]      =   dcline_data.iloc["length", i]
        v_dcline["voltage"]     =   dcline_data.iloc["voltage", i]
        v_dcline["cables"]      =   dcline_data.iloc["cables", i]

    for j in range(0, len(dcline_data)):
        v_spec["r_ohm_per_km"] = dcline_specifications.iloc["r_ohm_per_km", j]
        v_spec["i_a_therm"] = dcline_specifications.iloc["i_a_therm", j]

        v_numb_syst = round(v_dcline["cables"]/2)
        v_pmax = 2 * v_dcline["voltage"] * v_spec["i_a_therm"]
        v_R = (v_spec["r_ohm_per_km"] / 1000) * v_dcline["length"]

        v_loss1_max = v_spec["i_a_therm"] * v_R / (2 * v_dcline["voltage"])

        if dcline_data.iloc["branch_id", j] == v_dcline["branch_id"]:
            dcline_data.iloc["loss1", j] = v_loss1_max
            dcline_data.iloc["pmax", j] = v_pmax

    return dcline_data
"""


def otg_calc_max_node_power(bus_data, branch_data, dcline_data):
    dict_v_bus = {
        "id": []
    }
    v_bus = pd.DataFrame(dict_v_bus)

    v_bus.loc[bus_data["substation_id"] is not NULL, "id"] = bus_data.loc["id"]
    v_s_long_sum = 0

    v_s_long_sum_branch = sum(branch_data.loc[(((branch_data["f_bus"] == v_bus["id"]) or (branch_data["t_bus"] == v_bus["id"])) and
                                               ((branch_data["power"] == 'line') or (branch_data["power"] == 'cable'))), "S_long"])
    if v_s_long_sum_branch != 0:
        v_s_long_sum += v_s_long_sum_branch

    v_s_long_sum_dcline = sum(dcline_data.loc[(dcline_data["f_bus"] == v_bus["id"]) or
                                              (dcline_data["t_bus"] == v_bus["id"]), "pmax"])
    if v_s_long_sum_dcline != 0:
        v_s_long_sum += v_s_long_sum_dcline

    bus_data.loc[bus_data["id"] == v_bus["id"], "id"] = v_s_long_sum

    return bus_data

"""
    for i in range(0, len(bus_data)):
        if not bus_data.iloc["substation_id", i] is NULL: #Check what NULL is in our case
            v_bus["id"] = bus_data.iloc["id", i]
            for j in range(0, len(branch_data)):

                v_s_long_sum = 0
                if branch_data.iloc["f_bus", j] == v_bus["id"] or branch_data.iloc["t_bus", j] == v_bus["id"]:
                    if branch_data.iloc["power", j] == 'line' or branch_data.iloc["power", j] == 'cable':
                        v_s_long_sum_branch = sum(branch_data.iloc["S_long", j])
                if v_s_long_sum_branch != 0:
                    v_s_long_sum += v_s_long_sum_branch

                if dcline_data.iloc["f_bus", j] == v_bus["id"] or dcline_data.iloc["t_bus", j] == v_bus["id"]:
                    v_s_long_sum_dcline = sum(dcline_data.iloc["pmax", j])
                if v_s_long_sum_dcline != 0:
                    v_s_long_sum += v_s_long_sum_dcline

                if bus_data.iloc["id", j] == v_bus["id"]:
                    bus_data.iloc["id", j] = v_s_long_sum
    return bus_data
"""


def otg_calc_transformer_specifications(branch_data, bus_data, transformer_specifications, abstr_values):
    dict_v_branch = {
        "branch_id": [],
        "f_bus": [],
        "t_bus": [],
    }
    v_branch = pd.DataFrame[dict_v_branch]

    v_branch["branch_id"] = branch_data.loc[branch_data["power"] == 'transformer', "branch_id"]
    v_branch["f_bus"] = branch_data.loc[branch_data["power"] == 'transformer', "f_bus"]
    v_branch["t_bus"] = branch_data.loc[branch_data["power"] == 'transformer', "t_bus"]

    if bus_data.loc["id"] == v_branch["f_bus"] or bus_data.iloc["id"] == v_branch["t_bus"]:
        v_S_long_MVA_sum_max = (10 ^ (-6)) * min(bus_data.loc["S_long_sum"])
        if v_S_long_MVA_sum_max != 0:
            v_S_long_MVA_sum_max = (10 ^ (-6)) * max(bus_data.loc["S_long_sum"])
    v_S_long_MVA_sum_max = v_S_long_MVA_sum_max / 2

    if bus_data.loc["id"] == v_branch["f_bus"] or bus_data.loc["id"] == v_branch["t_bus"]:
        v_U_OS = max(bus_data.loc["voltage"])

    if bus_data.loc["id"] == v_branch["f_bus"] or bus_data.iloc["id"] == v_branch["t_bus"]:
        v_U_US = min(bus_data.loc["voltage"])

    if transformer_specifications.loc["U_OS"] == v_U_OS and transformer_specifications.loc["U_US"] == v_U_US:
        v_numb_transformers = round(v_S_long_MVA_sum_max / transformer_specifications.loc["S_MVA"], 0)
        v_Srt = transformer_specifications.loc["S_MVA"] * 10 ^ 6
        v_u_kr = transformer_specifications.loc["u_kr"]

    v_Z_TOS = v_u_kr / 100 * v_U_OS ^ 2 / v_Srt
    v_X_TOS = v_Z_TOS
    v_Bl_TOS = -1 / v_X_TOS
    v_Bl_TOS_all = v_numb_transformers * v_Bl_TOS
    v_X_TOS_all = -1 / v_Bl_TOS_all

    if abstr_values.loc["val_description"] == 'base_MVA':
        v_Z_base = abstr_values.loc["val_int"] * 10 ^ 6

    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "br_r"] = 0
    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "br_x"] = v_X_TOS_all / v_Z_base
    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "br_b"] = 0
    branch_data.loc[(branch_data["branch_id"] == v_branch["branch_id"]) and
                    (transformer_specifications["U_OS"] == v_U_OS) and
                    transformer_specifications["U_US"] == v_U_US,
                    "S_long"] = 10 ^ 6 * v_numb_transformers * transformer_specifications.loc["S_MVA"]
    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "tap"] = 1
    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "shift"] = 0
    branch_data.loc[branch_data["branch_id"] == v_branch["branch_id"], "numb_transformers"] = v_numb_transformers

    return branch_data

"""   
    for i in range(0, len(branch_data)):
        if branch_data.iloc["power", i] == 'transformer':
            v_branch["branch_id"] = branch_data.iloc["branch_id", j]
            v_branch["f_bus"] = branch_data.iloc["f_bus", j]
            v_branch["t_bus"] = branch_data.iloc["t_bus", j]

            for j in range(0, len(bus_data)):
                if bus_data.iloc["id", j] == v_branch["f_bus"] or bus_data.iloc["id", j] == v_branch["t_bus"]:
                    v_S_long_MVA_sum_max = (10^(-6)) * min(bus_data.iloc["S_long_sum", j])
                    if v_S_long_MVA_sum_max != 0:
                            v_S_long_MVA_sum_max = (10^(-6)) * max(bus_data.iloc["S_long_sum", j])
                v_S_long_MVA_sum_max = v_S_long_MVA_sum_max/2

                if bus_data.iloc["id", j] == v_branch["f_bus"] or bus_data.iloc["id", j] == v_branch["t_bus"]:
                    v_U_OS = max(bus_data.iloc["voltage", j])

                if bus_data.iloc["id", j] == v_branch["f_bus"] or bus_data.iloc["id", j] == v_branch["t_bus"]:
                    v_U_US = min(bus_data.iloc["voltage", j])

                if transformer_specifications.iloc["U_OS", j] == v_U_OS and transformer_specifications.iloc["U_US", j] == v_U_US:
                    v_numb_transformers = round(v_S_long_MVA_sum_max / transformer_specifications.iloc["S_MVA", j], 0)
                    v_Srt = transformer_specifications.iloc["S_MVA", j] * 10^6
                    v_u_kr = transformer_specifications.iloc["u_kr", j]

                v_Z_TOS = v_u_kr/100 * v_U_OS^2 / v_Srt
                v_X_TOS = v_Z_TOS
                v_Bl_TOS = -1 / v_X_TOS
                v_Bl_TOS_all = v_numb_transformers * v_Bl_TOS
                v_X_TOS_all = -1 / v_Bl_TOS_all

                if abstr_values.iloc["val_description", j] == 'base_MVA':
                    v_Z_base = abstr_values.iloc["val_int", j] * 10^6

                if branch_data.iloc["branch_id", j] == v_branch["branch_id"]:
                    branch_data.iloc["br_r", j] = 0
                    branch_data.iloc["br_x", j] = v_X_TOS_all / v_Z_base
                    branch_data.iloc["br_b", j] = 0
                    if transformer_specifications.iloc["U_OS", j] == v_U_OS and transformer_specifications.iloc["U_US", j] == v_U_US:
                        branch_data.iloc["S_long", j] = 10^6 * v_numb_transformers * transformer_specifications.iloc["S_MVA", j]
                    branch_data.iloc["tap", j] = 1
                    branch_data.iloc["shift", j] = 0
                    branch_data.iloc["numb_transformers", j] = v_numb_transformers

    return branch_data
"""


def otg_seperate_voltage_levels(): # noch in Bearbeitung
    dict_v_line = {
        "way": [],
        "power": [],
        "numb_volt_lev": [],
        "voltage_array": [],
        "cables_array": [],
        "wires_array": [],
        "frequency_array": [],
        "length": [],
        "startpoint": [],
        "endpoint": [],
    }
    v_line = pd.DataFrame(dict_v_line)

    v_line["way"]               =   power_line["way"]
    v_line["power"]             =   power_line["power"]
    v_line["numb_volt_lev"]     =   power_line["numb_volt_lev"]
    v_line["voltage_array"]     =   power_line["voltage_array"]
    v_line["cables_array"]      =   power_line["cables_array"]
    v_line["wires_array"]       =   power_line["wires_array"]
    v_line["frequency_array"]   =   power_line["frequency_array"]
    v_line["length"]            =   power_line["length"]
    v_line["startpoint"]        =   power_line["startpoint"]
    v_line["endpoint"]          =   power_line["endpoint"]


def otg_numb_unknown_cables_lev(id):

    if power_line["cables_array"].loc[power_line["cables_array"][i]].isnull() == False:
        v_numb_known_cable_lev += 1
        numb_unknown_volt_lev = len(power_line["cables_array"]) - v_numb_known_cable_lev

    return numb_unknown_volt_lev


def otg_numb_unknown_freq_lev(id):

    if power_line["frequency_array"].loc[power_line["frequency_array"][i]].isnull() == False:
        v_numb_known_frequency_lev += 1
        numb_unknown_frequency_lev = len(power_line["frequency_array"]) - v_numb_known_frequency_lev

    return numb_unknown_frequency_lev


def otg_check_all_cables_complete():

    # ?????????????
    dict_ok = {
        "ok": []
    }
    ok = pd.DataFrame(dict_ok)

    if (power_line.loc[power_line["voltage_array"][1].isnull() == False and power_line["cables_array"][1].isnull() == True or
                    power_line.loc[power_line["voltage_array"][2].isnull() == False] and power_line.loc[power_line["cables_array"][2].isnull()] == True or
                    power_line.loc[power_line["voltage_array"][3].isnull() == False] and power_line["cables_array"][3].isnull() == True] or
                    power_line.loc[power_line["voltage_array"][4].isnull() == False] and power_line["cables_array"][4].isnull() == True]):
        ok.loc["ok"] = False

    return ok


def otg_all_freq_like(v_voltage_array, v_frequency_array, v_freq):

    if v_voltage_array.isnull():
        return False

    for a in len(v_voltage_array):
        if v_voltage_array[a].isnull() or v_frequency_array[a].isnull() or v_frequency_array[a] != v_freq:
            return False

    return True


def otg_int_sum(int_1, int_2, int_3, int_4):

    v_sum = 0
    if int_1.isnull():
        v_sum = v_sum + int_1

    if int_2.isnull():
        v_sum = v_sum + int_2

    if int_3.isnull():
        v_sum = v_sum + int_3

    if int_4.isnull():
        v_sum = v_sum + int_4

    return v_sum


def otg_known_cables_sum(v_id):

    cables_array = power_line.loc[power_line["id"] == v_id, "cables_array"]
    return otg_int_sum(int_1 = cables_array[1], int_2 = cables_array[2], int_3 = cables_array[3], int_4 = cables_array[4])


def otg_check_cable_complete(v_id, v_lev):

    x = power_line.loc[(power_line["id"] == v_id) and
                       (power_line["voltage_array"][v_lev].isnull() is not True) and
                       (power_line["cables_array"][v_lev].isnull() is True)]
    return x


def otg_3_cables_heuristic():

    dict_v_line = {
        "id": [],
        "voltage_array": [],
        "cables_sum": [],
        "frequency_array": []
    }
    v_line = pd.DataFrame(dict_v_line)

    ok = otg_check_all_cables_complete(id)

    v_line.loc[:, ["id", "voltage_array", "cables_sum", "frequency_array"]] = power_line.loc[(ok["ok"] == False) and
                                                                                            power_line["power"] == 'line',
                                                                                            ["id", "voltage_array", "cables_sum", "frequency_array"]]

    if (otg_all_freq_like(v_voltage_array = v_line.voltage_array, v_frequency_array = v_line.frequency_array, v_freq = 50) and
        (v_line.cables_sum - otg_known_cables_sum(v_line.id)) / otg_numb_unknown_cables_lev(v_line.id) == 3):

        for i in range(1,4):
            otg_check_cable_complete(v_line["id"], i)
            power_line.loc[power_line["id"] == v_id, "cables_array"[i]] = 3
            if power_line.loc["id"] == v_id:
                cables_from_3_cables = True

df_neighbours_startpoint_indexes["index1"].loc[df_neighbours_startpoint_indexes["index1"].str.len() >0]
def otg_neighbour_heuristic():

    dict_all_neighbours = {
        "id": [],
        "cables": [],
        "frequency": []
    }
    all_neighbours = pd.DataFrame(dict_all_neighbours)

    v_id_line = power_line.loc["id", "all_neighbours", "voltage_array", "cables_array", "frequency_array"]

    for i in range(1,4):
        if (v_id_line.loc[v_id_line["voltage_array"][i]].isnull() or
            (v_id_line.loc[v_id_line["cables_array"][i]].isnull() is False and
             v_id_line.loc[v_id_line["frequency_array"][i]].isnulll() is False)):
            for j in range(1,2):

                if v_id_line["all_neighbours"][i][j][1][1].isnull():
                    for k in range(1,10):
                        all_neighbours.loc[["id", "cables", "frequency"]] = power_line.loc[power_line["id"] == v_id_line["all_neighbours"][i][j][k][1],
                                                                                                          ["id", cables_array[v_id_line.all_neighbours [i][j][k][2]], [frequency_array[v_id_line.all_neighbours [i][j][k][2]]]]
                if v_id_line["frequency_arrray"][i].isnull() and
                        all(element == all_neighbours["frequency"][0] for element in list(all_neighbours["frequency"])) is True and
                        all(element == 0 for element in list(all_neighbours["frequency"])) is False:
                    power_line.loc[power_line["id"] == v_id_line.id, frequency_array[i]] = all_neighbours["frequency"][1]

                if not v_id_line["frequency_arrray"][i].isnull():
                    all_neighbours.drop(all_neighbours.loc[all_neighbours["frequency"] != v_id_line["frequency_array"][i]])

                if not all_neighbours["frequency_array"].isnull():
                    break

                if v_id_line["cables_array"][i].isnull() and not all_neighbours["cables"].isnull():
                    power_line.loc[power_line["id"] == v_id_line["id"], ["cables_array", "cables_from_neighbour"]] = [all_neighbours["cables"], True]

    all_neighbours.drop()


def otg_sum_heuristic():

    dict_v_line = {
        "id": [],
        "cables_sum": []
    }
    v_line = pd.DataFrame(dict_v_line)
    ok = otg_check_all_cables_complete()
    if ok.loc["ok"] is False and power_line.loc["power"] == 'line':
        v_line.loc["id", "cables_sum"] = power_line["id", "cables_sum"]

    if otg_numb_unknown_cables_lev(v_line.loc(["id"])) == 1 and not v_line["cables_sum"].isnull():
        v_cables_left = v_line.loc["cables_sum"] - otg_known_cables_sum(v_line.loc(["id"]))
        for i in range(1,4):
            if not otg_check_cable_complete(v_line["id"], i):
                power_line.loc[power_line["id"] == v_line["id"], ["cables_array"[i], "cables_from_sum"]] = [v_cables_left, True]


def otg_unknown_value_heuristic():

    v_count_end = otg_numb_unknown_cables_lev(id) + sum(otg_numb_unknown_freq_lev(id))

    while v_count_end > 0:
        otg_3_cables_heuristic()
        otg_neighbour_heuristic()
        otg_sum_heuristic()
        v_count_end = otg_numb_unknown_cables_lev(id) + sum(otg_numb_unknown_freq_lev(id))


def otg_wires_assumption():

    v_min_voltage = abstr_values.loc[abstr_values["val_descripton"] == 'min_voltage', 'val_int']

    branch_data.loc[(branch_data["voltage"] >= 380000) and
                    (branch_data["frequency"] == 50) and
                    (branch_data["power"] == 'line'),
                    'wires'] = 4

    branch_data.loc[(branch_data["wires"].isnull() is True) and
                    (branch_data["voltage"] >= v_min_voltage) and
                    (branch_data["voltage"] < 380000) and
                    (branch_data["voltage"] < 110000) and
                    (branch_data["power"] == 'line'),
                    "wires"] = 2

    branch_data.loc[(branch_data["wires"].isnull() is True)
                    and (branch_data["power"] == 'line'),
                    'wires'] = 1


def otg_array_search_2(v_element, v_array):

    v_sub = pd.DataFrame()
    for i in range(1, len(v_array)):
        v_sub[i] = np.find(v_array[i], v_element)

    return v_sub


def otg_110kv_cables():

   dict_v_line = {
       "id": [],
       "voltage_array": [],
       "cables_array": [],
       "frequency_array": []
   }
   v_line = pd.DataFrame(dict_v_line)
   v_line["id", "voltage_array", "cables_array", "frequency_array"] = power_line["id", "voltage_array", "cables_array", "frequency_array"]

   v_volt_idx = otg_array_search_2(v_element=11000, v_array=v_line["voltage_array"])

   power_line.loc[(power_line["id"] == v_line["id"]) and
                  (v_line["cables_array"][v_volt_idx].isnull() is True) and
                  ((v_line["frequency_array"][v_volt_idx].isnull() is True) or
                  (v_line["frequency_array"][v_volt_idx] == 50)), "cables_array"[v_volt_idx]] = 3


def otg_point_inside_geometry(param_geom):

    param_geom = gpd.GeoSeries(param_geom)
    # GeoSeries.centroid replaces ST_Centroid
    var_cent = param_geom.centroid
    var_result = var_cent

    while var_result.intersection(param_geom, align=False).is_empty():
        # Replaces ST_Expand
        var_result.scale(2,2)

    var_result = gpd.GeoSeries(var_result.intersection(param_geom, align=False)).representative_point()

    return var_result


def otg_split_table(v_table, v_parameter):

    table_order = pd.DataFrame()
    table_order["v_parameter"] = v_table["v_parameter"].copy()

    number_of_rows = len(table_order)

    split_table_1 = table_order.iloc[:number_of_rows/10, :]
    split_table_2 = table_order.iloc[1 * number_of_rows / 10:2 * number_of_rows / 10, :]
    split_table_3 = table_order.iloc[2 * number_of_rows / 10:3 * number_of_rows / 10, :]
    split_table_4 = table_order.iloc[3 * number_of_rows / 10:4 * number_of_rows / 10, :]
    split_table_5 = table_order.iloc[4 * number_of_rows / 10:5 * number_of_rows / 10, :]
    split_table_6 = table_order.iloc[5 * number_of_rows / 10:6 * number_of_rows / 10, :]
    split_table_7 = table_order.iloc[6 * number_of_rows / 10:7 * number_of_rows / 10, :]
    split_table_8 = table_order.iloc[7 * number_of_rows / 10:8 * number_of_rows / 10, :]
    split_table_9 = table_order.iloc[8 * number_of_rows / 10:9 * number_of_rows / 10, :]
    split_table_10 = table_order.iloc[9 * number_of_rows / 10:, :]

    table_order.drop()


# 806-809:
# otg_power_circuits_problem_tg muss noch geschrieben werden

# 810-823
power_circ_members.drop(power_circ_members.loc[:, (power_circuits["id"] == power_circ_members["relation_id"]) and
                                               ((power_circ_members["f_bus"] in bus_data.loc['id']) or (power_circ_members["t_bus"] in bus_data.loc['id'])) and
                                               (bus_data.loc['origin'] == 'rel') and
                                               (bus_data.loc['cnt'] == 1) and
                                               (bus_data.loc['substation_id'] is NULL)])
"""
for i in range(0, len(power_circ_members)):
    if id in power_circ_members.iloc['relation_id', i]:
        if f_bus in bus_data.iloc['id', i] or t_bus in bus_data.iloc['id', i]:
            if bus_data.iloc['origin', i] == 'rel' and bus_data.iloc['cnt', i] == 1 and bus_data.iloc['substation_id', i] is NULL:
                # delete row
                power_circ_members.drop(power_circ_members.index[i])
"""

# 825-826
power_circ_members.drop(power_circ_members.loc[power_circ_members["relation_id"] != power_circuits["id"], :])
"""
for i in range(0, len(power_circ_members)):
    if power_circ_members.iloc['relation_id', i] != power_circuits['id', i]:
        # delete row
        power_circ_members.drop(power_circ_members.index[i])
"""

# 829-830
bus_data.drop(bus_data.loc[(bus_data.loc["origin"] == 'rel') and (bus_data.loc["cnt"] == 0), :])
"""
for i in range(0, len(bus_data)):
    if bus_data.iloc['origin', i] == 'rel' and bus_data.iloc['cnt', i] == 0:
        # delete row
        bus_data.drop(bus_data.index[i])
"""

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
power_line_sep.drop(power_line_sep.loc[(power_line_sep['voltage'] <= voltage_select) or (power_line_sep['cables'] == NULL) or (power_line_sep['cables'] == 0), :)]
"""
for i in range(0, len(power_line_sep)):
    if power_line_sep.iloc['voltage', i] <= voltage_select or power_line_sep.iloc['cables', i] = NULL or power_line_sep.iloc['cables', i] == 0:
        # delete row
        power_line_sep.drop(power_line_sep.index[i])
"""

# 935-938
power_line_sep.loc[(power_line_sep["frequency"] == NULL) and
                   (power_line_sep['voltage'] in (220, 380)),
                   "frequency"] = 50
power_line_sep.loc[(power_line_sep["voltage"] == 110) and
                   (power_line_sep['cables'] in (3, 6, 9, 12)),
                   "frequency"] = 50
power_line_sep.loc[(power_line_sep["voltage"] == 110) and
                   (power_line_sep['cables'] in (2, 4)),
                   "frequency"] = 16.7
power_line_sep.loc[(power_line_sep["frequency"] != NULL) and
                   (power_line_sep["voltage"] != 110),
                   "frequency"] = 50
"""
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
"""

# 1002-1004
bus_data.drop(bus_data.loc[(bus_data['origin'] == 'lin') and
                           (bus_data['cnt'] == 0), :])
"""
for i in range(0, len(bus_data)):
    if bus_data.iloc['origin', i] == 'lin' and bus_data.iloc['cnt', i] == 0:
        # löschen der Knoten
"""

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

# 1063-1064
del branch_data.loc[:, (branch_data['frequency'] != 50) or
                    (branch_data['frequency'] = NULL)]
"""               
for i in range(0, len(branch_data)):
    if branch_data.iloc['frequency', i] != 50 or branch_data.iloc['frequency', i] = NULL
        del branch_data.iloc[:, i]
"""

# 1069
del branch_data.loc[:, branch_data["cables"] <= 1]
"""
for i in range(0, len(branch_data)):
    if branch_data.iloc['cables', i] <= 1:
        del branch_data.iloc[:, i]
"""

# 1072-1073
del bus_data.loc[:, (bus_data["id"] in branch_data["f_bus"]) and
                 (bus_data["id"] in branch_data["t_bus"])]
"""
for i in range(0, len(branch_data)):
    if not id in branch_data.iloc['fbus', i] and id in branch_data.iloc['tbus', i]:
        del branch_data.iloc[:, i]
"""

# 1084-1089
for i in range(0, len(branch_data)):
    if branch_data.iloc['f_bus', i] == id or branch_data.iloc['f_bus', i] == id:
        branch_data.iloc['way', i] = ST_AddPoint(way, bus_data.iloc['the_geom', i])

# 1112-1120
branch_data.loc['line_ids'] = np.array(branch_data.loc['line_id'])
branch_data["line_id"].drop()
branch_data.loc['ways'] = np.array(branch_data.loc['way'])
branch_data["way"].drop()
"""
for i in range(0, len(branch_data)):
    branch_data.iloc['line_id', i] = array(branch_data.iloc['line_id', i])
    branch_data.iloc['way', i] = array(branch_data.iloc['way', i])
"""

# 1130-1133
# ST_Multi, ST_union

# 1139
branch_data['discovered'] = False

# 1141
# in discovered: If in Python Input graph_dfs is selected True, then disconnected graphs will be deleted

# 1145-1148
# https://gis.stackexchange.com/questions/401311/creating-linestring-from-two-points-and-finding-mid-point
branch_data['simple_geom'] = bus_data.apply(lambda row: LineString([row['id'], row['id']]), axis=1)

# 1154-1157
dcline_data = branch_data.loc[branch_data["frequency"] == 0, :]
branch_data.drop(branch_data.loc[branch_data["frequency"] == 0, :])
"""
for i in range(0, len(branch_data)):
    if branch_data.iloc['frequency', i] == 0:
        dcline_data.loc[len(dcline_data.index)] = branch_data.iloc['frequency', i]
        # delete row
        branch_data.drop(branch_data.index[i])
"""

# 1166-1173
# falsch
# branch_specifications ist liste mit Norm-Spannungen: (220, 380, 400) kV
for i in range(0, len(branch_data)):
    if branch_data.iloc['power', i] == 'line' or branch_data.iloc['power', i] == 'cable':
        branch_data.iloc['spec_id', i] = branch_data.iloc['branch_specification', i]
        # branch_specifications.power = branch_data.power und ordne sie nach dem kleinsten Abstand zur angegebenen Spannung
        # maximal 1 Eintrag

# 1176-1180
dcline_data.loc['power'] = dcline_specifications.loc['specs.power']
dcline_data.loc['spec_id'] = dcline_specifications_specs.loc['spec_id']
"""
for i in range(0, len(dcline_data)):
    if dcline_data.iloc['power', i] == dcline_specifications.iloc['specs.power', i]:
        dcline_data.iiloc['spec_id', i] = dcline_specifications_specs.iloc['spec_id', i]
"""

# 1186-1191
# kann weggelassen werden da man in Python Spalten nicht extra erstellen muss

# 1194
# berechne Leitungsdaten
branch_data = otg_calc_branch_specifications(branch_data, branch_specifications, bus_data, abstr_values)

# 1195-1200
branch_data.loc[branch_data["power"] != 'transformer', "tap"] = 0
branch_data.loc[branch_data["power"] != 'transformer', "shift"] = 0
"""
for i in range(0, len(branch_data)):
    if branch_data.iloc['power', i] != 'transformer':
        branch_data.iloc['tap', i] = 0
        branch_data.iloc['shift', i] = 0
"""

# 1204-1205
# kann weggelassen werden da man in Python Spalten nicht extra erstellen muss

# 1207
# berechne DC_Kabel Sepzifikationen
dcline_data = otg_calc_dcline_specifications(dcline_data, dcline_specifications)

# 1214
# kann weggelassen werden da man in Python Spalten nicht extra erstellen muss

# 1216
# Berechnet für jeden Knoten innerhalb eines Umspannwerks die maximal übertragbare Leistung
bus_data = otg_calc_max_node_power(bus_data, branch_data, dcline_data)

# 1224
# Berechnet die genauen Trafospezifikationen
branch_data = otg_calc_transformer_specifications(branch_data, bus_data, transformer_specifications, abstr_values)

# 1231
branch_data['bus_type'] = list(1) * len(branch_data['power'])

# 1235-1243
""" keine Ahnung """
bus_data.loc[(abstr_values.loc['val_description'] == 'main_station') and
             (bus_data['substation_id'] = abstr_values['val_int']),
             "bus_type"] = 3
"""
for i in range(0, len(branch_data)):
    if abstr_values.iloc['val_description', i] = 'main_station':
        if bus_data.iloc['substation_id', i] = abstr_values.iloc['val_int', i]:
            if bus_data.iloc['bus_type', i] = bus_data.iloc['id', i]:
                bus_data.iloc['bus_type', i] = 3
"""

# 1249
del power_substation.loc[:, (bus_data["substation_id"] == NULL) and
                         (power_substation.loc['id'] not in bus_data.loc['substation_id'])]
"""
for i in range(0, len(power_substation)):
    if not bus_data.iloc['substation_id', i] != NULL:
        if not power_substation.iloc['id'] in bus_data.iloc['substation_id', i]:
            # delete row
            power_substation.drop(power_substation.index[i])
"""

# 1254-1257
power_substation.loc[bus_data["substation_id"] == power_substation["id"], 's_long'] = sum(bus_data.loc['s_long_sum'])
"""
for i in range(0, len(power_substation)):
    if bus_data.iloc['substation_id', i] == power_substation.iloc['id', i]:
        power_substation.iloc['s_long', i] = sum(bus_data.iloc['s_long_sum', i])
"""

# 1273-1276
bus_data['bus_area'] = list(1) * len(bus_data)
bus_data['zone'] = list(1) * len(bus_data)

# 1279-1282
bus_data['vm'] = list(1) * len(bus_data)
bus_data['va'] = list(0) * len(bus_data)

# 1285-1286
# kann weggelassen werden da man in Python Spalten nicht extra erstellen muss