# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 18:44:01 2022

@author: Jan
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import json
from matplotlib import pyplot as plt
#env Datei erstellen.
#import geoalchemy2
#import psycopg2
#import fiona
import matplotlib
from shapely.geometry import shape
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry import Polygon
from shapely.ops import polygonize, nearest_points
import os
from shapely import wkt
import ast
import time
#Processingtime in minutes
timer_start = time.time()
#df = pd.concat(map(pd.read_csv, ["C:/Users/Jan/Documents/Studium/Master/1.Semester/Masterprojekt/osmTGmod/raw_data/Probedateien/node.csv","C:/Users/Jan/Documents/Studium/Master/1.Semester/Masterprojekt/osmTGmod/raw_data/Probedateien/way_neu.csv"]),ignore_index=True)

#print(df.head().to_string())
#print(df.to_string())

# Erstellen der Paths
data_path = os.getcwd()
raw_data_dir = os.path.dirname(os.getcwd()) + "/raw_data"

(raw_data_dir)

#   Eingabe des / der Filenames:
#filename_node = input("Geben sie den Node Filename ein, den Sie bearbeiten möchten (bspw.: Node.csv): ")
#filename_way = input("Geben sie den Way Filename ein, den Sie bearbeiten möchten (bspw.: way.csv): ")
#filename_relation =  input("Geben sie den Relation Filename ein, den Sie bearbeiten möchten (bspw.: relation.csv): ")

#   Zugriff auf die CSV Dateien.
file_node = raw_data_dir + "/node_test.csv"
file_way = raw_data_dir + "/way_test.csv"
file_relation = raw_data_dir + "/relation_test.csv"

#   Erstellen Nodes
df_nodes_alt =pd.read_csv(file_node)
#   Erstellen der Geometrie
df_nodes_alt [["longitude", "latitude"]]=df_nodes_alt.Location.str.split("/", expand=True)
#   df_nodes_alt.drop("Location")
geometry= gpd.points_from_xy(df_nodes_alt.longitude, df_nodes_alt.latitude)     # Point Geometrie mithilfe der longitude und Latitude
geo_df = gpd.GeoDataFrame(df_nodes_alt, geometry=geometry)                  # Erstellung der Geopandas Tabelle in die die Geometry eingetragen wird.
#   print(df_nodes_alt.head().to_string())
df_nodes_alt = df_nodes_alt.drop(["Location"], axis=1)      #Löschen des Columns Location
df_nodes_alt.rename(columns={"ID":"node_id"}, inplace=True)    # Umbennen des Columns ID, damit bei mehreren Tabellen die ID unter demselben Column steht (vereinfacht das zusammenfügen bei Betrachtung eines Indexes)
df_nodes = df_nodes_alt[["node_id", "power", "geometry"]].copy()  # Erstellung der table: nodes mit den nötigen columns
#print(df_nodes.head().to_string())

#   Erstellen Relation_Members // Es fehlt noch member type N (node) oder W (way) sowie member_role
df_relation_members_alt=pd.read_csv(file_relation)
df_relation_members_alt["Nodes"]=df_relation_members_alt["Nodes"].apply(ast.literal_eval)       # Zugriff auf die einzelnen Werte der Liste: "Members"
df_relation_members_alt_memb = df_relation_members_alt["Nodes"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")   # Die Members werden einzelnt untereinander geschrieben und einem neuen Index zugeordnet
df_relation_members = pd.merge(df_relation_members_alt["ID"], df_relation_members_alt_memb,  right_index=True, left_index=True).rename(columns={'value':'member_id', 'ID':'relation_id'})    # Die einzelnen Members werden der relation zugeorndet
df_relation_members["sequential_ID"]=df_relation_members.groupby(["relation_id"]).cumcount()      # Erstellung einer sequentiell ID (abzählen wie häufig eine relation nacheinander kommt, somit Anzahl der Member
#pd.merge(pd.merge(df_relation_members_alt_memb,left_index=True,right_index=True),df_relation_members_alt[["ID"]], left_index=True, right_index=True).rename(columns={'value_x':'Members'})
#print(df_relation_members.to_string())
#print(df_relation_members_alt.head().to_string())

#   Erstellen der Way_nodes Tabelle
df_way_nodes_alt=pd.read_csv (file_way)
df_way_nodes_alt["Nodes"]=df_way_nodes_alt["Nodes"].apply(ast.literal_eval)
df_way_nodes_alt_nt=df_way_nodes_alt["Nodes"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")
df_way_nodes = pd.merge(df_way_nodes_alt["ID"],df_way_nodes_alt_nt, right_index=True, left_index=True).rename(columns={'value':'node_id', 'ID':'way_id'})
df_way_nodes["sequential_ID"]=df_way_nodes.groupby(["way_id"]).cumcount()
#df_way_nodes_alt_ntt = df_way_nodes_alt["Nodes"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")
#df_way_nodes_new = pd.merge(df_way_nodes["node_id"], df_nodes["ID"], right_index=True, left_index=True)
#print(df_way_nodes)

#   Erstellung power_ways Table // Geometry der Ways
#print(df_geometry_index)
df_power_ways_alt = df_way_nodes
df_power_ways_alt["node_id"]=df_power_ways_alt["node_id"].astype("str")     # Umschreiben der beiden IDs zu String, da eine Object und eine Integer ist.
df_nodes["node_id"]=df_nodes["node_id"].astype("str")
df_power_ways_alt = pd.merge(df_power_ways_alt, df_nodes, how="left", on="node_id")     # Zusammenfügen der Ways und Nodes Tabelle um die Geometry über die Ways zu legen
df_power_ways_alt_2 = df_power_ways_alt[["way_id", "geometry"]].copy()            # Erstellung der power_ways tabelle mit den Einzelnen Punkten der Geometrie
df_power_ways_alt_2= df_power_ways_alt_2.rename(columns={"way_id":"ID"})        # rename way_id for adding new columns
df_power_ways = df_power_ways_alt_2.groupby(["ID"])["geometry"].agg(lambda x: LineString(x.tolist()))   # Die einzelnen Punkte werden zusammengefasst und zum Linestring umgewandelt für jeden Way
#df_power_ways=df_power_ways_alt_2.groupby("way_id").agg({"geometry":list})
df_power_way_columns = df_way_nodes_alt[["ID", "power", "voltage", "cables", "wires", "circuits", "frequency", "name"]].copy()      # add needed columns
df_power_ways = pd.merge(df_power_ways, df_power_way_columns, how="left", on="ID")
df_power_ways=gpd.GeoDataFrame(df_power_ways, geometry="geometry")      # Erstellen der Geopandasdataframes
#print(df_power_ways.head().to_string())
#df_power_ways.plot()
#plt.show()
#gpd_ways = gpd.GeoDataFrame(df_power_ways, geometry=geometry)

# Erstellen Power_relations table
df_power_relations = pd.read_csv(file_relation)
df_power_relations_new = df_power_relations[["ID", 'voltage', "cables", "wires", "frequency", "Nodes"]]  # drop unwanted columns
df_power_relations_new = df_power_relations_new.reindex(columns=["ID", 'voltage', "cables", "wires", "circuits", "frequency", "Members"])  # reorder columns

#df_power_relations_new = df_power_relations[["ID", 'voltage', "cables", "wires", "circuits", "frequency", "Nodes"]]  # drop unwanted columns
#df_power_relations_new = df_power_relations_new.reindex(columns=["ID", 'voltage', "cables", "wires", "circuits", "frequency", "Nodes"])  # reorder columns 5
#print(df_power_relations_new.head().to_string())

#Erstellen power_relations_applied_changes
df_power_relations_applied_changes = df_power_relations.copy()
#print(df_power_relations.head().to_string())

# Erstellung der Relations table
relations = {'id': [], 'version': [], 'user_id': [], 'tstamp': [], 'changeset_id': [], 'tags': []}
df_relations_new = pd.DataFrame(relations)
df_relations = pd.read_csv(file_relation)
columns = list(df_relations.columns)
#print(columns)
columns.remove('Unnamed: 0')
columns.remove('ID')
columns.remove('Nodes')
#print(columns)
df_relations_new['id'] = df_relations['ID'].copy()
# print(len(df_ways))
# print(len(columns))
for i in range(0, len(df_relations)):
    tag_string = ''
    for a in range(0, len(columns)):
        key = columns[a]
        if len(str(df_relations[key].loc[i])) > 1:
            tag_string = tag_string + '"' + str(key) + '"' + '=>' + '"' + str(df_relations[key].loc[i]) + '"' + ', '
    # print(tag_string)
    df_relations_new.loc[i, 'tags'] = tag_string
# df_relations_new.to_csv('test.csv')


# Erstellung der Ways table
ways = {'id': [], 'version': [], 'user_id': [], 'tstamp': [], 'changeset_id': [], 'tags': [], 'nodes': []}
df_ways_new = pd.DataFrame(ways)
df_ways = pd.read_csv(file_way)
columns = list(df_ways.columns)
#print(columns)
columns.remove('Unnamed: 0')
columns.remove('ID')
columns.remove('Nodes')
#print(columns)
df_ways_new['id'] = df_ways['ID'].copy()
df_ways_new['nodes'] = df_ways['Nodes'].copy()
# print(len(df_ways))
# print(len(columns))
for i in range(0, len(df_ways)):
    tag_string = ''
    for a in range(0, len(columns)):
        key = columns[a]
        if len(str(df_ways[key].loc[i])) > 1:
            tag_string = tag_string + '"' + str(key) + '"' + '=>' + '"' + str(df_ways[key].loc[i]) + '"' + ', '
    # print(tag_string)
    df_ways_new.loc[i, 'tags'] = tag_string
# df_ways_new.to_csv('test.csv')

#   Umsetzung des Power_scripts

#   Create the Topologie-Tabellen

#--Bus Data TODO: Index  line 57-65
bus_data_columns = {"id":[], 'cnt': [], 'the_geom': [], 'voltage': [], 'substation_id': [], 'buffered': []}
df_bus_data = pd.DataFrame(bus_data_columns)

#--branch_data TODO: Index line 72-84
branch_data_columns = {"branch_id":[], 'relation_id': [], 'line_id': [], 'length': [], 'way geometry': [], 'f_bus': [], 't_bus': [], 'voltage': [], 'cables': [], 'wires': [], 'frequency':[], "power":[]}
df_branch_data = pd.DataFrame(branch_data_columns)

#   Create power_ways_applied_changes       
df_power_ways_applied_changes = df_power_ways.copy()
#print(df_power_ways_applied_changes.head().to_string())

#   Change the Voltage of 400kV     line 106
df_power_relations_applied_changes["frequency"] = df_power_relations_applied_changes["frequency"].astype("str")  #change from float to string for the if function

df_power_relations_applied_changes["voltage"].loc[(df_power_relations_applied_changes["frequency"]== "50") & (df_power_relations_applied_changes["voltage"]=="400000")] ="380000"


#   Create table power_line     114-118
df_power_line = df_power_ways_applied_changes[df_power_ways_applied_changes.isin(["line", "cable"]).any(axis = 1)]       # Select only the lines with power = cable or line
#TODO Set a new Index!!
#df_power_line.set_index("ID", drop=False)
#print(df_power_line.head().to_string())

#   Create table power_substation   128-148
df_power_substation = df_power_ways_applied_changes[df_power_ways_applied_changes.isin(['substation','sub_station','station', 'plant']).any(axis = 1)]  #select only lines where Power is 'substation','sub_station','station', 'plant'
for x in df_power_substation.index:
    if len(df_power_substation["geometry"][x].coords) < 4:          #delete geometry with less than 4 Points (you need 4 Points for a Polygon)
        df_power_substation.drop(index=x, inplace= True)


df_power_substation_geometry = df_power_substation["geometry"].agg(lambda x: Polygon(x))        # Create Polygon out of Linestrings
df_power_substation = gpd.GeoDataFrame(df_power_substation, geometry = df_power_substation_geometry)    # create the gpd with the Polygons
#for x in df_power_substation ["geometry"]:
 #   if

df_power_substation = df_power_substation.reset_index()
#print(df_power_substation.head().to_string())
#df_power_substation.plot()         #Create the Plot
#plt.show()
# Check if geometry is empty :print(df.is_empty)

#   152 create Points in the center of the Polygons, If its outside the nearest point in the polygon to the center is chosen.
df_power_substation["Point"]=""
df_power_substation["var_cent"] = df_power_substation["geometry"].centroid
df_power_substation["var_result"] = df_power_substation["var_cent"]
df_power_substation["point_intersect"]  = df_power_substation["geometry"].intersects(df_power_substation["var_result"])
df_power_substation["Point"].loc[df_power_substation["point_intersect"]==True] = df_power_substation["var_result"].loc[df_power_substation.index]

df_point_outside = pd.DataFrame()
df_point_outside["geometry"] = df_power_substation["geometry"].loc[df_power_substation["point_intersect"] ==False]
df_point_outside["var_result"] = df_power_substation["var_cent"].loc[df_power_substation["point_intersect"]==False]

for x in df_point_outside.index:
    geometry = df_point_outside.loc[x, "geometry"]
    p1, p2 = nearest_points (geometry, df_point_outside.loc[x, "var_result"])
    var_result = p1
    df_power_substation.loc[x, "Point"] = var_result

   #####    Funktion umgesetzt 
# def point_inside_geometry(df):
#     for x in df.point_outside.index:
#         geometry = df.loc[x, "geometry"]
#         var_cent = geometry.centroid
#         var_result = var_cent
        
    
#         if geometry.intersects(var_result) == False:
#             p1, p2 = nearest_points (geometry, var_result)
#             var_result = p1
            
#         df.loc[x, "Point"] = var_result

# point_inside_geometry(df_power_substation)


#for y in df_power_substation.index:
#    geometry=df_power_substation["geometry"]
#    point = df_power_substation.loc[y, "Point"]
#    if geometry.contains(point) == True and df_power_substation["power"] == "plant" and df_power_substation["ID"] != df_power_substation.loc[y, "ID"]:
#        df_power_substation.drop(index = y, inplace =True)
#df_power_substation.set_index("index", inplace=True)
#df_power_substation["contains"] = ""
#df_power_substation["contains"] = df_power_substation.agg(lambda y: df_power_substation.loc["geometry"].contains(df_power_substation.iloc[y, "Point"]).loc[(df_power_substation["power"] == "plant") & (df_power_substation["ID"] != df_power_substation.iloc[y, "ID"])])
#df_power_substation["contains"] = df_power_substation.loc["geometry"].contains(df_power_substation.loc[y, "Point"]).loc[(df_power_substation["power"] == "plant") & (df_power_substation["ID"] != df_power_substation.loc[y, "ID"])].drop(inplace=True)
#df_power_substation["power"].loc[(df_power_substation["power"]=="plant") & (df_power_substation["geometry"].contains(df_power_substation.loc[484, "Point"])==False) & (df_power_substation["ID"]!=df_power_substation.loc[484, "ID"])]
#df_power_substation["ID"].loc[(df_power_substation["power"]=="plant") & (df_power_substation["geometry"].contains(df_power_substation.loc[y, "Point"])==False) & (df_power_substation["ID"]!=df_power_substation.loc[484, "ID"])]

# #   156     Nochmal genau angucken
# for x in df_power_substation.index:
#     geometry=df_power_substation.loc[x, "geometry"]
#     for y in df_power_substation.index:
#         point = df_power_substation.loc[y, "Point"]
#         if geometry.contains(point) == True and df_power_substation.loc[x, "power"] == "plant" and df_power_substation.loc[x, "ID"] != df_power_substation.loc[y, "ID"]:
#             df_power_substation.drop(index = x, inplace =True)
            
# #df_power_substation["geometry"].contains(df_power_substation["Point"])
    
# for y in df_power_substation.index:
#     point = df_power_substation.loc[y, "Point"]
#     geometry_y = df_power_substation.loc[y, "geometry"]
#     for y in df_power_substation.index:
#         geometry=df_power_substation.loc[x, "geometry"]
#         if geometry.contains(point) == True and (df_power_substation.loc[y, "power"] == "plant" or (df_power_substation.loc[y, "power"] != "plant" and df_power_substation.loc[x, "power"] != "plant")) and df_power_substation.loc[x, "ID"] != df_power_substation.loc[y, "ID"] and geometry_y.area < geometry.area:
#             df_power_substation.drop(index = y, inplace =True)
            



# Get endpoints and Startpoints from Linestring for Power_line      173-184
df_power_line = df_power_line.reset_index()
df_power_line["startpoint"]=""
df_power_line["endpoint"] = ""
df_power_line["startpoint"] = df_power_line["geometry"].agg(lambda x: Point(x.coords[0]))
df_power_line["endpoint"] = df_power_line["geometry"].agg(lambda x: Point(x.coords[-1]))



#   243
df_power_line["point_substation_id_1"]=np.empty((len(df_power_line), 0)).tolist()
df_power_line["point_substation_id_2"]=np.empty((len(df_power_line), 0)).tolist()
df_power_line["point_substation_id_start"]=np.empty((len(df_power_line), 0)).tolist()
df_within_start = pd.DataFrame
df_within_end = pd.DataFrame


#df_within_start= df_power_substation["geometry"].agg(lambda x: df_power_line["startpoint"].within(x))==True 
#df_contains_start = df_power_line["startpoint"].agg(lambda x: df_power_substation["geometry"].contains(x))==True 

# df_point_contains_start = df_contains_start.apply(lambda row: row[row == 1].index.tolist() , axis=1)
# df_power_line["point_within_start"] = df_point_contains_start
# df_point_contains_split = pd.DataFrame(df_power_line["point_within_start"].to_list())
# df_point_contains_split=df_point_contains_split.fillna(0)

# df_point_within_start = ((df_within_start == True).idxmax(axis=1))

# #df_point_within_start = df_within_start.apply(lambda row: row[row == 1].index.tolist() , axis=1)
# df_power_substation["point_within_start"] = df_point_within_start
# #df_point_within_split = pd.DataFrame(df_power_substation["point_within_start"].to_list())
# #df_point_within_split=df_point_within_split.fillna(0)
# #x = df_power_substation["point_within_start"].str.len()
# for x in range(df_power_substation["point_within_start"].str.len().max()):
#     df_power_substation["point_within_start_"+str(x)]=df_point_within_split[x]
#     df_power_line["point_substation_id_"+str(x)]=np.empty((len(df_power_line), 0)).tolist()
#     df_power_line["point_substation_id_"+str(x)].loc[df_power_substation["point_within_start_"+str(x)]] = df_power_substation["ID"]
#     # df_power_line["point_substation_id_"+str(x)].append(df_power_line["point_substation_id_"+str(x)].loc[df_power_substation["point_within_start_"+str(x)]] = df_power_substation["ID"])
#     df_power_line["point_substation_id_start"].append(df_power_line["point_substation_id_"+str(x)])
# #     x=x+1
# # #df_power_line["point_substation_id_1"].loc[df_power_substation["point_within_start"]] = df_power_substation["ID"]
# df_contains_start = df_power_line["startpoint"].agg(lambda x: df_power_substation["geometry"].contains(x))==True 
# df_point_contains_start = df_contains_start.apply(lambda row: row[row == 1].index.tolist() , axis=1)
# df_power_line["point_within_start"] = df_point_contains_start
# df_point_contains_split = pd.DataFrame(df_power_line["point_within_start"].to_list())
# df_point_contains_split=df_point_contains_split.replace({np.nan:None})
# df_power_line["point_within_start"] = df_point_contains_split[0]


# df_within_end= df_power_line["endpoint"].agg(lambda x: df_power_substation["geometry"].contains(x))==True 
# df_point_within_end_2 = ((df_within_end == True).idxmax(axis=1))
# df_power_line["point_within_end"] = df_point_within_end_2
# df_power_line["point_substation_id_2"] = df_power_substation["ID"].loc[df_power_line["point_within_end"]].reset_index()["ID"]

#   Get the indexes where the geometry contains the point. Then get just the first Index and the ID of the geometry and write it in Line
df_within_start = df_power_line["startpoint"].agg(lambda x: df_power_substation["geometry"].contains(x))==True
df_within_start["indexes"] = df_within_start.apply(lambda row: row[row == 1].index.tolist() , axis=1)
df_power_line["point_within_start"] = df_within_start["indexes"].str[0]
df_power_line["point_substation_id_1"].loc[df_power_line["point_within_start"] >=0] = df_power_substation["ID"]


df_within_end = df_power_line["endpoint"].agg(lambda x: df_power_substation["geometry"].contains(x))==True
df_within_end["indexes"] = df_within_end.apply(lambda row: row[row == 1].index.tolist() , axis=1)
df_power_line["point_within_end"] = df_within_end["indexes"].str[0]
df_power_line["point_substation_id_2"].loc[df_power_line["point_within_end"] >=0] = df_power_substation["ID"]

#   Delete where point 1 and 2 have the same ID
df_power_line.drop(df_power_line.loc[df_power_line["point_substation_id_1"]==df_power_line["point_substation_id_2"]].index.to_list(), inplace=True)


# for x in df_power_line.index:
#     start_point = df_power_line.loc[x, "startpoint"]
#     end_point = df_power_line.loc[x, "endpoint"]
#     for y in df_power_substation.index:
#         poly = df_power_substation.loc[y, "geometry"]
#         if start_point.within(poly) == True:
#             df_power_line.loc[x, "point_substation_id_1"].append(df_power_substation.loc[y, "ID"])
#         if end_point.within(poly) == True:
#             df_power_line.loc[x, "point_substation_id_2"].append(df_power_substation.loc[y, "ID"])

# for x in df_power_line.index:
#     start_id = df_power_line.loc[x, "point_substation_id_1"]
#     end_id = df_power_line.loc[x, "point_substation_id_2"]   
#     print(start_id, end_id)
#     if str(start_id) == str(end_id):
#         df_power_line.drop(index=x , inplace= True)
        
#   Untersuchung Spannungsebene     266

df_power_line["numb_volt_lev"]=""
# TODO
df_power_line["numb_volt_lev"] = df_power_line["voltage"].replace(r'^\s*$', None, regex=True).loc[df_power_line["voltage"].isnull()==False].str.count(";") +1
df_power_line["numb_volt_lev"].loc[df_power_line["numb_volt_lev"].isnull()==True] = 0
#df_power_line["numb_volt_lev2"] =df_power_line["voltage"].agg(lambda x: x.replace(";", ""))
#df_power_line["numb_volt_lev3"] = df_power_line["voltage"]
#df_power_line["numb_volt_lev"] = df_power_line["voltage"].str.len()-df_power_line["numb_volt_lev2"].str.len()+1


#   277

df_power_line_voltage_array=pd.DataFrame
df_power_line_voltage_array = df_power_line.voltage.str.split(";", expand= True)
df_power_line["voltage_array_1"]=""
df_power_line["voltage_array_2"]=""
df_power_line["voltage_array_3"]=""
df_power_line["voltage_array_4"]=""

for x in range(int(df_power_line["numb_volt_lev"].max())):
    df_power_line["voltage_array_"+str(x+1)] = df_power_line_voltage_array[x]
    df_power_line["voltage_array_"+str(x+1)].loc[df_power_line["voltage_array_"+str(x+1)] == "60000"] = "110000"
df_power_line["voltage_array_1"]
# TODO 303 Wird hier alles gelöscht oder nur einzelne Werte? Das ist in Pandas nicht möglich und bereits als NOne gespeichert


#   322 add Cables
# TODO nur für line einfügen
df_power_line["cables_sum"]=""
df_power_line["cables_sum"] = df_power_line["cables"]
df_power_line["cables_sum"].loc[df_power_line["cables_sum"].isna()==True]= "0"
df_power_line["cables_sum"].loc[df_power_line["cables_sum"].str.contains(r'^\s*$', na=False)] = "0"

v_semic_numb = df_power_line["cables_sum"].str.count(";")+1
numbers = df_power_line.cables_sum.str.split(";", expand = True)
numbers = numbers.apply(pd.to_numeric)
v_sum = numbers.sum(axis=1, numeric_only = True)
df_power_line["cables_sum"] = v_sum

#   adds the value of the cables if there are as much values for the voltage as for the cables
df_power_line["cables_array_1"]=""
df_power_line["cables_array_2"]=""
df_power_line["cables_array_3"]=""
df_power_line["cables_array_4"]=""

for x in range((v_semic_numb).max()):
    df_power_line["cables_array_"+str(x+1)]=""
    df_power_line["cables_array_"+str(x+1)].loc[df_power_line["numb_volt_lev"]
                                                == v_semic_numb] = numbers[x]


#   340 Mark all substations (not plants), which have 110kV connection. Thus they connect lower voltage grids.
df_power_substation["numb_volt_lev"]=""
# df_power_substation["numb_volt_lev"] = df_power_substation.voltage.str.count(";")+1
df_power_substation["numb_volt_lev"] = df_power_substation["voltage"].replace(r'^\s*$', None, regex=True).loc[df_power_substation["voltage"].isnull()==False].str.count(";") +1
df_power_substation["numb_volt_lev"].loc[df_power_substation["numb_volt_lev"].isnull()==True] = 0
numbers_voltage_substation = df_power_substation.voltage.str.split(";", expand = True)

#numbers_voltage_substation = numbers_voltage_substation.apply(pd.to_numeric, errors="ignore")

df_power_substation["connection_110kV"] = ""
df_power_substation["voltage_array_1"] = ""
df_power_substation["voltage_array_2"] = ""
df_power_substation["voltage_array_3"] = ""
df_power_substation["voltage_array_4"] = ""

# Using For schleife, so no hard coding is needed for the number of different voltages.
#   TODO drop columns
for x in range((df_power_substation.voltage.str.count(";")).max()):
    numbers_voltage_substation[x] = numbers_voltage_substation[x].fillna("")
    df_power_substation["int"+str(x)] = numbers_voltage_substation[x].agg(lambda y: y.isnumeric())
    df_power_substation["voltage_array_"+str(x+1)]=numbers_voltage_substation[x].loc[df_power_substation["int"+str(x)]]
    df_power_substation["voltage_array_"+str(x+1)].loc[df_power_substation["voltage_array_"+str(x+1)] == "60000"] = "110000"
    df_power_substation["connection_110kV"].loc[df_power_substation["voltage_array_"+str(x+1)] == "110000"] = True
  

#   Consider all substations which have power lines with 110kV and end or start in a substation also connected to 110kV 
df_power_substation["connection_110kV"].loc[df_power_line["point_within_start"].loc[(df_power_line["point_within_start"]>=0) & ((df_power_line["voltage_array_1"] == "110000") | (df_power_line["voltage_array_2"] == "110000")| (df_power_line["voltage_array_3"] == "110000")| (df_power_line["voltage_array_4"] == "110000"))]] = True
df_power_substation["connection_110kV"].loc[df_power_line["point_within_end"].loc[(df_power_line["point_within_end"]>=0) & ((df_power_line["voltage_array_1"] == "110000") | (df_power_line["voltage_array_2"] == "110000")| (df_power_line["voltage_array_3"] == "110000")| (df_power_line["voltage_array_4"] == "110000"))]] =True

df_power_substation.drop("voltage_array_1", inplace =True, axis=1)
df_power_substation.drop("voltage_array_2", inplace =True, axis=1)
df_power_substation.drop("voltage_array_3", inplace =True, axis=1)
df_power_substation.drop("voltage_array_4", inplace =True, axis=1)

                
                
# Create a column including the wires as integers
df_power_line["wires_array"]=""
df_power_line["wires_array"] = df_power_line["wires"].loc[(df_power_line["wires"].str.count(";")==0) & (df_power_line["wires"]!=np.isnan) & (df_power_line["power"]=="line")]

df_power_line["wires_array"].loc[df_power_line["wires_array"]=="quad"]=4
df_power_line["wires_array"].loc[df_power_line["wires_array"]=="triple"]=3
df_power_line["wires_array"].loc[df_power_line["wires_array"]=="double"]=2
df_power_line["wires_array"].loc[df_power_line["wires_array"]=="single"]=1


#   Create column including the Frequency when it's not null and it has no semicolons

df_power_line["frequency_array_1"] =""
df_power_line["frequency_array_2"] =""
df_power_line["frequency_array_3"] =""
df_power_line["frequency_array_4"] =""
df_power_line["frequency_array_1"] = df_power_line["frequency"].loc[(df_power_line["frequency"].str.count(";")==0) & (df_power_line["numb_volt_lev"]>0) & (df_power_line["power"]=="line") & (df_power_line["frequency"].agg(lambda x: x.replace(".","")).str.isnumeric())]
df_power_line["frequency_array_2"] = df_power_line["frequency"].loc[(df_power_line["frequency"].str.count(";")==0) & (df_power_line["numb_volt_lev"]>(1)) & (df_power_line["frequency"]!=np.isnan) & (df_power_line["power"]=="line") & (df_power_line["frequency"].agg(lambda x: x.replace(".","")).str.isnumeric())]
df_power_line["frequency_array_3"] = df_power_line["frequency"].loc[(df_power_line["frequency"].str.count(";")==0) & (df_power_line["numb_volt_lev"]>(2)) & (df_power_line["frequency"]!=np.isnan) & (df_power_line["power"]=="line") & (df_power_line["frequency"].agg(lambda x: x.replace(".","")).str.isnumeric())]
df_power_line["frequency_array_4"] = df_power_line["frequency"].loc[(df_power_line["frequency"].str.count(";")==0) & (df_power_line["numb_volt_lev"]>3) & (df_power_line["frequency"]!=np.isnan) & (df_power_line["power"]=="line") & (df_power_line["frequency"].agg(lambda x: x.replace(".","")).str.isnumeric())]

#df_power_line["frequency"].agg(lambda x: x.replace(".","")).str.isnumeric()     # Used for getting also float numbers


#   Create column thats measuring the length of the lines
#TODO was ist das für eine Länge ???? Mit der Geometrie auseinandersetzen bzgl EPSG etc...
df_power_line["length"] = df_power_line["geometry"].length
# df_power_line["geometry"] = df_power_line["geometry"].set_crs(epsg="4326")
# df_power_line["geometry"] = df_power_line["geometry"].to_crs(4326)


#   Neighbours 451
#v_id = df_power_line.copy()
#v_volt = v_id.voltage_array[1]
#v_count = 0

    
# Creates a Dataframe with every Index    
df_neighbours_startpoint = pd.DataFrame
df_neighbours_endpoint = pd.DataFrame

df_power_line["index_2"]=df_power_line.index
#df_power_line["index_2"].agg(lambda x: df_power_line["startpoint"].loc[df_power_line["ID"]!= df_power_line["ID"][x]].geom_equals(df_power_line["startpoint"][x]))
#df_power_line["voltage"] = df_power_line["voltage"].fillna(0, inplace = True)
df_power_line["voltage_array_1"]= df_power_line["voltage_array_1"].replace(r'^\s*$', None, regex=True)
df_power_line["voltage_array_2"]= df_power_line["voltage_array_2"].replace(r'^\s*$', None, regex=True)
df_power_line["voltage_array_3"]= df_power_line["voltage_array_3"].replace(r'^\s*$', None, regex=True)
df_power_line["voltage_array_4"]= df_power_line["voltage_array_4"].replace(r'^\s*$', None, regex=True)


# df_neighbours_startpoint= df_power_line["index_2"].loc[df_power_line["voltage_array_1"].isnull() == False].agg(lambda x: df_power_line["startpoint"].loc[(df_power_line["ID"]!= df_power_line["ID"][x])].geom_equals(df_power_line["startpoint"][x]))
# df_neighbours_endpoint = df_power_line["index_2"].agg(lambda x: df_power_line["endpoint"].loc[df_power_line["ID"]!= df_power_line["ID"][x]].geom_equals(df_power_line["startpoint"][x]))
# # df_neighbours_startpoint = df_power_line["startpoint"].agg(lambda x: df_power_line["startpoint"].geom_equals(x)) 
# # df_neighbours_endpoint = df_power_line["startpoint"].agg(lambda x: df_power_line["endpoint"].geom_equals(x))
# df_neighbours_startpoint["indexes"] = df_neighbours_startpoint.apply(lambda row: row[row == 1].index.tolist() , axis=1)
# df_neighbours_endpoint["indexes"] = df_neighbours_endpoint.apply(lambda row: row[row == 1].index.tolist() , axis=1)


#Auswahl Variablen die bei Spannung null sind
#df_neighbours_startpoint= df_power_line["index_2"].loc[df_power_line["voltage_array_1"].isnull()].agg(lambda x: df_power_line["startpoint"].loc[(df_power_line["ID"]!= df_power_line["ID"][x])].geom_equals(df_power_line["startpoint"][x]))
#df_power_line["voltage_array_1"].loc[df_power_line["voltage_array_1"].isnull()==False] = df_power_line["voltage_array_1"].loc[df_power_line["voltage_array_1"].isnull()==False].agg(lambda x: int(x))
#Implementieren von Spannungsebene muss enthalhten sein!



# df_neighbours_startpoint= df_power_line["index_2"].loc[df_power_line["voltage_array_1"].isnull() == False].agg(lambda x: df_power_line["startpoint"]
#                                                     .loc[((df_power_line["voltage_array_1"][x] == df_power_line["voltage_array_1"]) | 
#                                                        (df_power_line["voltage_array_1"][x] == (df_power_line["voltage_array_2"])) | 
#                                                        (df_power_line["voltage_array_1"][x] == (df_power_line["voltage_array_3"])) | 
#                                                        (df_power_line["voltage_array_1"][x] == (df_power_line["voltage_array_4"]))) & (df_power_line["ID"]!= df_power_line["ID"][x]) ].geom_equals(df_power_line["startpoint"][x]))
# df_neighbours_startpoint["indexes"] = df_neighbours_startpoint.apply(lambda row: row[row == 1].index.tolist() , axis=1)

#   Neighbours function to identify possible neighbours for every line
df_neighbours_startpoint_indexes = df_power_line[["index_2"]].copy()
df_neighbours_endpoint_indexes = df_power_line[["index_2"]].copy()
for y in range(1,5,1):
    if len(df_power_line["voltage_array_"+str(y)].loc[df_power_line["voltage_array_"+str(y)].isnull() ==True]) != len(df_power_line["voltage_array_"+str(y)]):
        df_neighbours_startpoint= df_power_line["index_2"].loc[df_power_line["voltage_array_"+str(y)].isnull() == False].agg(lambda x: df_power_line["startpoint"]
                                                            .loc[((df_power_line["voltage_array_"+str(y)][x] == df_power_line["voltage_array_1"]) | 
                                                               (df_power_line["voltage_array_"+str(y)][x] == (df_power_line["voltage_array_2"])) | 
                                                               (df_power_line["voltage_array_"+str(y)][x] == (df_power_line["voltage_array_3"])) | 
                                                               (df_power_line["voltage_array_"+str(y)][x] == (df_power_line["voltage_array_4"]))) & (df_power_line["ID"]!= df_power_line["ID"][x]) ].geom_equals(df_power_line["startpoint"][x]))
        
        if df_neighbours_startpoint.empty == False:
            df_neighbours_startpoint["indexes"] = df_neighbours_startpoint.apply(lambda row: row[row == 1].index.tolist() , axis=1)
            df_neighbours_startpoint_indexes["index"+str(y)] = df_neighbours_startpoint["indexes"]

        df_neighbours_endpoint= df_power_line["index_2"].loc[df_power_line["voltage_array_"+str(y)].isnull() == False].agg(lambda x: df_power_line["endpoint"]
                                                            .loc[((df_power_line["voltage_array_"+str(y)][x] == df_power_line["voltage_array_1"]) | 
                                                               (df_power_line["voltage_array_"+str(y)][x] == (df_power_line["voltage_array_2"])) | 
                                                               (df_power_line["voltage_array_"+str(y)][x] == (df_power_line["voltage_array_3"])) | 
                                                               (df_power_line["voltage_array_"+str(y)][x] == (df_power_line["voltage_array_4"]))) & (df_power_line["ID"]!= df_power_line["ID"][x]) ].geom_equals(df_power_line["startpoint"][x]))
        if df_neighbours_endpoint.empty == False:    
            df_neighbours_endpoint["indexes"] = df_neighbours_endpoint.apply(lambda row: row[row == 1].index.tolist() , axis=1)
            df_neighbours_endpoint_indexes["index"+str(y)] = df_neighbours_endpoint["indexes"]
            

# df_power_line["cables_array_1"]= df_power_line["cables_array_1"].replace(r'^\s*$', 0, regex=True)
# df_power_line["cables_array_2"]= df_power_line["cables_array_2"].replace(r'^\s*$', 0, regex=True)
# df_power_line["cables_array_3"]= df_power_line["cables_array_3"].replace(r'^\s*$', 0, regex=True)
# df_power_line["cables_array_4"]= df_power_line["cables_array_4"].replace(r'^\s*$', 0, regex=True)

cables_array = df_power_line[["cables_array_1","cables_array_2", "cables_array_3", "cables_array_4"]].replace(r'^\s*$', 0, regex=True).copy()
cables_array["cables_array_1"].loc[cables_array["cables_array_1"]>0]=1
cables_array["cables_array_2"].loc[cables_array["cables_array_2"]>0]=1
cables_array["cables_array_3"].loc[cables_array["cables_array_3"]>0]=1
cables_array["cables_array_4"].loc[cables_array["cables_array_4"]>0]=1
cables_array["v_numb_known_cable_lev"] = 0

frequency_array = df_power_line[["frequency_array_1","frequency_array_2", "frequency_array_3", "frequency_array_4"]].replace(np.nan, 0, regex=True).copy()
frequency_array["frequency_array_1"].loc[frequency_array["frequency_array_1"].astype(float)>0]=1
frequency_array["frequency_array_2"].loc[frequency_array["frequency_array_2"].astype(float)>0]=1
frequency_array["frequency_array_3"].loc[frequency_array["frequency_array_3"].astype(float)>0]=1
frequency_array["frequency_array_4"].loc[frequency_array["frequency_array_4"].astype(float)>0]=1
frequency_array["v_numb_known_frequency_lev"]=0

for x in range(int(df_power_line["numb_volt_lev"].max())):
    
    cables_array["v_numb_known_cable_lev"] += (cables_array["cables_array_"+str(x+1)])# + cables_array_2 + cables_array_3 + cables_array_4

    frequency_array["v_numb_known_frequency_lev"] += frequency_array["frequency_array_"+str(x+1)]



v_count_end = (df_power_line["numb_volt_lev"] - cables_array["v_numb_known_cable_lev"]).sum() + (df_power_line["numb_volt_lev"] -frequency_array["v_numb_known_frequency_lev"]).sum()

df_power_line["numb_volt_lev"] - cables_array["v_numb_known_cable_lev"]
# df_neighbours_startpoint= df_power_line["index_2"].loc[df_power_line["voltage_array_1"].isnull() == False].agg(lambda x: df_power_line["startpoint"]
#                                                     .loc[((df_power_line["voltage_array_1"][x] == (df_power_line["voltage_array_2"])) & (df_power_line["ID"]!= df_power_line["ID"][x])) ].geom_equals(df_power_line["startpoint"][x]))

# ((df_power_line["voltage_array_1"] == df_power_line["voltage_array_1"][x]) | (df_power_line["voltage_array_1"] == df_power_line["voltage_array_2"][x]) | (df_power_line["voltage_array_1"] == df_power_line["voltage_array_3"][x]) | (df_power_line["voltage_array_1"] == df_power_line["voltage_array_4"][x]))
# len(df_power_line["voltage_array_"+str(y)].loc[df_power_line["voltage_array_"+str(y)].isnull() ==True]) == len(df_power_line["voltage_array_"+str(y)])

timer_end = time.time()
print('Runtime ' + str(round((timer_end-timer_start)/60, 2)) + ' Minutes')

    
    