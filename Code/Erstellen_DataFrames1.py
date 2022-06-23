# Derk Test

import geopandas as gpd
import pandas as pd
import numpy as np
import json
from matplotlib import pyplot as plt
#env Datei erstellen.
#import geoalchemy2
#import psycopg2
import fiona
import matplotlib
from shapely.geometry import shape
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry import Polygon
from shapely.ops import polygonize
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
data_path=os.getcwd()
raw_data_dir = os.path.dirname(os.getcwd()) + "/raw_data"
print(raw_data_dir)

#   Eingabe des / der Filenames:
#filename_node = input("Geben sie den Node Filename ein, den Sie bearbeiten möchten (bspw.: Node.csv): ")
#filename_way = input("Geben sie den Way Filename ein, den Sie bearbeiten möchten (bspw.: way.csv): ")
#filename_relation =  input("Geben sie den Relation Filename ein, den Sie bearbeiten möchten (bspw.: relation.csv): ")

#   Zugriff auf die CSV Dateien.
file_node = raw_data_dir + "/node_test.csv"
file_way = raw_data_dir +"/way_test.csv"
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
df_relation_members_alt["Members"]=df_relation_members_alt["Members"].apply(ast.literal_eval)       # Zugriff auf die einzelnen Werte der Liste: "Members"
df_relation_members_alt_memb = df_relation_members_alt["Members"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")   # Die Members werden einzelnt untereinander geschrieben und einem neuen Index zugeordnet
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
print(df_power_ways.head().to_string())
#df_power_ways.plot()
#plt.show()
#gpd_ways = gpd.GeoDataFrame(df_power_ways, geometry=geometry)

# Erstellen Power_relations table
df_power_relations = pd.read_csv(file_relation)
df_power_relations_new = df_power_relations[["ID", 'voltage', "cables", "wires", "circuits", "frequency", "Nodes"]]  # drop unwanted columns
df_power_relations_new = df_power_relations_new.reindex(columns=["ID", 'voltage', "cables", "wires", "circuits", "frequency", "Nodes"])  # reorder columns #no circuits in relations.csv?
#print(df_power_relations_new.head().to_string())

#Erstellen power_relations_applied_changes
df_power_relations_applied_changes = df_power_relations.copy()
#print(df_power_relations.head().to_string())

# Erstellung der Relations table
relations = {'id': [], 'version': [], 'user_id': [], 'tstamp': [], 'changeset_id': [], 'tags': []}
df_relations_new = pd.DataFrame(relations)
df_relations = pd.read_csv(file_relation)
columns = list(df_relations.columns)
print(columns)
columns.remove('Unnamed: 0')
columns.remove('ID')
columns.remove('Members')
print(columns)
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

#--Bus Data TODO: Index
bus_data_columns = {"id":[], 'cnt': [], 'the_geom': [], 'voltage': [], 'substation_id': [], 'buffered': []}
df_bus_data = pd.DataFrame(bus_data_columns)

#--branch_data TODO: Index
branch_data_columns = {"branch_id":[], 'relation_id': [], 'line_id': [], 'length': [], 'way geometry': [], 'f_bus': [], 't_bus': [], 'voltage': [], 'cables': [], 'wires': [], 'frequency':[], "power":[]}
df_branch_data = pd.DataFrame(branch_data_columns)

#   Create power_ways_applied_changes
df_power_ways_applied_changes = df_power_ways.copy()
#print(df_power_ways_applied_changes.head().to_string())

#   Change the Voltage of 400kV
df_power_relations_applied_changes["frequency"] = df_power_relations_applied_changes["frequency"].astype("str")  #change from string to float for the if function
for x in df_power_relations_applied_changes["frequency"]:
    if x == "50":
        df_power_relations_applied_changes['voltage'].mask(df_power_relations_applied_changes['voltage'] == 400000, 380000,
                                                       inplace=True)
#print(df_power_relations_applied_changes.to_string())

#   Create table power_line
df_power_line = df_power_ways_applied_changes[df_power_ways_applied_changes.isin(["line", "cable"]).any(axis = 1)]       # Select only the lines with power = cable or line
#TODO Set a new Index!!
#df_power_line.set_index("ID", drop=False)
#print(df_power_line.head().to_string())

#   Create table power_substation
df_power_substation = df_power_ways_applied_changes[df_power_ways_applied_changes.isin(['substation','sub_station','station', 'plant']).any(axis = 1)]  #select only lines where Power is 'substation','sub_station','station', 'plant'
df_power_substation_geometry = df_power_substation["geometry"].agg(lambda x: Polygon(x))        # Create Polygon out of Linestrings

df_power_substation = gpd.GeoDataFrame(df_power_substation, geometry = df_power_substation_geometry)    # create the gpd with the Polygons
#for x in df_power_substation ["geometry"]:
 #   if

df_power_substation = df_power_substation.set_index("ID", drop=False)
#print(df_power_substation.head().to_string())
#df_power_substation.plot()         #Create the Plot
#plt.show()


#495
# Create table power_circuits
power_circuits = df_power_relations_applied_changes  # braucht man das PK?

#503
# Create table power_circ_members #gets information from power_line table
power_circ_members = {'relation_id': [], 'line_id': []}  # line_id of relation members

#515
#Change datatypes to int
def change_datatype_semic(column, position):
    x = 0
    while x < len(power_circuits[column]):
        try:
            power_circuits.iloc[x, power_circuits.columns.get_loc(column)] = int(
                power_circuits.at[x, column].split(";", position)[position-1])  # value at *position* will be considered, if there are more than one
        except ValueError as ex:
            power_circuits.iloc[x, power_circuits.columns.get_loc(column)] = 0
        x = x + 1


change_datatype_semic("cables", position) #position starts from 1
change_datatype_semic("voltage", position)
change_datatype_semic("circuits", position)
change_datatype_semic("frequency", position)

def change_datatype_wires(column, position):
    x = 0
    while x < len(power_circuits[column]):
        wire_type = power_circuits.at[x, column].split(";", position)[position-1]  # wire_type at *position* will be considered, if there are more than one
        match wire_type:
            case "quad":
                power_circuits.iloc[x, power_circuits.columns.get_loc(column)] = 4
            case "triple":
                power_circuits.iloc[x, power_circuits.columns.get_loc(column)] = 3
            case "double":
                power_circuits.iloc[x, power_circuits.columns.get_loc(column)] = 2
            case "single":
                power_circuits.iloc[x, power_circuits.columns.get_loc(column)] = 1
            case _:
                power_circuits.iloc[x, power_circuits.columns.get_loc(column)] = 0 #works only for quad, triple, double, single
        x = x + 1

change_datatype_wires("wires", 1)#position starts from 1

''' #print more rows
pd.set_option('display.max_columns', None) 
pd.set_option('display.width', 100000000)
pd.set_option('display.max_rows', None)
'''

#536
#ASSUMPTION: Voltage of 110kV to 60kV
for x in power_circuits["voltage"]:
    power_circuits['voltage'].mask(power_circuits['voltage'] == 110000, 60000, inplace=True)

#print(power_circuits)
#print(power_circuits.head().to_string())



#Processingtime in minutes
timer_end = time.time()
print('Runtime ' + str(round((timer_end-timer_start)/60, 2)) + ' Minutes')






