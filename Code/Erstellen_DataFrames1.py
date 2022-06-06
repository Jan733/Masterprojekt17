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
import os
from shapely import wkt
import ast

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
df_nodes=pd.read_csv(file_node)
#   Erstellen der Geometrie
df_nodes[["longitude", "latitude"]]=df_nodes.Location.str.split("/", expand=True)
#   df_nodes.drop("Location")
geometry= gpd.points_from_xy(df_nodes.longitude, df_nodes.latitude)     # Point Geometrie mithilfe der longitude und Latitude
geo_df = gpd.GeoDataFrame(df_nodes, geometry=geometry)                  # Erstellung der Geopandas Tabelle in die die Geometry eingetragen wird.
#   print(df_nodes.head().to_string())
df_nodes = df_nodes.drop(["Location"], axis=1)      #Löschen des Columns Location
df_nodes.rename(columns={"ID":"node_id"}, inplace=True)    # Umbennen des Columns ID, damit bei mehreren Tabellen die ID unter demselben Column steht (vereinfacht das zusammenfügen bei Betrachtung eines Indexes)
df_nodes_new = df_nodes[["node_id", "power", "geometry"]].copy()  # Erstellung der table: nodes mit den nötigen columns
#print(df_nodes_new.head().to_string())

#   Erstellen Relation_Members // Es fehlt noch member type N (node) oder W (way) sowie member_role
df_relation_members=pd.read_csv(file_relation)
df_relation_members["Members"]=df_relation_members["Members"].apply(ast.literal_eval)       # Zugriff auf die einzelnen Werte der Liste: "Members"
df_relation_members_memb = df_relation_members["Members"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")   # Die Members werden einzelnt untereinander geschrieben und einem neuen Index zugeordnet
df_new = pd.merge(df_relation_members["ID"], df_relation_members_memb,  right_index=True, left_index=True).rename(columns={'value':'member_id', 'ID':'relation_id'})    # Die einzelnen Members werden der relation zugeorndet
df_new["sequential_ID"]=df_new.groupby(["relation_id"]).cumcount()      # Erstellung einer sequentiell ID (abzählen wie häufig eine relation nacheinander kommt, somit Anzahl der Member
#pd.merge(pd.merge(df_relation_members_memb,left_index=True,right_index=True),df_relation_members[["ID"]], left_index=True, right_index=True).rename(columns={'value_x':'Members'})
#print(df_new.to_string())
#print(df_relation_members.head().to_string())

#   Erstellen der Way_nodes Tabelle
df_way_nodes=pd.read_csv (file_way)
df_way_nodes["Nodes"]=df_way_nodes["Nodes"].apply(ast.literal_eval)
df_way_nodes_nt=df_way_nodes["Nodes"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")
df_way_nodes_new = pd.merge(df_way_nodes["ID"],df_way_nodes_nt, right_index=True, left_index=True).rename(columns={'value':'node_id', 'ID':'way_id'})
df_way_nodes_new["sequential_ID"]=df_way_nodes_new.groupby(["way_id"]).cumcount()
#df_way_nodes_ntt = df_way_nodes["Nodes"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")
#df_way_nodes_new_new = pd.merge(df_way_nodes_new["node_id"], df_nodes_new["ID"], right_index=True, left_index=True)
#print(df_way_nodes_new)

#   Erstellung power_ways Table // Geometry der Ways
#print(df_geometry_index)
df_power_ways = df_way_nodes_new
df_power_ways["node_id"]=df_power_ways["node_id"].astype("str")     # Umschreiben der beiden IDs zu String, da eine Object und eine Integer ist.
df_nodes_new["node_id"]=df_nodes_new["node_id"].astype("str")
df_power_ways = pd.merge(df_power_ways, df_nodes_new, how="left", on="node_id")     # Zusammenfügen der Ways und Nodes Tabelle um die Geometry über die Ways zu legen
df_power_ways_new = df_power_ways[["way_id", "geometry"]].copy()            # Erstellung der power_ways tabelle mit den Einzelnen Punkten der Geometrie
grouped = df_power_ways_new.groupby(["way_id"])["geometry"].agg(lambda x: LineString(x.tolist()))   # Die einzelnen Punkte werden zusammengefasst und zum Linestring umgewandelt für jeden Way
#grouped=df_power_ways_new.groupby("way_id").agg({"geometry":list})
grouped=gpd.GeoDataFrame(grouped, geometry="geometry")      # Erstellen der Geopandasdataframes
print(grouped.head().to_string())
grouped.plot()
plt.show()
#gpd_ways = gpd.GeoDataFrame(grouped, geometry=geometry)

#Erstellung Power_relations table
df_power_relations = pd.read_csv(file_relation)
df_power_relations = df_power_relations.drop(["Unnamed: 0","from","name","note","operator","route","to","type","via","colour","fixme","operator:wikidata","operator:wikipedia","old_operator","ref","via:2","rating"], axis=1) #drop unwanted columns
df_power_relations = df_power_relations.reindex(columns=["ID",'voltage',"cables","wires","frequency","Members"]) #reorder columns #no circuits in relations.csv?
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
print(columns)
columns.remove('Unnamed: 0')
columns.remove('ID')
columns.remove('Nodes')
print(columns)
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

# Umsetzung des Power_scripts
#In Relation von 380 kV auf 400 kV
#df_relation_members['voltage'].mask(df_relation_members['voltage'] >= 380000, 400000, inplace=True)

#print(df_relation_members['voltage'])






