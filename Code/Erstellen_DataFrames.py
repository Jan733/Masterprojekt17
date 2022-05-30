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

file_node = "C:/Users/Jan/Documents/Studium/Master/1.Semester/Masterprojekt/osmTGmod/raw_data/Probedateien/node_test.csv"
file_way = "C:/Users/Jan/Documents/Studium/Master/1.Semester/Masterprojekt/osmTGmod/raw_data/Probedateien/way_test.csv"
file_relation = "C:/Users/Jan/Documents/Studium/Master/1.Semester/Masterprojekt/osmTGmod/raw_data/Probedateien/relation_test.csv"

#   Erstellen Nodes
df_nodes=pd.read_csv(file_node)
#Erstellen der Geometrie
df_nodes[["longitude", "latitude"]]=df_nodes.Location.str.split("/", expand=True)
#df_nodes.drop("Location")
geometry= gpd.points_from_xy(df_nodes.longitude, df_nodes.latitude)
geo_df = gpd.GeoDataFrame(df_nodes, geometry=geometry)
#print(df_nodes.head().to_string())
df_nodes = df_nodes.drop(["Location"], axis=1)
# Umbennen von ID damit später zusammengefügt werden kann
df_nodes.rename(columns={"ID":"node_id"}, inplace=True)
df_nodes_new = df_nodes[["node_id", "power", "geometry"]].copy()
#print(df_nodes_new.head().to_string())

#   Erstellen Relation_Members // Es fehlt noch member type N (node) oder W (way) sowie member_role
df_relation_members=pd.read_csv(file_relation)
df_relation_members["Members"]=df_relation_members["Members"].apply(ast.literal_eval)       # Members sind die zugehörigen Nodes / ways
df_relation_members_memb = df_relation_members["Members"].apply(pd.Series).reset_index().melt(id_vars="index").dropna()[["index","value"]].set_index("index")
df_new = pd.merge(df_relation_members["ID"], df_relation_members_memb,  right_index=True, left_index=True).rename(columns={'value':'member_id', 'ID':'relation_id'})
df_new["sequential_ID"]=df_new.groupby(["relation_id"]).cumcount()
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

# Erstellung power_ways Table // Geometry der Ways
#print(df_geometry_index)
df_power_ways = df_way_nodes_new
df_power_ways["node_id"]=df_power_ways["node_id"].astype("str")
df_nodes_new["node_id"]=df_nodes_new["node_id"].astype("str")
df_power_ways = pd.merge(df_power_ways, df_nodes_new, how="left", on="node_id")
df_power_ways_new = df_power_ways[["way_id", "geometry"]].copy()
grouped = df_power_ways_new.groupby(["way_id"])["geometry"].agg(lambda x: LineString(x.tolist()))
#grouped=df_power_ways_new.groupby("way_id").agg({"geometry":list})
grouped=gpd.GeoDataFrame(grouped, geometry="geometry")
print(grouped.head().to_string())
grouped.plot()
plt.show()
#gpd_ways = gpd.GeoDataFrame(grouped, geometry=geometry)



# Umsetzung des Power_scripts
#In Relation von 380 kV auf 400 kV
#df_relation_members['voltage'].mask(df_relation_members['voltage'] >= 380000, 400000, inplace=True)

#print(df_relation_members['voltage'])






