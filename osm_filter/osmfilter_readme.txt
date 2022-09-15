Input-File: .osm.pbf-file, Same directory as osmfilter_pyosmium.py
Output-Files: .csv-files, directory_of_osmfilter_pyosmium/CSV

Input / Abfolge bei Ausführung osmfilter_pyosmium.py:

C:\Users\derkg\anaconda3\envs\Masterpr\pythonw.exe C:/Users/derkg/OneDrive/Desktop/Ma_Sc_Projekt/osm-filter/osmfilter_pyosmium.py
The following packages are used:
osmium: 3.2.0
pandas: 1.3.5

    You can choose between the following options:
    1. Filter OSM-File and writing it into a CSV-File
    2. Writing of an already filtered OSM-File into a CSV-File
    
What would you like to do?1
Insert filename of raw osm.pbf-file: nordrhein-westfalen-latest.osm.pbf
The filtering has begun...
1st run: search in relations for way members
1st run took 2.3873488903045654 seconds
Anzahl an zugehörigen Ways in Relations: 4606
2nd run: serch in ways for nodes and the ways from relations
2nd run took 542.1871945858002 seconds
Anzahl an zugehörigen Nodes in Ways: 79897
3rd run: search in nodes and write all data into file
The Files have been written to the csv
Anzahl an Ways: 12923
Anzahl an Relations: 1004
Anzahl an Knotenpunkten: 79895
Overall execution time in seconds: 1293.6205613613129
OSM-Data has been written to CSVs, the following files have been created: CSV/way_nordrhein-westfalen-latest.csv, CSV/node_nordrhein-westfalen-latest.csv, CSV/relation_nordrhein-westfalen-latest.csv