import time
import osmium
import pandas as pd
# from osmium.geom import GeoJSONFactory
# import shapely.wkb as wkblib
# import geojson
# import os

print("The following packages are used:")
print("osmium: " + '3.2.0')
print("pandas: " + pd.__version__)


""" FIRST RUN: SEARCH IN RELATIONS FOR WAYS """
class FirstRunPowerHandler(osmium.SimpleHandler):
    def __init__(self, new_way_list):
        super(FirstRunPowerHandler, self).__init__()
        self.way_list = new_way_list

    def relation(self, r):
        # search in relation if tags(route) = power
        if r.tags.get('route') == 'power':

            # create a list of the members for each relation
            list_members = list(r.members)

            # goes through list of members and copies ways in relation in new list
            for i in range(0, len(list_members)):
                way_in_relation = str(list_members[i].ref)
                self.way_list.add(way_in_relation)


""" SECOND RUN: SEARCH IN WAYS FOR NODES """
class SecondRunPowerHandler(osmium.SimpleHandler):
    def __init__(self, new_way_list, new_node_list):
        super(SecondRunPowerHandler, self).__init__()
        self.way_list = new_way_list
        self.node_list = new_node_list

    def way(self, w):
        # create string from tags in ways
        tag_string = str(w.tags)

        # If this string contains the word power in any way or the id is in the list from the first run
        if tag_string.__contains__('power') or w.id in self.way_list:
            # print(w)
            # Goes throug those ways anc copies the nodes in a list for later usage
            for i in range(0, len(w.nodes)):
                node_in_ways = w.nodes[i].ref
                self.node_list.add(node_in_ways)


""" THIRD RUN: SEARCH IN ALL DATA STRUCTURES AND WRITE ALL DATA TO DICTIONARIES """
class ThirdRunPowerHandler(osmium.SimpleHandler):
    def __init__(self, new_way_list, new_node_list, node_dict, way_dict, relation_dict):
        super(ThirdRunPowerHandler, self).__init__()
        self.num_nodes = 0
        self.num_relations = 0
        self.num_way = 0
        # self.writer = writefile
        self.node_list = new_node_list
        self.way_list = new_way_list
        # Dictionaries for CSV File
        self.node_dict = node_dict
        self.way_dict = way_dict
        self.relation_dict = relation_dict

    # Search in nodes
    def node(self, n):
        if n.id in self.node_list:
            self.num_nodes += 1
            # Write to osm File
            # self.writer.add_node(n)

            """ Write to Dictionaries """
            # IDs
            self.node_dict['ID'].append(n.id)
            # Location
            self.node_dict['Location'].append(str(n.location))
            # Tags
            list_tags = list(n.tags)
            # print(list_tags)
            list_keys = list(self.node_dict.keys())
            list_keys.remove('ID')
            list_keys.remove('Location')
            for i in range(0, len(list_tags)):
                # print(list_tags[i])
                entry = str(list_tags[i])
                # print(x)
                entry_split = entry.split("=")
                key = entry_split[0]
                value = entry_split[1]
                if key in self.node_dict.keys():
                    self.node_dict[key].append(value)
                    list_keys.remove(key)
                else:
                    size_list = [' '] * (len(self.node_dict['ID']) - 1)
                    self.node_dict[key] = size_list
                    self.node_dict[key].append(value)
            for a in range(0, len(list_keys)):
                self.node_dict[list_keys[a]].append(' ')


    def way(self, w):
        tag_string = str(w.tags)
        if tag_string.__contains__('power') or w.id in self.way_list:
            self.num_way += 1
            # Write to osm File
            # self.writer.add_way(w)

            """ Write to Dictionaries """
            # IDs
            self.way_dict['ID'].append(w.id)
            # Nodes
            list_nodes = list(w.nodes)
            w_nodes_for_dict = list()
            for i in range(0, len(list_nodes)):
                x = str(list_nodes[i].ref)
                w_nodes_for_dict.append(x)
            self.way_dict['Nodes'].append(w_nodes_for_dict)
            # Tags
            list_tags = list(w.tags)
            list_keys = list(self.way_dict.keys())
            list_keys.remove('ID')
            list_keys.remove('Nodes')
            for i in range(0, len(list_tags)):
                entry = str(list_tags[i])
                entry_split = entry.split("=")
                key = entry_split[0]
                value = entry_split[1]
                if key in self.way_dict.keys():
                    self.way_dict[key].append(value)
                    list_keys.remove(key)
                else:
                    size_list = [' '] * (len(self.way_dict['ID']) - 1)
                    self.way_dict[key] = size_list
                    self.way_dict[key].append(value)
            for a in range(0, len(list_keys)):
                self.way_dict[list_keys[a]].append(' ')


    def relation(self, r):
        if r.tags.get('route') == 'power':
            self.num_relations += 1
            # self.writer.add_relation(r)

            """ Write to Dictionaries """
            # IDs
            self.relation_dict['ID'].append(r.id)
            # Members
            list_members = list(r.members)
            r_members_for_dict = list()
            for i in range(0, len(list_members)):
                # print(list_nodes[i].ref)
                x = str(list_members[i].ref)
                r_members_for_dict.append(x)
            self.relation_dict['Members'].append(r_members_for_dict)
            # Tags
            list_tags = list(r.tags)
            list_keys = list(self.relation_dict.keys())
            list_keys.remove('ID')
            list_keys.remove('Members')
            for i in range(0, len(list_tags)):
                entry = str(list_tags[i])
                entry_split = entry.split("=")
                key = entry_split[0]
                value = entry_split[1]
                if key in self.relation_dict.keys():
                    self.relation_dict[key].append(value)
                    list_keys.remove(key)
                else:
                    size_list = [' '] * (len(self.relation_dict['ID']) - 1)
                    self.relation_dict[key] = size_list
                    self.relation_dict[key].append(value)
            for a in range(0, len(list_keys)):
                self.relation_dict[list_keys[a]].append(' ')


class ToCSVHandler(osmium.SimpleHandler):
    def __init__(self, way_dict, node_dict, relation_dict):
        osmium.SimpleHandler.__init__(self)
        self.total = 0
        self.num_nodes = 0
        self.num_relations = 0
        self.num_way = 0
        self.way_dict = way_dict
        self.node_dict = node_dict
        self.relation_dict = relation_dict

    def node(self, n):
        self.num_nodes += 1

        """ Write to Dictionaries """
        # IDs
        self.node_dict['ID'].append(n.id)
        # Location
        self.node_dict['Location'].append(str(n.location))
        # Tags
        list_tags = list(n.tags)
        # print(list_tags)
        list_keys = list(self.node_dict.keys())
        list_keys.remove('ID')
        list_keys.remove('Location')
        for i in range(0, len(list_tags)):
            # print(list_tags[i])
            entry = str(list_tags[i])
            # print(x)
            entry_split = entry.split("=")
            key = entry_split[0]
            value = entry_split[1]
            if key in self.node_dict.keys():
                self.node_dict[key].append(value)
                list_keys.remove(key)
            else:
                size_list = [' '] * (len(self.node_dict['ID']) - 1)
                self.node_dict[key] = size_list
                self.node_dict[key].append(value)
        for a in range(0, len(list_keys)):
            self.node_dict[list_keys[a]].append(' ')

    def way(self, w):
        self.num_way += 1

        """ Write to Dictionaries """
        # IDs
        self.way_dict['ID'].append(w.id)
        # Nodes
        list_nodes = list(w.nodes)
        w_nodes_for_dict = list()
        for i in range(0, len(list_nodes)):
            x = str(list_nodes[i].ref)
            w_nodes_for_dict.append(x)
        self.way_dict['Nodes'].append(w_nodes_for_dict)
        # Tags
        list_tags = list(w.tags)
        list_keys = list(self.way_dict.keys())
        list_keys.remove('ID')
        list_keys.remove('Nodes')
        for i in range(0, len(list_tags)):
            entry = str(list_tags[i])
            entry_split = entry.split("=")
            key = entry_split[0]
            value = entry_split[1]
            if key in self.way_dict.keys():
                self.way_dict[key].append(value)
                list_keys.remove(key)
            else:
                size_list = [' '] * (len(self.way_dict['ID']) - 1)
                self.way_dict[key] = size_list
                self.way_dict[key].append(value)
        for a in range(0, len(list_keys)):
            self.way_dict[list_keys[a]].append(' ')

    def relation(self, r):
        self.num_relations += 1

        """ Write to Dictionaries """
        # IDs
        self.relation_dict['ID'].append(r.id)
        # Members
        list_members = list(r.members)
        r_members_for_dict = list()
        for i in range(0, len(list_members)):
            # print(list_nodes[i].ref)
            x = str(list_members[i].ref)
            r_members_for_dict.append(x)
        self.relation_dict['Members'].append(r_members_for_dict)
        # Tags
        list_tags = list(r.tags)
        list_keys = list(self.relation_dict.keys())
        list_keys.remove('ID')
        list_keys.remove('Members')
        for i in range(0, len(list_tags)):
            entry = str(list_tags[i])
            entry_split = entry.split("=")
            key = entry_split[0]
            value = entry_split[1]
            if key in self.relation_dict.keys():
                self.relation_dict[key].append(value)
                list_keys.remove(key)
            else:
                size_list = [' '] * (len(self.relation_dict['ID']) - 1)
                self.relation_dict[key] = size_list
                self.relation_dict[key].append(value)
        for a in range(0, len(list_keys)):
            self.relation_dict[list_keys[a]].append(' ')


if __name__ == '__main__':
    print("""
    You can choose between the following options:
    1. Filter OSM-File and writing it into a CSV-File
    2. Writing of an already filtered OSM-File into a CSV-File
    """)

    choice = input("What would you like to do?")

    if choice == '1':
        filepath = input("Insert filename of raw osm.pbf-file: ")
    elif choice == '2':
        filepath = input("Insert filename of filtered osm.pbf-file:")

    # Filenames for CSV Files
    filename = filepath.replace('.osm.pbf', '')
    node_file = "CSV/node_" + filename + ".csv"
    way_file = "CSV/way_" + filename + ".csv"
    relation_file = "CSV/relation_" + filename + ".csv"

    # Dictionaries for way, nodes and relations
    dict_way = {'ID': [], 'Nodes': []}
    dict_node = {'ID': [], 'Location': []}
    dict_relation = {'ID': [], 'Members': []}

    # Start Execution time
    startTime = time.time()

    """ OSM-Filter """
    if choice == '1':
        print("The filtering has begun...")

        # 1st Run
        print('1st run: search in relations for way members')
        h1 = FirstRunPowerHandler(new_way_list={0, 1})
        h1.apply_file(filepath)
        way_list = h1.way_list
        FirstRunExecutionTime = (time.time() - startTime)
        print("1st run took " + str(FirstRunExecutionTime) + " seconds")
        print("Anzahl an zugehörigen Ways in Relations: " + str(len(way_list)))

        # 2nd Run
        print('2nd run: serch in ways for nodes and the ways from relations')
        h2 = SecondRunPowerHandler(new_way_list=way_list, new_node_list={0, 1})
        h2.apply_file(filepath)
        node_list = h2.node_list
        SecondRunExecutionTime = (time.time() - startTime)
        print("2nd run took " + str(SecondRunExecutionTime) + " seconds")
        print("Anzahl an zugehörigen Nodes in Ways: " + str(len(node_list)))

        # 3rd Run
        print('3rd run: search in nodes and write all data into file')
        dict_way = {'ID': [], 'Nodes': []}
        dict_node = {'ID': [], 'Location': []}
        dict_relation = {'ID': [], 'Members': []}
        h3 = ThirdRunPowerHandler(new_way_list=way_list, new_node_list=node_list, node_dict=dict_node, way_dict=dict_way, relation_dict=dict_relation)
        h3.apply_file(filepath)

        # Create Dataframes from Dictionaries
        node_dataframe = pd.DataFrame(h3.node_dict)
        way_dataframe = pd.DataFrame(h3.way_dict)
        relation_dataframe = pd.DataFrame(h3.relation_dict)
        # Write Dataframes to CSV File
        node_dataframe.to_csv(node_file)
        way_dataframe.to_csv(way_file)
        relation_dataframe.to_csv(relation_file)
        print("The Files have been written to the csv")

        # Important information regarding performance
        print("Anzahl an Ways: " + str(h3.num_way))
        print("Anzahl an Relations: " + str(h3.num_relations))
        print("Anzahl an Knotenpunkten: " + str(h3.num_nodes))
        executionTime = (time.time() - startTime)
        print('Overall execution time in seconds: ' + str(executionTime))

    if choice == '2':
        print("Writing of OSM-Data to CSV has begun...")
        # Write filtered file to CSV
        s = ToCSVHandler(way_dict=dict_way, node_dict=dict_node, relation_dict=dict_relation)
        s.apply_file(filepath, locations=True)

        # Create Dataframes from Dictionaries
        way_dataframe = pd.DataFrame(s.way_dict)
        node_dataframe = pd.DataFrame(s.node_dict)
        relation_dataframe = pd.DataFrame(s.relation_dict)
        # Write Dataframes to CSV File
        way_dataframe.to_csv(way_file)
        node_dataframe.to_csv(node_file)
        relation_dataframe.to_csv(relation_file)

    print("OSM-Data has been written to CSVs, the following files have been created: "
          + way_file + ", "
          + node_file + ", "
          + relation_file)

""" OLD VERSIONS
class PowerHandler(osmium.SimpleHandler):
    def __init__(self, writer, run, new_node_list, new_way_list):
        super(PowerHandler, self).__init__()
        self.num_nodes = 0
        self.num_relations = 0
        self.num_way = 0
        self.writer = writer
        self.run = run
        self.node_list = new_node_list
        self.way_list = new_way_list

    def node(self, n):
        if self.run == 3 and n.id in self.node_list:
            print(n)
            self.num_nodes += 1
            self.writer.add_node(n)
            # del self.node_list[self.node_list.index(n.id)]

    def way(self, w):
        tag_string = str(w.tags)
        if self.run == 2:
            if tag_string.__contains__('power') or w.id in self.way_list:
                print(w)
                for i in range(0, len(w.nodes)):
                    node_in_ways = str(w.nodes[i].ref)
                    self.node_list.add(node_in_ways)
"""
"""
            if w.tags.get('power') == 'line':
                # print(w)
                self.num_way_lines += 1
                # self.writer.add_way(w)
            elif w.tags.get('power') == 'tower':
                # print(w)
                self.num_way_tower += 1
                # self.writer.add_way(w)
            elif w.tags.get('power') == 'substation':
                # print(w)
                self.num_way_substation += 1
                # self.writer.add_way(w)
            elif w.tags.get('power') == 'station':
                # print(w)
                self.num_way_station += 1
                # self.writer.add_way(w)
            # if w.tags.get('power') == True:
            #     print(w)
            #     self.num += 1
            #     self.writer.add_way(w)
            """
"""
        if self.run == 3:
            if tag_string.__contains__('power') or w.id in self.way_list:
                self.num_way += 1
                self.writer.add_way(w)

    def relation(self, r):
        if self.run == 1 and r.tags.get('route') == 'power':
            print(r)
            list_members = list(r.members)
            # print(list_members)
            for i in range(0, len(list_members)):
                # print(list_members[i].ref)
                way_in_relation = str(list_members[i].ref)
                self.way_list.add(way_in_relation)
        if self.run == 3 and r.tags.get('route') == 'power':
            # print(r)
            self.num_relations += 1
            self.writer.add_relation(r)
"""
"""
# print("Anzahl ways alt: " + str(h.num_way_lines + h.num_way_substation + h.num_way_station + h.num_way_tower))

# Pyrosm comes with a couple of test datasets
# that can be used straight away without
# downloading anything
# fp = ("C:/Users/Jan/Documents/Studium/Master/1.Semester/Masterprojekt/osmTGmod/raw_data/germany_15-05-07_powerfilter.osm.pbf")

# Initialize the OSM parser object
# osm = OSM("C:/Users/derkg/OneDrive/Desktop/Ma_Sc_Projekt/osm-filter/koeln-regbez-latest.osm.pbf")
# print(type(osm))
# print(osm)

# Read POIs with custom filter A
# my_filter = {"power": True}

# pois = osm.get_pois(custom_filter=my_filter)
# print("Number of POIS: ", len(pois))


startTime = time.time()
points_that_are_nodes = osm.get_data_by_custom_criteria(custom_filter=my_filter, keep_nodes=True)
executionTime = (time.time() - startTime)

print("Alter Filter + nodes: ", len(points_that_are_nodes))
print('Execution time in seconds: ' + str(executionTime))
points_that_are_nodes.plot(column="power", legend=True, markersize=1, figsize=(14, 6),
               legend_kwds=dict(loc='upper left', ncol=4, bbox_to_anchor=(1, 1)))
plt.show()


# osm_keys_to_keep = ["ways"]
custom_filter = dict(
    power=['yes'],
    route=['power']
)

startTime = time.time()
new_pois1 = osm.get_data_by_custom_criteria(custom_filter=custom_filter)
print(new_pois1.tail(5))
executionTime = (time.time() - startTime)

print("Mit Custom Fílter: ", len(new_pois1))
print('Execution time in seconds: ' + str(executionTime))
new_pois1.plot(column="power", legend=True, markersize=1, figsize=(14, 6),
               legend_kwds=dict(loc='upper left', ncol=4, bbox_to_anchor=(1, 1)))
plt.show()

startTime = time.time()
new_pois = osm.get_data_by_custom_criteria(custom_filter=custom_filter, keep_nodes=True, keep_ways=True, keep_relations=True)
executionTime = (time.time() - startTime)

print("Mit Custom Filter + nodes + ways + relations: ", len(new_pois))
print('Execution time in seconds: ' + str(executionTime))
new_pois.plot(column="power", legend=True, markersize=1, figsize=(14, 6),
              legend_kwds=dict(loc='upper left', ncol=4, bbox_to_anchor=(1, 1)))
plt.show()

# Plot
# ax = pois.plot(column="power", legend=True, markersize=1, figsize=(14, 6),
#               legend_kwds=dict(loc='upper left', ncol=4, bbox_to_anchor=(1, 1)))

# plt.show()

# pois.to_postgis("my_test")

"""
