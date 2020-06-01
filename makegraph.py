#!/usr/bin/python
import filldb
from graphviz import Digraph
import sqlite3
from sqlite3 import Error
from geoip import geolite2 # pip install python-geoip  pip install python-geoip-geolite2
import socket
import flag #pip install emoji-country-flag

def format_db_data( db_file ):
	global con, cursorObj
	con = sqlite3.connect(db_file)
	cursorObj = con.cursor()

	
	#gets [(id, mac, ip, res_mac)] of all clients
	cursorObj.execute("SELECT * FROM client")
	clients = cursorObj.fetchall()
	#dictionary of {client:[connection_ids,]}
	clients_connections = {}
	clients_queries = {}
	for client in clients:
		entities = (client[0], client[0])
		#gets [(id)] of ip addresses that were connected for each client
		cursorObj.execute('''SELECT server_id FROM transport_out WHERE client_id = ? UNION SELECT server_id FROM transport_in WHERE client_id = ?;''', entities)
		ip_ids = cursorObj.fetchall()
		for i in range(len(ip_ids)):
			ip_ids[i] = ip_ids[i][0]
		clients_connections[client[0]]=ip_ids

	suspicious_ips = {}
	temp = []
	for client in clients:	
		#gets [(id, ip)] that were not acquired through dns 
		entities = [client[0]]
		#for selecting queries with no such name: SELECT web.id, web.web_address from web_address as web where web.id not in (select dns_id from dns);
		cursorObj.execute("SELECT ip.id, ip.ip FROM ip_address AS ip WHERE ip.id NOT IN (SELECT ip_id FROM dns WHERE client_id = ?);", entities)
		suspicious_ips[client[0]] = cursorObj.fetchall()
		for susp_connection in suspicious_ips[client[0]]:	
			if susp_connection[0] in clients_connections[client[0]]:
				entities = (susp_connection[0], client[0])
				cursorObj.execute('''SELECT DISTINCT dst_port FROM transport_out WHERE server_id = ? and client_id = ?;''', entities)
				ports_out = cursorObj.fetchall()
				for i in range(len(ports_out)):
					ports_out[i] = ports_out[i][0]
				cursorObj.execute('''SELECT DISTINCT src_port FROM transport_in WHERE server_id = ? and client_id = ?;''', entities)
				ports_in = cursorObj.fetchall()
				for i in range(len(ports_in)):
					ports_in[i] = ports_in[i][0]
				temp.append([susp_connection[1], ports_out, ports_in])
		clients_connections[client[0]] = temp # now all clients connections are just its suspicius connections, as well as port used for that specific conn
		temp = []	
	
	suspicious_ips_all = []

	for client, connections in clients_connections.items():
		if not connections:
			for item in clients:
				if client in item:
					clients.remove(item)
			del clients_connections[client]
		else:
			for connection in connections:
				suspicious_ips_all.append(connection[0])


	con_info = [clients, clients_connections, suspicious_ips_all]
	con.close()
	return con_info

def get_country_info(suspicious_ips_all):
	countries = {"Unknown":[]}

	for item in suspicious_ips_all: #item is just ip address
		ip_info = geolite2.lookup(item)
		PTR = item
		try:
			PTR = socket.gethostbyaddr(item)[0]
		except:
			pass
		try:
			if ip_info.country not in countries:
				countries[ip_info.country] = [] # a dict of lists
				countries[ip_info.country].append([item, PTR, ip_info.timezone])
			else:
				countries[ip_info.country].append([item, PTR, ip_info.timezone])
		except:
			countries["Unknown"].append([item, PTR, "Unknown"])

	return countries

def make_graph(clients, clients_connections, countries, graph_file):
	g = Digraph('G', filename=graph_file)

	# NOTE: the subgraph name needs to begin with 'cluster' (all lowercase)
	#       so that Graphviz recognizes it as a special cluster subgraph
	memo_con = sqlite3.connect('memories.db')
	cursorObj2 = memo_con.cursor()
	count = 0
	known_queries = {}
	for country, val in countries.items():
		name = 'cluster_'+str(count)
		with g.subgraph(name=str(name)) as c:
			c.attr(color='blue')
			for item in val:
				entities =[item[0]]
				cursorObj2.execute('''SELECT COUNT(dns.client_id) FROM dns INNER JOIN ip_address AS ip ON dns.ip_id = ip.id WHERE ip.ip = ?;''', entities)
				times_resolved = cursorObj2.fetchall()[0][0]
				cursorObj2.execute('''SELECT web.web_address FROM dns INNER JOIN ip_address AS ip ON dns.ip_id = ip.id INNER JOIN web_address AS web ON dns.web_id = web.id WHERE ip.ip = ?;''', entities)
				queries = cursorObj2.fetchall()
				if queries:
					known_queries[item[0]] = queries
				if times_resolved == 1: # for gramatical purposes
					node_info =  "PTR: " + str(item[1]) + "\n" + "IP: " + str(item[0])+ "\n" + "Timezone: " + str(item[2]) + "\n" + "This IP was resolved " + str(times_resolved) + " time in other captures."
				else:
					node_info =  "PTR: " + str(item[1]) + "\n" + "IP: " + str(item[0])+ "\n" + "Timezone: " + str(item[2]) + "\n" + "This IP was resolved " + str(times_resolved) + " times in other captures."
				if times_resolved > 0:
					c.attr('node', color='yellow') # if the IP was resolved by other device the not edges will be coloured green
				else:
					c.attr('node', color='red') #otherwise red
				c.node( str(item[0]), node_info )
			try:			
				c.attr(label=flag.flagize(country + " :"+str(country)+":"))
			except:				
				c.attr(label=str(country))
		count = count + 1

	for ip, queries in known_queries.items():
		list_of_queries = ""
		check_queries = []
		for item in queries:
			if item[0] not in check_queries:
				list_of_queries = list_of_queries + item[0] + "\n"
			check_queries.append(item[0])
		c.attr('node', rank="sink")	
		g.node(str(ip)+"_list", "This IP is resolved in: " + "\n" + list_of_queries)
		g.edge(str(ip), str(ip)+"_list")	
	#client_dict = {}
	for client in clients:
		#client_dict[client[0]]=client[1]
		g.node(str(client[0]), str(client[2] + "\n" + client[1]))

	#print client_dict
	for client, connections in clients_connections.items():
		for connection in connections:
			g.edge(str(client), str(connection[0]), label="Out: " + str(connection[1]) + "\n" + "In: " + str(connection[2]))
	g.view()

def main():
	db_file = filldb.databasefile()
	graph_file = filldb.graphfile()
	if db_file != None and graph_file != None:
		print "Making graph..."
		# a list with clients, dict about clients connections and list with suspicious ips
		con_info = format_db_data(db_file) 
		clients = con_info[0]
		clients_connections = con_info[1]
		suspicious_ips_all = con_info[2]

		# dict of {country:[{ip:ptr}]}
		countries = get_country_info(suspicious_ips_all)

		make_graph(clients, clients_connections, countries, graph_file)
	
	

if __name__ == "__main__":
    main()
