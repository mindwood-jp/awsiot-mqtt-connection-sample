#!/usr/bin/python3
"""
Split and publish data for a specified period of time.
Start manually from the command line.
"""

__author__ = 'MindWood'

from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import os
import json
import glob
import time
from datetime import datetime, timedelta
import mysql.connector

def get_data_from_db(mac_id):
		with open(dir + '/data_format.json') as f:
				df = json.load(f)

		from_date_str = current_date.strftime('%Y-%m-%d %H:%M:%S')
		to_date_str = (current_date + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
		print(f'>> From {from_date_str} To {to_date_str}')

		df['t'] = to_date_str
		df['deviceId'] = mac_id
		data = df['values'].pop(0)

		conn = mysql.connector.connect(
				host = 'HOSTNAME',
				port = '3306',
				user = 'USERNAME',
				password = 'PASSWORD',
				database = 'DATABASE',
		)
		cur = conn.cursor(dictionary=True)
		sql = "SELECT *, hex(MacID), from_unixtime(Time) as data_time FROM data_table WHERE MacID=unhex(%s) and from_unixtime(Time) BETWEEN %s and %s;"
		cur.execute(sql, [mac_id, from_date_str, to_date_str])
		db_data = cur.fetchall()
		conn.close()

		gendata = []
		for d in db_data:
				data['tm'] = datetime.strftime(d['data_time'], '%Y-%m-%d %H:%M:%S')
				data['foo'] = d['a']
				data['bar'] = d['b']
				gendata.append(data.copy())

		df['values'] = gendata
		return df

def send(mac_id):
		message = get_data_from_db(mac_id)
		if len(message['values']) == 0:
				print(mac_id + ' no data.')
				return

		print(mac_id + ' data length ' + str(len(message['values'])))

		path_to_certs = glob.glob(dir + '/certs/' + mac_id + '/*certificate.pem.crt')
		if len(path_to_certs) == 0:
				print(mac_id + ' certificate.pem.crt not found')
				return
		path_to_keys = glob.glob(dir + '/certs/' + mac_id + '/*private.pem.key')
		if len(path_to_keys) == 0:
				print(mac_id + ' private.pem.key not found')
				return

		event_loop_group = io.EventLoopGroup(1)
		host_resolver = io.DefaultHostResolver(event_loop_group)
		mqtt_connection = mqtt_connection_builder.mtls_from_path(
				endpoint = 'foobar.iot.ap-northeast-1.amazonaws.com',
				cert_filepath = path_to_certs[0],
				pri_key_filepath = path_to_keys[0],
				client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver),
				ca_filepath = dir + '/certs/AmazonRootCA1.pem',
				client_id = mac_id,
				clean_session = False,
				keep_alive_secs = 6
		)
		print('MQTT Connecting...')
		connect_future = mqtt_connection.connect()
		connect_future.result()
		print('Connected, Begin Publish')
		time.sleep(1)
		mqtt_connection.publish(topic='zzzzz/' + mac_id, payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
		time.sleep(1)
		print('Publish End')
		disconnect_future = mqtt_connection.disconnect()
		disconnect_future.result()

def main():
		global current_date
		while current_date < datetime(2023, 6, 1):
				send('XXXXXXXXX')
				current_date += timedelta(hours=3)
				time.sleep(5)

if __name__ == '__main__':
		dir = os.path.dirname(__file__)
		current_date = datetime(2023, 4, 1)
		main()
