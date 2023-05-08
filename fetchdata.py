import csv
import psycopg2

#dev_uuid = '1c645990-5bd8-461a-815d-2a3a86793164'
dev_uuid = 'CD0D1D84-7182-4FF7-8713-E4931DD52111'
gw0_mac = 'e0:e2:e6:70:18:5c'
gw1_mac = 'e0:e2:e6:70:0f:fc'
gw2_mac = '24:6f:28:86:c5:74'
#start_time = '2023-04-27T20:44:00'
start_time = '2023-05-04T14:30:00'


def get_data(gw_mac, dev_uuid):
    conn = psycopg2.connect(dbname="bledata", user="user", password="password", host="localhost")
    with conn.cursor() as cur:
        cur.execute("SELECT rssi, time FROM ibeacon WHERE gw_mac = %s AND uuid = %s AND time > %s", (gw_mac, dev_uuid, start_time))
        rows = cur.fetchall()
    conn.close()
    return rows


def write_csv(gw_mac, dev_uuid, filename):
    rows = get_data(gw_mac, dev_uuid)
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['rssi', 'time'])
        for row in rows:
            writer.writerow(row)


write_csv(gw0_mac, dev_uuid, 'gw0.csv')
write_csv(gw1_mac, dev_uuid, 'gw1.csv')
write_csv(gw2_mac, dev_uuid, 'gw2.csv')
