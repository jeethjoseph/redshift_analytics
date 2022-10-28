#Connect to the cluster
import redshift_connector
import pickle
import time
import os
from pathlib import Path
from dotenv import load_dotenv
import slack


ts = time.time()
print(ts)

env_path = Path('.')/'.env'
load_dotenv(dotenv_path=env_path)


try:
    localStorage = open('localStorage', 'rb')
    staleLocalData= pickle.load(localStorage)
    localStorage.close()

except:
    print("Something went wrong while trying to Read")



count = 0
maxTries = 3
print("starting While")
while (count < maxTries):
    try:
        print("reached try block")
        conn = redshift_connector.connect(
            host=os.environ['HOST'],
            database=os.environ['DATABASE'],
            user=os.environ['DBUSER'],
            password=os.environ['PASSWORD'])
        # Create a Cursor object
        cursor = conn.cursor()

        # Query a table using the Cursor
        cursor.execute("select distinct m.manufacturer, count(distinct p.sku)from products.products p join products.supplier_detail_product_mappings sdpm on p.sku = sdpm.sku join products.product_supplier_details psd on sdpm.supplier_detail_id = psd.supplier_detail_id  join products.inventory i on psd.supplier_detail_id =i.supplier_detail_id  join products.manufacturers m on p.manufacturer = m.manufacturer  where p.is_active = 'true' and psd.is_active = 'true' and i.quantity > 0 and m.is_active = 'true' group by m.manufacturer order by 2 desc;")
        #Retrieve the query result set
        result: tuple  = cursor.fetchall()
        print("try")
        conn.close()
        skuCountPerManufacturer = dict(result)
        break
    except:
        print("No connection.Will sleep 60 secs and try again")
        count = count + 1
        time.sleep(60)        



try:
    localStorage = open('localStorage', 'wb')
    pickle.dump(skuCountPerManufacturer, localStorage)
    localStorage.close()

except:
    print("Something went wrong while trying to write")

res = {key: skuCountPerManufacturer[key] - staleLocalData.get(key, 0)
                       for key in skuCountPerManufacturer.keys()}

diff = {x:y for x,y in res.items() if (y is not None and y!=0) }
print(diff)
print_payload = "Counts of Active SKUs diffed per manufacturer" + "\n"
for manufacturer in diff:
    print_payload+=manufacturer + " : " + str(diff[manufacturer])+"\n"

client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
client.chat_postMessage(channel='#coverage',text=print_payload)
