from web3 import Web3
import requests
import pandas as pd
import json
import time

## setup the eth node (if infura plug in project id)
dfweb3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/d84606165772420ea3b28a05ff5b0d97'))

## query airtable for data
airtable_api = 'app2Ev80HOJrh1xUA'  # update from testing API
baseid = 'keyVHedBxGXktnoib' # update
tableid = 'tbl2Vm1bpHmNOEafe' # update

url = f"https://api.airtable.com/v0/{baseid}/{tableid}"

headers = {'Authorization': f'Bearer {airtable_api}'}

response = requests.request("GET", url, headers=headers).json()['records']

# turn response into df
airtable_rows = [] 
airtable_index = []
for record in response:
  airtable_rows.append(record['fields'])
  airtable_index.append(record['id'])
df = pd.DataFrame(airtable_rows, index=airtable_index)

# Running available transactions first and marking those blocked as "failed" to be checked later
for index, row in df.iterrows():    
  status = row["Status"]

  if status == "Ready":
    eth = row["EthAmount"]
    to_w = row["ToAddress"]
    from_w = row["FromAddress"]
    private_key = row["FromPK"]
    status = row["Status"]
    
    nonce = web3.eth.getTransactionCount(from_w)
    
    try:     
      ## get gas and gas price
      gas_price = web3.eth.gasPrice / 10**9 # turn into gwei
      gas = web3.eth.estimateGas({
          "from": from_w,
          "nonce": nonce,
          "to": to_w,
          "value": 2  # convert to 'round(amount)' for more dynamic gas
          })
  
      #build a transaction in a dictionary
      tx = {
          'nonce': nonce,
          'to': to_w,
          'value': web3.toWei(eth, 'ether'),  # Ether can be split into 10^18 Wei
          'gas': gas,  # amount of gas
          'gasPrice': web3.toWei(gas_price, 'gwei')  # gas price updated
          }
  
      #sign the transaction with private key
      signed_tx = web3.eth.account.sign_transaction(tx, private_key)
  
      #send transaction to the eth node
      tx_hash = web3.eth.sendRawTransaction(signed_tx.rawTransaction)
  
      #get transaction hash
      print("Completed " + web3.toHex(tx_hash) + " tx for " + eth + "eth")
      
      #PATCH into airtable the hash of successful transaction into status column
      record_id = index
      record_url = url + "/" + record_id
      headers = {'Authorization': f'Bearer {airtable_api}', "Content-type": "application/json"}
  
      upload_data = {"Status" : "Complete"}
      upload_dict = {"fields" : upload_data}
      upload_json = json.dumps(upload_dict)
      response_patch = requests.patch(record_url, data=upload_json, headers=headers)
        
    except: 
      #PATCH into airtable a "FAILED" into status column
      record_id = index
      record_url = url + "/" + record_id
      headers = {'Authorization': f'Bearer {airtable_api}', "Content-type": "application/json"}
  
      upload_data = {"Status" : "Failed"}
      upload_dict = {"fields" : upload_data}
      upload_json = json.dumps(upload_dict)
      response_patch = requests.patch(record_url, data=upload_json, headers=headers)


# Running through "FAILED" transaction and checking ETHERSCAN for issues

# Pandas filter df_eth for failed
df_failed = df[df["Status"] == "Failed"]

for index, row in df_failed.iterrows():
    eth = row["ethAmount"]
    to_w = row["ToWalletAddress"]
    from_w = row["FromWalletAddress"]
    nonce = web3.eth.getTransactionCount(from_w)
    
    try:
        # sleep in fromwallet has a pending transaction
        time.sleep(60)
        
        ## get gas and gas price
        gas_price = web3.eth.gasPrice / 10**9 # turn into gwei
        gas = web3.eth.estimateGas({
            "from": from_w,
            "nonce": nonce,
            "to": to_w,
            "value": 2  # convert to 'round(amount)' for more dynamic gas
            })
    
        #build a transaction in a dictionary
        tx = {
            'nonce': nonce,
            'to': to_w,
            'value': web3.toWei(eth, 'ether'),  # Ether can be split into 10^18 Wei
            'gas': gas,  # amount of gas
            'gasPrice': web3.toWei(gas_price, 'gwei')  # gas price updated
            }
    
        #sign the transaction with private key
        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
    
        #send transaction to the eth node
        tx_hash = web3.eth.sendRawTransaction(signed_tx.rawTransaction)
    
        #get transaction hash
        print("Completed " + web3.toHex(tx_hash) + " tx for " + eth + "eth")
        
        #PATCH into airtable the hash of successful transaction into status column
        record_id = index
        record_url = url + "/" + record_id
        headers = {'Authorization': f'Bearer {airtable_api}', "Content-type": "application/json"}
    
        upload_data = {"Status" : "Complete"}
        upload_dict = {"fields" : upload_data}
        upload_json = json.dumps(upload_dict)
        response_patch = requests.patch(record_url, data=upload_json, headers=headers)
        
    except:  
        # Check if wallet funds are too low for transaction + patch that airtable
        url_escan = "https://api.etherscan.io/api"
        escan_api = "KI6E261EI7Z12KNUIAY35S7JMYAC8EMD1B"
        params = {"module":"account", "action":"balance", "address":from_w, "tag":"latest", "apikey":escan_api}
        funds = int(requests.get(url_escan, params=params).json()['result'])/ 10**18 # eth response in wei
        if funds < eth:
            #PATCH into "Failed" column the lack of funds
            record_id = index
            record_url = url + "/" + record_id
            headers = {'Authorization': f'Bearer {airtable_api}', "Content-type": "application/json"}
    
            upload_data = {"ErrorMessage" : "LOW FUNDS"}
            upload_dict = {"fields" : upload_data}
            upload_json = json.dumps(upload_dict)
            response_patch = requests.patch(record_url, data=upload_json, headers=headers)
        