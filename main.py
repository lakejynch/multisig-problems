import requests, json, time, pandas as pd

# Adjust inputs on info.json before running script.

def cleanAddresses(known_addresses):
	new = {}
	for key, value in known_addresses.items():
		new[key.lower()] = value
	return new

def getHistory(address, API_KEY, endblock, startblock = 0):
	URL = "https://api.etherscan.io/api"
	PARAMS = {
		"module":"account",
		"action":"txlist",
		"address":address,
		"startblock":startblock,
		"endblock":endblock,
		"apikey":API_KEY
	}
	r = requests.get(url = URL, params = PARAMS)
	data = r.json()
	# To protect against limitations of free API, reject answer if more than 10k txns.
	# In this case, a better solution is needed
	if len(data["result"]) > 10000:
		print("Error: Pagination needed, too many transactions")
		return False
	return data["result"]

def getBlock(API_KEY, timestamp = int(time.time())):
	# If no arg passed uses current time.
	URL = "https://api.etherscan.io/api"
	PARAMS = {
		"module":"block",
		"action":"getblocknobytime",
		"timestamp":timestamp,
		"closest":"before",
		"apikey":API_KEY
		}
	r = requests.get(url = URL, params = PARAMS)
	data = r.json()
	return data["result"]


def processTxns(address, known_addresses, txns):
	# Returns accounts with gas fees paid as dataframe and last block cached as int.
	results = {}
	# columns: ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'transactionIndex', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed','gasUsed', 'confirmations']
	if len(txns) != 0:
		df = pd.DataFrame(txns)
		address = address.lower()
		for index, row in df.iterrows():
			if row["to"] == address:
				gas_cost = float(row["gasUsed"])
				sender = row["from"]
				if sender in list(results.keys()):
					results[sender]["gas_fees"] += gas_cost
				else:
					results[sender] = {"gas_fees":gas_cost,"name":None}
		for r in results.keys():
			if r in list(known_addresses.keys()):
				results[r]["name"] = known_addresses[r]
		output = pd.DataFrame.from_records(results).T
		return output, df["blockNumber"].max() # last block cached
	return 0, 0


def loadData():
	with open("info.json", "r") as jf:
		data = json.load(jf)
	return data

def cacheData(data):
	with open("info.json", "w") as jf:
		json.dump(data, jf, indent = 5)
	print("Progress saved")
	return


if __name__ == "__main__":
	data = loadData()
	LAST_BLOCK = 1 + data["LAST_BLOCK"]
	KNOWN_ADDRESSES = cleanAddresses(data["KNOWN_ADDRESSES"])
	
	if not LAST_BLOCK:
		TXNS = getHistory(data["ADDRESS"], data["API_KEY"], endblock = getBlock(data["API_KEY"]))
	else:
		TXNS = getHistory(data["ADDRESS"], data["API_KEY"], endblock = getBlock(data["API_KEY"]), startblock = LAST_BLOCK)
	output, LAST_BLOCK = processTxns(data["ADDRESS"], KNOWN_ADDRESSES, TXNS)

	if LAST_BLOCK == 0:
		print("No new transactions.")
	else:
		data["LAST_BLOCK"] = int(LAST_BLOCK)
		print(output)
		output.to_csv("output.csv")
		cacheData(data)