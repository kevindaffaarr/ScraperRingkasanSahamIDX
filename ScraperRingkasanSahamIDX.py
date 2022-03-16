#!.\env\Scripts\python.exe

# =================================================================================================
# MODULE IMPORT : BROWSER
# =================================================================================================
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# =================================================================================================
# MODULE IMPORT : PYMONGO
# =================================================================================================
from pymongo import MongoClient

# =================================================================================================
# MODULE IMPORT : TELETHON
# =================================================================================================
from telethon.sync import TelegramClient

# =================================================================================================
# MODULE IMPORT : OTHER
# =================================================================================================
import datetime, time
import json
import os
import numpy as np

# =================================================================================================
# INITIATING CREDENTIALS
# =================================================================================================
with open("credentials.txt","r") as f:
	credentials = json.load(f)

# =================================================================================================
# INITIATING VARIABLE AND INPUT
# =================================================================================================
errorClass = np.array([])
errorStatus = True
i = 0
maxTry = 20

def sendMessage(clientTelegram, targetUserID, message):
    entity = clientTelegram.get_input_entity(targetUserID)

    if not isinstance(message,np.ndarray):
        clientTelegram.send_message(entity=entity, message=message)
    elif isinstance(message,np.ndarray):
        if message[0]=="photo":
            for j in range(2, message.shape[0]):
                if j==2:
                    sendFile(clientTelegram,targetUserID,message[1],message[j])
                else:
                    clientTelegram.send_message(entity=entity, message=message[j])
        else:
            for j in range(0, message.shape[0]):
                clientTelegram.send_message(entity=entity, message=message[j])

def sendFile(clientTelegram, targetUserID, fileloc, caption='None'):
    clientTelegram.send_file(targetUserID, fileloc, caption=caption)

while i < maxTry:
	# ============================================================================================================================================
	# TRY
	# ============================================================================================================================================
	try:
		# =================================================================================================
		# TRY INDEX
		# =================================================================================================
		i = i + 1
		
		# =================================================================================================
		# INITIATE BROWSER DRIVER
		# =================================================================================================
		# Initiate a chrome options object so you can set the size and headless preference
		chrome_options = Options()
		#chrome_options.add_argument("--headless")
		#chrome_options.add_argument("--window-size=1920x1080")

		# download the chrome driver from https://sites.google.com/a/chromium.org/chromedriver/downloads and put it in the current directory
		chrome_driver = os.getcwd() +"\\chromedriver.exe"

		# =================================================================================================
		# INITIATE DATABASE CONNECTION
		# =================================================================================================
		username = credentials["dbusername"]
		password = credentials["dbpassword"]
		# clientMongoExample = MongoClient("mongodb+srv://"+username+":"+password+"@idxdatabase.opd8z.mongodb.net/data?retryWrites=true&w=majority")
		clientMongo = MongoClient("mongodb://"+username+":"+password+credentials["dbclient"])
		db = clientMongo.data

		# =================================================================================================
		# DEFINE TANGGAL
		# =================================================================================================
		# Cek tanggal terakhir di database > Define startDate
		# Stock
		startDate = db.idxStockRaw.find().sort("date", -1).limit(1) 
		for startDate in startDate:
			dataQuery = startDate["dataJson"]
			dataQuery = dataQuery.replace("\'","\"")
			dataQuery = json.loads(dataQuery)
			recordsTotal = dataQuery["recordsTotal"]

			startDate = str(startDate["date"])
			startDate = startDate[0:10]
			startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")

			if recordsTotal != 0:
				startDate = startDate + datetime.timedelta(days=1)
			
			startDate = startDate.date()
		# Index
		startDateIndex = db.idxIndexRaw.find().sort("date", -1).limit(1) 
		for startDateIndex in startDateIndex:
			dataQuery = startDateIndex["dataJson"]
			dataQuery = dataQuery.replace("\'","\"")
			dataQuery = json.loads(dataQuery)
			recordsTotal = dataQuery["recordsTotal"]

			startDateIndex = str(startDateIndex["date"])
			startDateIndex = startDateIndex[0:10]
			startDateIndex = datetime.datetime.strptime(startDateIndex, "%Y-%m-%d")

			if recordsTotal != 0:
				startDateIndex = startDateIndex + datetime.timedelta(days=1)
			
			startDateIndex = startDateIndex.date()
		# Conclusion
		startDate = min(startDate,startDateIndex)

		# Cek tanggal sekarang > Define endDate
		endDate = datetime.date.today()
		hour = int(datetime.datetime.now().strftime("%H"))
		if hour<16 :
			endDate = endDate - datetime.timedelta(days=1)

		# =================================================================================================
		# LOOPING SCRAPING
		# =================================================================================================
		# Looping startDate to endDate
		scrapingStatus = False
		while startDate <= endDate and startDate < datetime.date(2025,1,1):
			driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)
			scrapingStatus = True
			print(startDate)

			# =================================================================================================
			# Scrape IDX API : GetStockSummary
			# =================================================================================================
			# go to IDX page
			yyyymmdd = startDate.strftime("%Y%m%d")
			url = "https://www.idx.co.id/umbraco/Surface/TradingSummary/GetStockSummary?date="+str(yyyymmdd)+"&start=0&length=1"
			driver.get(url)

			browserResponse = driver.find_element_by_xpath("/html/body/pre").get_attribute("innerHTML")
			browserResponse_json = json.loads(browserResponse)

			if browserResponse_json['recordsTotal']>0:
				url = "https://www.idx.co.id/umbraco/Surface/TradingSummary/GetStockSummary?date="+str(yyyymmdd)+"&start=0&length="+str(browserResponse_json['recordsTotal'])
				driver.get(url)

				browserResponse = driver.find_element_by_xpath("/html/body/pre").get_attribute("innerHTML")
				browserResponse_json = json.loads(browserResponse)

			# check date, does exist in database
			dataDate = datetime.datetime.combine(startDate,datetime.time(0,0,0))
			countQuery = db.idxStockRaw.find({"date":dataDate}).count()
			status = False
			if countQuery == 0:
				# insert_one({date, dataJson})
				browserResponse = browserResponse.replace("\"","\'")
				insertStatus = db.idxStockRaw.insert_one({"date":dataDate,"dataJson":browserResponse})
				status = insertStatus.acknowledged
			else:
				# If record found, check does the new document recordsTotal > 0
				if browserResponse_json['recordsTotal']>0:
					query = db.idxStockRaw.find_one({"date":dataDate},{"_id":1,"dataJson":1})
					dataQuery = query["dataJson"]
					dataQuery = dataQuery.replace("\'","\"")
					dataQuery = json.loads(dataQuery)
					_idQuery = query["_id"]

					# If recordsTotal of document from Query == 0 AND recordsTotal of new document > 0 then UPDATE
					if dataQuery["recordsTotal"] == 0:
						browserResponse = browserResponse.replace("\"","\'")
						insertStatus = db.idxStockRaw.update_one({"_id":_idQuery},{"$set": {"dataJson":browserResponse}})
						status = insertStatus.acknowledged

			print("StockSummary - Date: "+str(dataDate)+" Status: " + str(status))

			# =================================================================================================
			# Scrape IDX API : GetIndexSummary
			# =================================================================================================
			# go to IDX page
			yyyymmdd = startDate.strftime("%Y%m%d")
			url = "https://www.idx.co.id/umbraco/Surface/TradingSummary/GetIndexSummary?date="+str(yyyymmdd)+"&start=0&length=1"
			driver.get(url)

			browserResponse = driver.find_element_by_xpath("/html/body/pre").get_attribute('innerHTML')
			browserResponse_json = json.loads(browserResponse)

			if browserResponse_json['recordsTotal']>0:
				url = "https://www.idx.co.id/umbraco/Surface/TradingSummary/GetIndexSummary?date="+str(yyyymmdd)+"&start=0&length="+str(browserResponse_json['recordsTotal'])
				driver.get(url)

				browserResponse = driver.find_element_by_xpath("/html/body/pre").get_attribute('innerHTML')
				browserResponse_json = json.loads(browserResponse)

			# check date, does exist in database
			dataDate = datetime.datetime.combine(startDate,datetime.time(0,0,0))
			countQuery = db.idxIndexRaw.find({"date":dataDate}).count()
			status = False
			if countQuery == 0:
				# insert_one({date, dataJson})
				browserResponse = browserResponse.replace("\"","\'")
				insertStatus = db.idxIndexRaw.insert_one({"date":dataDate,"dataJson":browserResponse})
				status = insertStatus.acknowledged
			else:
				# If record found, check does the new document recordsTotal > 0
				if browserResponse_json['recordsTotal']>0:
					query = db.idxIndexRaw.find_one({"date":dataDate},{"_id":1,"dataJson":1})
					dataQuery = query["dataJson"]
					dataQuery = dataQuery.replace("\'","\"")
					dataQuery = json.loads(dataQuery)
					_idQuery = query["_id"]

					# If recordsTotal of document from Query == 0 AND recordsTotal of new document > 0 then UPDATE
					if dataQuery["recordsTotal"] == 0:
						browserResponse = browserResponse.replace("\"","\'")
						insertStatus = db.idxIndexRaw.update_one({"_id":_idQuery},{"$set": {"dataJson":browserResponse}})
						status = insertStatus.acknowledged
			print("IndexSummary - Date: "+str(dataDate)+" Status: " + str(status))

			startDate += datetime.timedelta(days=1)
			
		if not scrapingStatus:
			errorClass = np.append(errorClass,"Data already updated. No action executed!")
	
	# ============================================================================================================================================
	# EXCEPT
	# ============================================================================================================================================
	except Exception as e:
		errorStatus = True
		trace = []
		tb = e.__traceback__
		while tb is not None:
			trace.append({
				"filename": tb.tb_frame.f_code.co_filename,
				"name": tb.tb_frame.f_code.co_name,
				"lineno": tb.tb_lineno
			})
			tb = tb.tb_next
		errorDetail = (str({
			'type': type(e).__name__,
			'message': str(e),
			'trace': trace
		}))
		errorClass = np.append(errorClass, errorDetail)

		time.sleep(30)
		continue #Continue to try until maxTry

	# ============================================================================================================================================
	# ELSE
	# ============================================================================================================================================
	else:
		break
	
	# ============================================================================================================================================
	# FINALLY
	# ============================================================================================================================================
	finally:
		errorClass = np.append(errorClass,"end")

		api_id = credentials["telegram_api_id"]
		api_hash = credentials["telegram_api_hash"]
		session = 'scraperRawIdx20202024'
		phone = credentials["telegram_phone"]
		targetUserID = credentials["telegram_targetUserID"]
		bot_token = credentials["telegram_bot_token"]

		clientTelegram = TelegramClient(session, api_id, api_hash).start(bot_token=bot_token)
		clientTelegram.disconnect()
		clientTelegram.connect()

		if not clientTelegram.is_user_authorized():
			clientTelegram.send_code_request(phone)
			clientTelegram.sign_in(phone, input('Enter Code: '))
		print("Connected...")

		sendMessage(clientTelegram,targetUserID,"Number of Try "+str(i))
		sendMessage(clientTelegram,targetUserID,errorClass)

		if not errorStatus:
			targetUserID = credentials["telegram_targetBotID"]
			message = "Database StockXel Updated... Date: "+str(dataDate)
			sendMessage(clientTelegram,targetUserID,message)

		clientTelegram.disconnect()
		clientMongo.close()
		
		print("end")
