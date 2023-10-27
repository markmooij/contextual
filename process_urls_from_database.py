import pymongo
import json
import datetime
import asyncio
import aiohttp
import time

# Get URL's from local database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["epic"]
COL = db["urls"]

API_KEY = "YOUR_API_KEY_HERE"
CONTEXT_ID = "IAB_MAIN"  # YOUR CATEGORY NAME HERE

SUCCESS_REPLY = 0
NON_SUCCESS_REPLY = 0
NO_REPLY_TIMEOUT = 0
INVALID_JSON = 0

NO_WORKERS = 20
TIMEOUT = 20


async def perform_post(name, work_queue):
    global SUCCESS_REPLY
    global NON_SUCCESS_REPLY
    global NO_REPLY_TIMEOUT
    global INVALID_JSON

    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=10)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while not work_queue.empty():
            item = await work_queue.get()
            use_server = "https://api.epicai.co/url"

            payload = {
                "include_processing_info": True,
                "include_raw_topics": True,
                "include_html": False,
                "url": item["url"],
                "api_key": API_KEY,
                "context_id": CONTEXT_ID,
            }

            try:
                async with session.post(use_server, data=json.dumps(payload)) as response:
                    reply = await response.text()
                    try:
                        reply = json.loads(reply)

                        if reply["success"] == True:
                            SUCCESS_REPLY += 1
                            if reply["success"] == True:
                                if "processed" in reply:
                                    newvalues = {
                                        "$set": {'status': reply["processed"], "topics": {CONTEXT_ID: reply["topics"]},
                                                 "raw_topics": reply["raw_topics"],
                                                 "language": reply["language"],
                                                 "first_processed": datetime.datetime.now()}}
                                else:
                                    newvalues = {
                                        "$set": {'status': "EDIT: No language?",
                                                 "topics": {CONTEXT_ID: reply["topics"]},
                                                 "raw_topics": reply["raw_topics"],
                                                 "language": reply["language"],
                                                 "first_processed": datetime.datetime.now()}}
                            else:
                                newvalues = {
                                    "$set": {'status': reply["processed"], "first_processed": datetime.datetime.now()}}
                        else:
                            NON_SUCCESS_REPLY += 1
                            newvalues = {
                                "$set": {'status': reply["description"], "first_processed": datetime.datetime.now()}}

                        filter = {'_id': item['_id']}
                        COL.update_one(filter, newvalues)

                    except Exception as e:
                        print("Invalid JSON reply:", reply)
                        INVALID_JSON += 1
            except Exception as e:
                print("No reply within response timeout:", str(e))
                NO_REPLY_TIMEOUT += 1


async def main():
    """
    This is the main entry point for the program
    """

    global SUCCESS_REPLY
    global NON_SUCCESS_REPLY
    global NO_REPLY_TIMEOUT
    global INVALID_JSON

    SKIP = 0

    # This routine requests URL's already posted to API from database to check whether they have been processed (and updates accordingly)
    # The second part posts new URL's to the API to start processing (and updates accordingly)
    for i in range(0, 100):

        # Check database for URL's posted to API and in processing status
        SUCCESS_REPLY = 0
        NON_SUCCESS_REPLY = 0
        NO_REPLY_TIMEOUT = 0
        INVALID_JSON = 0

        print("Total documents in db: ", "{:,}".format(COL.count_documents({})))
        print("Iteration", i)

        no_unsuccess = COL.count_documents(
            {'status.success': False, 'status.description': 'Processing is still in progress...'})

        print("Requesting 10.000 items already posted...(status.success=False)")
        x = COL.find({'status.success': False, 'status.description': 'Processing is still in progress...'}).limit(
            10000).skip(SKIP)

        # Create the queue of work
        work_queue = asyncio.Queue()

        # Put some work in the queue
        for item in x:
            await work_queue.put(item)

        workers = [
            asyncio.create_task(perform_post(str(i), work_queue)) for i in range(NO_WORKERS)
        ]

        # Run the tasks
        await asyncio.gather(
            *workers
        )

        new_nounsuccess = COL.count_documents(
            {'status.success': False, 'status.description': 'Processing is still in progress...'})
        print("No. of unsuccess items in the db: ", "{:,}".format(int(no_unsuccess)))
        print("No. of items updated: ", (no_unsuccess - new_nounsuccess))
        print("Sucessfull replies:", SUCCESS_REPLY)
        print("Unsuccessfull replies:", NON_SUCCESS_REPLY)
        print("Timeouts:", NO_REPLY_TIMEOUT)
        print("Invalid JSON reply:", INVALID_JSON)
        print("Short sleep after requesting already posted items in iteration:", i)
        time.sleep(TIMEOUT)

        # Check database for new URL's (not posted to API yet)
        SUCCESS_REPLY = 0
        NON_SUCCESS_REPLY = 0
        NO_REPLY_TIMEOUT = 0
        INVALID_JSON = 0

        # Create the queue of work
        work_queue = asyncio.Queue()

        no_unprocessed = COL.count_documents({"first_processed": False})
        print("Posting 10.000 new items... (first processed=False)")
        x = COL.find({"first_processed": False}).limit(10000)

        # Put some work in the queue
        for item in x:
            await work_queue.put(item)

        # Run the tasks
        workers = [
            asyncio.create_task(perform_post(str(i), work_queue)) for i in range(NO_WORKERS)
        ]
        await asyncio.gather(
            *workers
        )

        new_no_unprocessed = COL.count_documents({"first_processed": False})
        print("No. of unprocessed items in the db: ", no_unprocessed)
        print("No. of items updated: ", (no_unprocessed - new_no_unprocessed))
        print("Sucessfull replies:", SUCCESS_REPLY)
        print("Unsuccessfull replies:", NON_SUCCESS_REPLY)
        print("Timeouts:", NO_REPLY_TIMEOUT)
        print("Invalid JSON reply:", INVALID_JSON)
        print("Short sleep after posting new items in iteration:", i)
        time.sleep(TIMEOUT)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())