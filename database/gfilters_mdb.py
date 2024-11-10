import pymongo
from info import DATABASE_URI, DATABASE_NAME
from pyrogram import enums
import logging

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Database setup
myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]


async def add_gfilter(gfilters, text, reply_text, btn, file, alert):
    """Add or update a global filter in the specified collection."""
    mycol = mydb[str(gfilters)]
    data = {
        'text': str(text),
        'reply': str(reply_text),
        'btn': str(btn),
        'file': str(file),
        'alert': str(alert)
    }

    try:
        mycol.update_one({'text': str(text)}, {"$set": data}, upsert=True)
    except Exception as e:
        logger.exception("Error occurred while adding/updating gfilter", exc_info=True)


async def find_gfilter(gfilters, name):
    """Find a specific global filter by text."""
    mycol = mydb[str(gfilters)]
    query = mycol.find({"text": name})

    try:
        for file in query:
            reply_text = file['reply']
            btn = file['btn']
            fileid = file['file']
            alert = file.get('alert', None)
            return reply_text, btn, alert, fileid
    except Exception as e:
        logger.exception("Error occurred while finding gfilter", exc_info=True)
        return None, None, None, None


async def get_gfilters(gfilters):
    """Get all global filter texts from the specified collection."""
    mycol = mydb[str(gfilters)]
    texts = []

    try:
        for file in mycol.find():
            texts.append(file['text'])
    except Exception as e:
        logger.exception("Error occurred while fetching gfilters", exc_info=True)

    return texts


async def delete_gfilter(message, text, gfilters):
    """Delete a specific global filter by text."""
    mycol = mydb[str(gfilters)]
    query = {'text': text}

    try:
        if mycol.count_documents(query) == 1:
            mycol.delete_one(query)
            await message.reply_text(
                f"'`{text}`' deleted. I won't respond to that gfilter anymore.",
                quote=True,
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await message.reply_text("Couldn't find that gfilter!", quote=True)
    except Exception as e:
        logger.exception("Error occurred while deleting gfilter", exc_info=True)
        await message.reply_text("Error while deleting the gfilter!", quote=True)


async def del_allg(message, gfilters):
    """Delete all global filters in the specified collection."""
    if str(gfilters) not in mydb.list_collection_names():
        await message.edit_text("Nothing to remove!")
        return

    mycol = mydb[str(gfilters)]
    try:
        mycol.drop()
        await message.edit_text("All gfilters have been removed!")
    except Exception as e:
        logger.exception("Error occurred while deleting all gfilters", exc_info=True)
        await message.edit_text("Couldn't remove all gfilters!")


async def count_gfilters(gfilters):
    """Count the number of global filters in the specified collection."""
    mycol = mydb[str(gfilters)]
    try:
        count = mycol.count_documents({})
        return count if count > 0 else False
    except Exception as e:
        logger.exception("Error occurred while counting gfilters", exc_info=True)
        return False


async def gfilter_stats():
    """Get statistics of total collections and filter count in the database."""
    collections = mydb.list_collection_names()
    totalcount = 0

    if "CONNECTION" in collections:
        collections.remove("CONNECTION")

    try:
        for collection in collections:
            mycol = mydb[collection]
            count = mycol.count_documents({})
            totalcount += count

        totalcollections = len(collections)
        return totalcollections, totalcount
    except Exception as e:
        logger.exception("Error occurred while gathering gfilter stats", exc_info=True)
        return 0, 0
