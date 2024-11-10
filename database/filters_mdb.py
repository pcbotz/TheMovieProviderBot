import pymongo
from info import DATABASE_URI, DATABASE_NAME
from pyrogram import enums
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Database setup
myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]


async def add_filter(grp_id, text, reply_text, btn, file, alert):
    """Add or update a filter in the specified group collection."""
    mycol = mydb[str(grp_id)]

    data = {
        'text': str(text),
        'reply': str(reply_text),
        'btn': str(btn),
        'file': str(file),
        'alert': str(alert)
    }

    try:
        mycol.update_one({'text': str(text)}, {"$set": data}, upsert=True)
        logger.info(f"Filter '{text}' added or updated successfully for group {grp_id}.")
    except Exception as e:
        logger.exception("Error occurred while adding/updating filter.", exc_info=True)


async def find_filter(group_id, name):
    """Find and return a filter's details in the specified group."""
    mycol = mydb[str(group_id)]
    query = mycol.find_one({"text": name})

    if query:
        reply_text = query.get('reply')
        btn = query.get('btn')
        fileid = query.get('file')
        alert = query.get('alert', None)  # Use None as default if alert is missing
        return reply_text, btn, alert, fileid
    else:
        logger.info(f"No filter found for '{name}' in group {group_id}.")
        return None, None, None, None


async def get_filters(group_id):
    """Get all filter names for the specified group."""
    mycol = mydb[str(group_id)]
    texts = []

    try:
        for file in mycol.find({}, {'text': 1}):
            texts.append(file['text'])
        logger.info(f"Retrieved all filters for group {group_id}.")
    except Exception as e:
        logger.exception("Error occurred while retrieving filters.", exc_info=True)
    
    return texts


async def delete_filter(message, text, group_id):
    """Delete a specific filter from the specified group."""
    mycol = mydb[str(group_id)]
    query = {'text': text}

    if mycol.count_documents(query) == 1:
        mycol.delete_one(query)
        await message.reply_text(
            f"`{text}` deleted. I'll no longer respond to that filter.",
            quote=True,
            parse_mode=enums.ParseMode.MARKDOWN
        )
        logger.info(f"Filter '{text}' deleted from group {group_id}.")
    else:
        await message.reply_text("Couldn't find that filter!", quote=True)
        logger.info(f"Filter '{text}' not found in group {group_id}.")


async def del_all(message, group_id, title):
    """Delete all filters for the specified group."""
    if str(group_id) not in mydb.list_collection_names():
        await message.edit_text(f"Nothing to remove in {title}!")
        return

    mycol = mydb[str(group_id)]
    try:
        mycol.drop()
        await message.edit_text(f"All filters from {title} have been removed.")
        logger.info(f"All filters removed from group {group_id}.")
    except Exception as e:
        await message.edit_text("Couldn't remove all filters from group!")
        logger.exception("Error occurred while deleting all filters.", exc_info=True)


async def count_filters(group_id):
    """Count the number of filters in the specified group."""
    mycol = mydb[str(group_id)]
    count = mycol.count_documents({})
    
    if count == 0:
        logger.info(f"No filters found in group {group_id}.")
        return False
    else:
        logger.info(f"{count} filters found in group {group_id}.")
        return count


async def filter_stats():
    """Get statistics on total filters across all groups."""
    collections = mydb.list_collection_names()
    
    # Remove the 'CONNECTION' collection if it exists
    if "CONNECTION" in collections:
        collections.remove("CONNECTION")

    totalcount = 0
    for collection in collections:
        mycol = mydb[collection]
        count = mycol.count_documents({})
        totalcount += count

    totalcollections = len(collections)
    logger.info(f"Total groups: {totalcollections}, Total filters: {totalcount}")
    
    return totalcollections, totalcount
