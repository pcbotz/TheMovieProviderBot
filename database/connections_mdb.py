import pymongo
from info import DATABASE_URI, DATABASE_NAME
import logging

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Database setup
myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]
mycol = mydb['CONNECTION']


async def add_connection(group_id, user_id):
    """Add a new connection for the user or update an existing one with the specified group."""
    try:
        query = mycol.find_one({"_id": user_id}, {"_id": 0, "active_group": 0})
        if query:
            group_ids = [x["group_id"] for x in query["group_details"]]
            if group_id in group_ids:
                logger.info(f"Group {group_id} already connected for user {user_id}.")
                return False

        group_details = {"group_id": group_id}
        data = {
            '_id': user_id,
            'group_details': [group_details],
            'active_group': group_id,
        }

        if mycol.count_documents({"_id": user_id}) == 0:
            mycol.insert_one(data)
        else:
            mycol.update_one(
                {'_id': user_id},
                {
                    "$push": {"group_details": group_details},
                    "$set": {"active_group": group_id}
                }
            )
        return True
    except Exception as e:
        logger.exception("Error occurred while adding connection", exc_info=True)
        return False


async def active_connection(user_id):
    """Get the active connection group ID for the user."""
    try:
        query = mycol.find_one({"_id": user_id}, {"_id": 0, "group_details": 0})
        return int(query['active_group']) if query and query['active_group'] is not None else None
    except Exception as e:
        logger.exception("Error occurred while fetching active connection", exc_info=True)
        return None


async def all_connections(user_id):
    """Get a list of all group connections for the user."""
    try:
        query = mycol.find_one({"_id": user_id}, {"_id": 0, "active_group": 0})
        return [x["group_id"] for x in query["group_details"]] if query else None
    except Exception as e:
        logger.exception("Error occurred while fetching all connections", exc_info=True)
        return None


async def if_active(user_id, group_id):
    """Check if the specified group is the active connection for the user."""
    try:
        query = mycol.find_one({"_id": user_id}, {"_id": 0, "group_details": 0})
        return query is not None and query['active_group'] == group_id
    except Exception as e:
        logger.exception("Error occurred while checking active status", exc_info=True)
        return False


async def make_active(user_id, group_id):
    """Set a specific group as the active connection for the user."""
    try:
        update = mycol.update_one({'_id': user_id}, {"$set": {"active_group": group_id}})
        return update.modified_count != 0
    except Exception as e:
        logger.exception("Error occurred while setting active connection", exc_info=True)
        return False


async def make_inactive(user_id):
    """Remove the active connection for the user, setting it to None."""
    try:
        update = mycol.update_one({'_id': user_id}, {"$set": {"active_group": None}})
        return update.modified_count != 0
    except Exception as e:
        logger.exception("Error occurred while making connection inactive", exc_info=True)
        return False


async def delete_connection(user_id, group_id):
    """Delete a specific group connection for the user."""
    try:
        # Remove the specified group connection
        update = mycol.update_one({"_id": user_id}, {"$pull": {"group_details": {"group_id": group_id}}})
        if update.modified_count == 0:
            return False

        # Check if there are remaining connections
        query = mycol.find_one({"_id": user_id}, {"_id": 0})
        if query and query["group_details"]:
            if query['active_group'] == group_id:
                # Set the last group in the list as the new active group if the active group was deleted
                prvs_group_id = query["group_details"][-1]["group_id"]
                mycol.update_one({'_id': user_id}, {"$set": {"active_group": prvs_group_id}})
        else:
            # Set active group to None if no connections remain
            mycol.update_one({'_id': user_id}, {"$set": {"active_group": None}})
        return True
    except Exception as e:
        logger.exception("Error occurred while deleting connection", exc_info=True)
        return False
