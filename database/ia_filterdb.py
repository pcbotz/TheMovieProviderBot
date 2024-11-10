import logging
import re
import base64
from struct import pack
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import DATABASE_URI_1, DATABASE_URI_2, DATABASE_URI_3, DATABASE_URI_4, DATABASE_NAME, COLLECTION_NAME, USE_CAPTION_FILTER, MAX_B_TN
from utils import get_settings, save_group_settings
from itertools import cycle

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize connections to each database
client_1 = AsyncIOMotorClient(DATABASE_URI_1)
client_2 = AsyncIOMotorClient(DATABASE_URI_2)
client_3 = AsyncIOMotorClient(DATABASE_URI_3)
client_4 = AsyncIOMotorClient(DATABASE_URI_4)

# Cycle through clients to balance load across databases
db_cycle = cycle([client_1[DATABASE_NAME], client_2[DATABASE_NAME], client_3[DATABASE_NAME], client_4[DATABASE_NAME]])

# Define umongo instances for each database
instance_1 = Instance.from_db(client_1[DATABASE_NAME])
instance_2 = Instance.from_db(client_2[DATABASE_NAME])
instance_3 = Instance.from_db(client_3[DATABASE_NAME])
instance_4 = Instance.from_db(client_4[DATABASE_NAME])

instances = [instance_1, instance_2, instance_3, instance_4]  # List of instances to rotate

@instance_1.register
@instance_2.register
@instance_3.register
@instance_4.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)

    class Meta:
        indexes = ('$file_name', )
        collection_name = COLLECTION_NAME

async def save_file(media):
    """Save file across multiple databases in a round-robin manner"""
    current_db = next(db_cycle)
    current_instance = instances[db_cycle.index(current_db)]

    # Extract file details
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))

    try:
        # Create Media document
        file = Media(
            file_id=file_id,
            file_ref=file_ref,
            file_name=file_name,
            file_size=media.file_size,
            file_type=media.file_type,
            mime_type=media.mime_type,
            caption=media.caption.html if media.caption else None,
            instance=current_instance
        )
    except ValidationError:
        logger.exception('Error occurred while saving file in database')
        return False, 2
    else:
        try:
            await file.commit()
        except DuplicateKeyError:
            logger.warning(f'{getattr(media, "file_name", "NO_FILE")} is already saved in database')
            return False, 0
        else:
            logger.info(f'{getattr(media, "file_name", "NO_FILE")} is saved to database')
            return True, 1

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """For given query, return (results, next_offset)"""
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        max_results = 10 if settings.get('max_btn') else int(MAX_B_TN)

    query = query.strip()
    raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])' if ' ' not in query else query.replace(' ', r'.*[\s\.\+\-_]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        return []

    search_filter = {'$or': [{'file_name': regex}, {'caption': regex}]} if USE_CAPTION_FILTER else {'file_name': regex}
    if file_type:
        search_filter['file_type'] = file_type

    results, total_results = [], 0
    for instance in instances:
        media_collection = instance.db[COLLECTION_NAME]
        total_results += await media_collection.count_documents(search_filter)
        
        cursor = media_collection.find(search_filter)
        cursor.sort('$natural', -1)
        cursor.skip(offset).limit(max_results)
        files = await cursor.to_list(length=max_results)
        results.extend(files)

    next_offset = offset + max_results if total_results > offset + max_results else ''
    return results, next_offset, total_results

async def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref


# Define encoding helpers
def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


