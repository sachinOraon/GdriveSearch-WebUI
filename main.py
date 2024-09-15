import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import io
import json
import logging
import os
import pickle
import re
import humanize
import requests
from sys import exit
from urllib.parse import unquote
from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from imdb import Cinemagoer
from natsort import natsorted
from typing import Optional, Dict

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
CONFIG_FILE_URL = os.getenv("CONFIG_FILE_URL")
PICKLE_FILE = "token.pickle"
MAX_RESULTS = 30
QUALITIES = ["360", "480", "720", "1080", "1440", "2160"]
TG_ARCHIVE_SEARCH_API = None
TG_ARCHIVE_AUTH_API = None
TG_ARCHIVE_TOKEN = None
TG_ARCHIVE_USER = None

class GdriveSearchError(Exception):
    pass

def isDownloadable(fileId, creds, shared_list) -> None:
    try:
        gdrive = build('drive', 'v3', credentials=creds, cache_discovery=False)
        req = gdrive.files().get_media(fileId=fileId)
        buf = io.BytesIO()
        MediaIoBaseDownload(buf, req, chunksize=1024*1024*2).next_chunk()
        buf.close()
        gdrive.close()
    except Exception:
        shared_list.append(fileId)

async def filter_files(fileId, creds, shared_list, is_tgapi):
    if is_tgapi:
        return
    await asyncio.to_thread(isDownloadable, fileId, creds, shared_list)

async def check_download(creds, resultsMap) -> None:
    shared_list = list()
    await asyncio.gather(*[asyncio.create_task(filter_files(fileId, creds, shared_list, resultsMap[fileId]['is_tgapi'])) for fileId in resultsMap])
    for fileId in shared_list:
        resultsMap.pop(fileId)
    del shared_list

def contains(substr: list, fullstr: str) -> bool:
    for s in substr:
        if fullstr.find(s.lower()) == -1:
            return False
    return True

def setup_config() -> None:
    global TG_ARCHIVE_USER
    global TG_ARCHIVE_SEARCH_API
    global TG_ARCHIVE_AUTH_API
    IS_CONFIG_OK = False
    if CONFIG_FILE_URL is not None:
        logger.info("Downloading config file")
        try:
            config_file = requests.get(url=CONFIG_FILE_URL)
            if config_file.ok:
                with open('config.env', 'wt', encoding='utf-8') as f:
                    f.write(config_file.text)
                logger.info("Loading config values")
                if load_dotenv('config.env', override=True):
                    TG_ARCHIVE_SEARCH_API = os.getenv(key='TG_ARCHIVE_SEARCH_API', default='').rstrip('/')
                    TG_ARCHIVE_AUTH_API = os.getenv(key='TG_ARCHIVE_AUTH_API', default='').rstrip('/')
                    TG_ARCHIVE_USER = os.getenv(key='TG_ARCHIVE_USER', default=None)
                    pickle_file = requests.get(url=os.environ['PICKLE_FILE_URL'])
                    if pickle_file.ok:
                        with open(PICKLE_FILE, 'wb') as f:
                            f.write(pickle_file.content)
                        IS_CONFIG_OK = True
                    else:
                        raise requests.exceptions.HTTPError
            else:
                logger.error("Error while downloading config file")
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, KeyError, json.JSONDecodeError):
            logger.error("Failed to setup config")
    else:
        logger.error("CONFIG_FILE_URL is None")
    if not IS_CONFIG_OK:
        exit(os.EX_CONFIG)

setup_config()
flask_app = Flask(__name__)
CORS(flask_app)

@flask_app.get('/')
def hello():
    return 'Hello from File Search Service'

def get_tg_auth_data() -> Dict:
    auth_data = {
        "id": TG_ARCHIVE_USER,
        "first_name": os.getenv(key="FIRST_NAME"),
        "username": os.getenv(key="USER_NAME"),
        "auth_date": os.getenv(key="AUTH_DATE"),
        "hash": os.getenv(key="HASH")
    }
    if LAST_NAME := os.getenv(key="LAST_NAME"):
        auth_data["last_name"] = LAST_NAME
    if PHOTO_URL := os.getenv(key="PHOTO_URL"):
        auth_data["photo_url"] = PHOTO_URL
    return auth_data

def fetch_tgarchive_token() -> Optional[str]:
    auth_token = None
    if not TG_ARCHIVE_AUTH_API or not TG_ARCHIVE_USER:
        logger.warning("Required params for fetching token are missing")
        return auth_token
    try:
        req_body = {
            "site": "tgarchive",
            "user_id": TG_ARCHIVE_USER,
            "api_number": 0,
            "auth_data": get_tg_auth_data()
        }
        req_body_str = json.dumps(req_body, separators=(",", ":"))
        req_headers = {
            "Content-Length": str(len(req_body_str)),
            "Content-Type": "application/json",
            "Origin": "https://tgarchive.eu.org",
            "Referer": "https://tgarchive.eu.org"
        }
        if secondary_tg_auth_api := os.getenv(key='SECONDARY_TG_AUTH_API'):
            logger.info(f"Fetching token from:: {secondary_tg_auth_api}")
            auth_response = requests.get(url=f"{secondary_tg_auth_api}/token/{req_body['auth_data']['username']}")
        else:
            logger.info(f"Fetching token from:: {TG_ARCHIVE_AUTH_API}")
            auth_response = requests.post(url=TG_ARCHIVE_AUTH_API, data=json.dumps(req_body), headers=req_headers)
        if auth_response.ok:
            logger.info(f"Received response:: {auth_response.text}")
            auth_token = json.loads(auth_response.content).get("token")
        else:
            logger.error(f"Received failure response:: {auth_response.text}")
    except requests.exceptions.RequestException as err:
        err_msg = err.response.text if hasattr(err, 'response') else err.__class__.__name__
        logger.error(f"Failed to fetch auth token for tgarchive, error:: {err_msg}")
    except json.JSONDecodeError as err:
        logger.error(f"Error while parsing json data:: {err.msg}")
    return auth_token

def fetch_tgarchive_files(query: str, retry_count: int = 1) -> Optional[Dict]:
    global TG_ARCHIVE_TOKEN
    results: Optional[Dict] = dict()
    if not TG_ARCHIVE_SEARCH_API:
        logger.warning("Required param for searching tgarchive is missing")
        return results
    if not TG_ARCHIVE_TOKEN:
        if not (TG_ARCHIVE_TOKEN := fetch_tgarchive_token()):
            logger.error("Unable to get tgarchive token")
            return results
    logger.info(f"Received request to search TG files for:: {query}")
    try:
        req_body = {
            "site": "tgarchive",
            "q": query,
            "pageToken": None,
            "api_number": 0,
            "auth_data": get_tg_auth_data()
        }
        req_body_str = json.dumps(req_body, separators=(",", ":"))
        req_headers = {
            "Authorization": f"Bearer {TG_ARCHIVE_TOKEN}",
            "Content-Length": str(len(req_body_str)),
            "Content-Type": "text/plain;charset=UTF-8",
            "Origin": "https://tgarchive.eu.org",
            "Referer": "https://tgarchive.eu.org"
        }
        if secondary_tg_search_api := os.getenv(key='SECONDARY_TG_SEARCH_API'):
            req_url = f"{secondary_tg_search_api}/search"
        else:
            req_url = TG_ARCHIVE_SEARCH_API
        logger.info(f"Sending request to:: {req_url}")
        files_response = requests.post(url=req_url, data=json.dumps(req_body), headers=req_headers)
        if files_response.ok:
            logger.debug(f"Received response:: {files_response.text}")
            files_list = json.loads(files_response.content).get("files")
            for _file in files_list:
                _file_obj = json.loads(_file.get("description"))
                file_id = _file_obj.get("id") if _file_obj.get("id") is not None else _file_obj.get("_id")
                if file_id is None:
                    logger.error(f"TG File id is missing for:: {_file_obj.get('name')}")
                    continue
                results[str(int(file_id))] = {
                    "name": _file_obj.get("name"),
                    "size": humanize.naturalsize(_file_obj.get("size")),
                    "mimeType": _file_obj.get("mime_type"),
                    "fileUniqueId": _file_obj.get("file_unique_id"),
                    "is_tgapi": True
                }
        else:
            logger.error(f"Received failure response:: {files_response.text}")
            if json.loads(files_response.text).get("message") == "Unauthorized":
                logger.warning("Token is expired")
                TG_ARCHIVE_TOKEN = None
                if retry_count <= 1:
                    results = fetch_tgarchive_files(query, retry_count+1)
    except (requests.exceptions.RequestException, AttributeError) as err:
        err_msg = err.response.text if hasattr(err, 'response') else err.__class__.__name__
        logger.error(f"Failed to fetch files from tgarchive, error:: {err_msg}")
    except json.JSONDecodeError as err:
        logger.error(f"Error while parsing json data:: {err.msg}")
    return results

@flask_app.get("/search/<query>")
def search(query: str):
    query = unquote(query)
    query = re.sub('[^a-zA-Z0-9-_ ]', '', query)
    logger.info(f"Searching: {query}")
    quality = None
    for resol in QUALITIES:
        if re.search(resol, query):
            quality = resol
            break
    try:
        if os.path.exists(PICKLE_FILE):
            with open(PICKLE_FILE, 'rb') as f:
                credentials = pickle.load(f)
                if credentials and credentials.expired and credentials.refresh_token:
                    try:
                        credentials.refresh(Request())
                    except RefreshError:
                        logger.error("Failed to refresh token")
                        raise GdriveSearchError
        else:
            err_msg = f"{PICKLE_FILE} not found"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
        gdrive_query = "mimeType != 'application/vnd.google-apps.folder' and "
        query_list = re.split(' ', query)
        for text in query_list:
            if quality is not None and re.search(quality, text):
                continue
            gdrive_query += f"name contains '{text}' and "
        gdrive_query += "trashed=false"
        results = service.files().list(supportsAllDrives=True, includeItemsFromAllDrives=True,
                                       q=gdrive_query, corpora='allDrives', spaces='drive',
                                       fields='files(id, name, mimeType, size, md5Checksum)').execute()["files"]
        service.close()
        count = 0
        response_dict = dict()
        resultsMap = dict()
        for file in results:
            if count >= MAX_RESULTS:
                break
            elif file.get('mimeType') == 'application/vnd.google-apps.shortcut' \
                    or contains(query_list, file.get('name').lower()) is False:
                continue
            else:
                resultsMap[file.get('id')] = {'name': file.get('name'), 'size': f"{humanize.naturalsize(file.get('size', 0))}", 'mimeType': file.get('mimeType'), 'is_tgapi': False}
                count += 1

        if len(resultsMap) < 10 and (tg_files := fetch_tgarchive_files(query)):
            resultsMap.update(tg_files)

        asyncio.run(check_download(credentials, resultsMap))
        logger.info(f"Query: {query}, Found: {len(resultsMap)}")
        response_dict['files'] = dict(natsorted(seq=resultsMap.items(), key=lambda x: x[1].get('name')))
        response_dict['status'] = 'success'
        return response_dict
    except Exception as e:
        return {'status': 'error', 'msg': f'Internal Server Error[{e.__class__.__name__}: {str(e)}]'}

@flask_app.get("/titles/<query>")
def titles(query: str):
    results = dict()
    title = []
    try:
        ia = Cinemagoer()
        query = unquote(query)
        logger.info(f"Getting titles for: {query}")
        for movie in ia.search_movie(title=query, results=10):
            name = movie.data.get('title').strip()
            year = movie.data.get('year')
            kind = movie.data.get('kind')
            if year is not None and kind == 'movie':
                name += f" {str(year)}"
            title.append(name)
        results['titles'] = list(set(title))
        results['status'] = 'success'
    except Exception:
        results['status'] = 'error'
    return results
