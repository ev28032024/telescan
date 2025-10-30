#!/usr/bin/env python3
import asyncio, sys, json, time
from telethon import TelegramClient
from telethon.sessions import StringSession

# config: укажи свои
API_JSON = "apis.json"         # должен содержать 1 api pair
SESSION_PATH = "./converted_27/telethon_from_<account>.session"  # <-- укажи реальный файл

async def main():
    apis = json.load(open(API_JSON))
    api = apis[0]
    print("Using API:", api.get("label", api.get("api_id")))
    client = TelegramClient(SESSION_PATH, int(api["api_id"]), api["api_hash"])
    t0 = time.time()
    try:
        await client.connect()
        print("connect(): OK (took %.2fs)" % (time.time()-t0))
        auth = await client.is_user_authorized()
        print("is_user_authorized:", auth)
        if auth:
            me = await client.get_me()
            print("get_me:", getattr(me,'id',None), getattr(me,'username',None), getattr(me,'phone',None))
    except Exception as e:
        print("EXCEPTION:", type(e).__name__, e)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())
