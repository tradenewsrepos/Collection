from telethon import TelegramClient
from tqdm import tqdm
import os

# These example values won't work. You must get your own api_id and
# api_hash from https://my.telegram.org, under API Development.

# Данные изменены на мои 13.03..23
api_id = "26600763"
token = "5802313190:AAE5APsUh6Bs6m3s8OAKTxuEEdYgAr0oAkA"
api_hash = "c14cc90abf3f85cb3642f235752fe065"
chat_id = '352318527'
name = 'trade_news_col'

client = TelegramClient(name, api_id, api_hash)


async def main():
    # Getting information about yourself
    me = await client.get_me()

    # "me" is a user object. You can pretty-print
    # any Telegram object with the "stringify" method:
    print(me.stringify())

    # When you print something, you see a representation of it.
    # You can access all attributes of Telegram objects with
    # the dot operator. For example, to get the username:
    username = me.username
    print(username)
    print(me.phone)

#     You can print all the dialogs/conversations that you are part of:
    async for dialog in client.iter_dialogs():
        print(dialog.name, 'has ID', dialog.id)

    # You can send messages to yourself...
    await client.send_message('me', 'Hello, myself!')
#     # ...to some chat ID
#     await client.send_message(-100123456, 'Hello, group!')
#     # ...to your contacts
#     await client.send_message('+34600123123', 'Hello, friend!')
#     # ...or even to any username
#     await client.send_message('username', 'Testing Telethon!')

    # You can, of course, use markdown in your messages:
    message = await client.send_message(
        'me',
        'This message has **bold**, `code`, __italics__ and '
        'a [nice website](https://example.com)!',
        link_preview=False
    )

    # Sending a message returns the sent message object, which you can use
    print(message.raw_text)

    # You can reply to messages directly if you have a message object
    await message.reply('Cool!')

    # Or send files, songs, documents, albums...
    await client.send_file('me', '/home/me/Pictures/holidays.jpg')

    # You can print the message history of any chat:
    async for message in client.iter_messages('me'):
        print(message.id, message.text)

        # You can download media from messages, too!
        # The method will return the path where the file was saved.
        if message.photo:
            path = await message.download_media()
            print('File saved to', path)  # printed after download is done
            

async with client:
    client.loop.run_until_complete(main())
# with TelegramClient(name, api_id, api_hash) as client:
#     messages = client.get_messages('610824357', limit=50) # limit defaults to 1
#     for msg in tqdm(messages):
#         print(msg)
# #         msg.download_media(file=os.path.join('media', '<file_name>'))
