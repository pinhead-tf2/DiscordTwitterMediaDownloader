import asyncio
import aiohttp
from mimetypes import guess_extension

urls = ['https://d.fxtwitter.com/onionarchive_23/status/1644779641855705089',
        'https://d.fxtwitter.com/axor_a/status/1723142360974938117',
        'https://d.fxtwitter.com/pinheadtf2/status/1746231089713037628']


async def main():
    async with aiohttp.ClientSession() as session:
        for url in urls:
            async with session.get(url) as response:
                if response.status == 200 and (
                        'image' in response.headers['Content-Type'] or 'video' in response.headers['Content-Type']):
                    print(f"Status code: {response.status}\n"
                          f"Response URL: {response.url}\n"
                          f"Content Type: {response.content_type}")
                    print(guess_extension(response.headers['Content-Type']))
                    print(guess_extension(response.headers['Content-Type'].partition(';')[0].strip()))
                    print()



if __name__ == '__main__':
    asyncio.run(main())
