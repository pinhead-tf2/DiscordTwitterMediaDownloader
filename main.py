import asyncio
import aiofiles
from aiocsv import AsyncReader
from json import loads, dumps
from collections import deque
from zipfile import ZipFile
from os import mkdir, path, listdir, walk
from time import time
from datetime import datetime, date
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, BarColumn, TaskProgressColumn, \
    MofNCompleteColumn, track
from aiohttp import ClientSession, TCPConnector

# set up progress bar
progress = Progress(
    SpinnerColumn(),
    TimeElapsedColumn(),
    BarColumn(),
    MofNCompleteColumn(),
    TaskProgressColumn()
)

# set up console
console = Console()

url_list = ['https://twitter.com', 'https://vxtwitter.com', 'https://fxtwitter.com', 'https://x.com']

def current_time():
    # prints out the colored current time with ms for console-like output
    return f"[aquamarine3][{datetime.now().strftime('%T.%f')[:-3]}][/aquamarine3] "


async def dequeue(semaphore: asyncio.Semaphore, sleep: int):
    """Wait for a duration, then increase the Semaphore"""
    try:
        await asyncio.sleep(sleep)
    finally:
        semaphore.release()


async def download_media(semaphore: asyncio.Semaphore, session: ClientSession, progress_task, sleep: int, url: str):
    """Decrement the semaphore, schedule an increment, and download a URL"""
    await semaphore.acquire()
    await asyncio.create_task(dequeue(semaphore, sleep))

    # do stuff


async def main():
    rprint(f"{current_time()}[green]pinhead's Discord Twitter Media Downloader v0.0[/green]\n"
           f"Finds and downloads all vx, fx, and Twitter media from your Discord data package.")

    # make downloads directory, named with date and unix time, to avoid file conflicts
    # mkdir(f"downloads_{date.today()}_{int(time())}")
    # TODO: remove on completion
    if not path.isdir("downloads"):
        mkdir("downloads")

    # check to see if the package even exists
    if not path.isfile('package.zip'):
        rprint(f"{current_time()}[bold red1]Error:[/bold red1] [italic purple]package.zip[/italic purple] not found! "
               "Did you place it in the same folder as me?")
        exit(0)

    # access the package zip
    data_package = ZipFile('package.zip', 'r')
    rprint(f"{current_time()}[green]Found package.zip![/green]")

    # extract only messages from zipfile for ease of use
    with progress:
        package_namelist = data_package.namelist()
        export_progress = progress.add_task("[cyan]Extracting messages...[/cyan]", total=len(package_namelist))

        for item in package_namelist:
            if item.startswith('messages/') and not item.endswith('.json'):
                data_package.extract(item, '')
            progress.update(export_progress, advance=1)
    rprint(f"{current_time()}[green]Extracted all messages![/green]")
    data_package.close()

    # initialize twit links list
    all_links = list()
    # begin crawling for links
    with progress:
        message_list = [path.join(r, file) for r, d, f in walk('messages') for file in f]
        crawl_progress = progress.add_task("[cyan]Extracting messages...[/cyan]", total=len(message_list))

        for message_file in message_list:
            # if message_file.endswith(".csv"):
            if '754506775582998588' in message_file:
                async with aiofiles.open(message_file, mode='r', encoding='utf-8') as csv_file:
                    async for row in AsyncReader(csv_file, delimiter=','):
                        if any(url in row[2] for url in url_list):
                            # TODO: filter to just the URL + status data, convert any links to fxtwitter links
                            all_links.append(row[2])
            progress.update(crawl_progress, advance=1)


if __name__ == '__main__':
    asyncio.run(main())
