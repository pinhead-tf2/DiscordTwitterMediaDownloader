import asyncio
import re
from datetime import datetime, date
from json import dumps
from mimetypes import guess_extension
from os import mkdir, path, walk
from shutil import rmtree
from time import time
from zipfile import ZipFile

import aiofiles
from aiocsv import AsyncReader
from aiohttp import ClientSession, TCPConnector
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, BarColumn, TaskProgressColumn, \
    MofNCompleteColumn, TextColumn

# set up Rich stuff
rich_console = Console()
timed_progress = Progress(
    TextColumn("[progress.description]{task.description}"),
    SpinnerColumn(),
    TimeElapsedColumn(),
    BarColumn(),
    MofNCompleteColumn(),
    TaskProgressColumn(), console=rich_console
)


def current_time():
    # prints out the colored current time with ms for console-like output
    return f"[aquamarine3][{datetime.now().strftime('%T.%f')[:-3]}][/aquamarine3] "


async def dequeue(semaphore: asyncio.Semaphore, sleep: float):
    """Wait for a duration, then increase the Semaphore"""
    try:
        await asyncio.sleep(sleep)
    finally:
        semaphore.release()


async def download_media(semaphore: asyncio.Semaphore, session: ClientSession, progress_task, save_directory: str,
                         sleep: float, url: str):
    """Decrement the semaphore, schedule an increment, and download a URL"""
    await semaphore.acquire()
    await asyncio.create_task(dequeue(semaphore, sleep))
    split_url = url.split('/')

    async with session.get(url) as response:
        if response.status == 200 and (
                'image' in response.headers['Content-Type'] or 'video' in response.headers['Content-Type']):
            downloads_directory_subfolder = response.headers['Content-Type'].split("/")[0]
            file_extension = guess_extension(response.headers['Content-Type'].partition(';')[0].strip())
            async with aiofiles.open(
                    f'{save_directory}/{downloads_directory_subfolder}s/{split_url[-3]}_{split_url[-1]}{file_extension}',
                    'wb') as file:
                await file.write(await response.read())
        else:
            # something fucked up, gather info for the user to understand what happened
            original_tweet = f"https://twitter.com/{split_url[-3]}/status/{split_url[-1]}"
            timed_progress.console.print(
                f"{current_time()}[bold red1]Download failed on tweet[/bold red1] [purple]{original_tweet}[/purple] "
                f"[orange1]({response.status}, {response.headers['Content-Type'].partition(';')[0].strip()})[/orange1]")
            return [url, original_tweet, response.status, response.headers['Content-Type']]

    timed_progress.update(progress_task, advance=1)


async def main():
    rprint(f"{current_time()}[#2196F3]pinhead's Discord Twitter Media Downloader v0.0[/#2196F3]\n"
           f"Finds and downloads all vx, fx, and Twitter media from your Discord data package.")

    # make downloads directory, named with date and unix time, to avoid file conflicts
    download_directory = f"downloads_{date.today()}_{int(time())}"
    mkdir(download_directory)
    mkdir(f"{download_directory}/images")
    mkdir(f"{download_directory}/videos")

    # check to see if the package even exists
    if not path.isfile('package.zip'):
        rprint(f"{current_time()}[bold red1]Error:[/bold red1] [italic purple]package.zip[/italic purple] not found! "
               "Did you place it in the same folder as me?")
        exit(0)

    # access the package zip
    data_package = ZipFile('package.zip', 'r')
    rprint(f"{current_time()}[green]Found package.zip![/green]")

    # extract only messages from zipfile for ease of use
    with timed_progress:
        package_namelist = data_package.namelist()
        active_task = export_progress = timed_progress.add_task("[dodger_blue2]Extracting messages...[/dodger_blue2]",
                                                                total=len(package_namelist))

        for item in package_namelist:
            if item.startswith('messages/') and not item.endswith('.json'):
                data_package.extract(item, '')
            timed_progress.update(export_progress, advance=1)

    # free up some memory
    data_package.close()
    rprint(f"{current_time()}[green]Extracted all messages![/green]")
    timed_progress.remove_task(active_task)

    # initialize twit links list
    download_links = list()
    # begin crawling for links
    with timed_progress:
        message_list = [path.join(r, file) for r, d, f in walk('messages') for file in f]
        active_task = crawl_progress = timed_progress.add_task("[dodger_blue2]Finding links...[/dodger_blue2]",
                                                               total=len(message_list))

        for message_file in message_list:
            if message_file.endswith(".csv"):
                async with aiofiles.open(message_file, mode='r', encoding='utf-8') as csv_file:
                    async for row in AsyncReader(csv_file, delimiter=','):
                        # this regex matches every link, which is sorta annoying, but it also acts as the filter
                        # by coincidence, it also works perfectly for letting me swap to d.fxtwitter links
                        # its not the best but it gets the job done here
                        matches = re.findall(
                            "https://(fixup|fixv|fx|vx)?(twitter|x).com/(\\w*)/status/(\\d*)", row[2])

                        # skip if empty array, due to non-twitter link
                        if len(matches) == 0:
                            continue

                        link_parts = matches[0]
                        fixed_link = f"https://d.fxtwitter.com/{link_parts[2]}/status/{link_parts[3]}"
                        download_links.append(fixed_link)
            timed_progress.update(crawl_progress, advance=1)

    rprint(f"{current_time()}[green]Found [bold purple]{len(download_links)}[/bold purple] links![/green]")
    timed_progress.remove_task(active_task)

    # cleaning up after ourselves
    try:
        rmtree('messages')
    except:
        rprint(f"{current_time()}[red]Failed to remove the [purple]messages[/purple] folder.[/red]")

    rprint(f"{current_time()}[green]Downloads starting![/green]")
    with timed_progress:
        active_task = download_progress = timed_progress.add_task("[dodger_blue2]Downloading media...[/dodger_blue2]",
                                                                  total=len(download_links))
        # failsafe
        with TCPConnector(limit=5) as connector:
            async with ClientSession(connector=connector) as session:
                # create async limit and sleep
                sleep_duration = 2.0
                semaphore = asyncio.Semaphore(5)
                # run task loop
                tasks = [asyncio.create_task(
                    download_media(semaphore, session, download_progress, download_directory, sleep_duration, url))
                    for url in download_links]
                failed_download_urls = await asyncio.gather(*tasks)

        # clear out successful results
        failed_download_urls = list(filter(None, failed_download_urls))

    timed_progress.remove_task(active_task)

    # save failed downloads to file and attach http code
    async with aiofiles.open('failed_downloads.json', 'w') as file:
        await file.write(dumps(failed_download_urls, indent=4))
    rprint(f"{current_time()}[green]Completed! Failed to download [red1]{len(failed_download_urls)}[/red1] GIFs. "
           f"Failed list has been saved to file as [purple]failed_downloads.json[/purple].")


if __name__ == '__main__':
    asyncio.run(main())
