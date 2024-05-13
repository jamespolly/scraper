import argparse
from bs4 import BeautifulSoup
import httpx
from pathlib import Path
import time
from typing import Callable, List, Tuple
from urllib.parse import urlparse, urlunparse


def assemble_links(url: str, contains: str, prefix: str = None) -> List[str]:
    """
    Get URLs of files matching extension and filter_on criteria.

    Args:
        url: valid URL string
        contains: string to filter results, e.g. 't_and_d' in 'cat_and_dog'.

    Returns:
        list of links matching content filter.
    """
    if not prefix:
        url_parts = urlparse(url)
        prefix = urlunparse([url_parts.scheme, url_parts.netloc, "", "", "", ""])
    page = httpx.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    links = [
        prefix + node.get("href") for node in soup.find_all("a") if node.get("href")
    ]
    return [i for i in links if contains in i]


def download_file(url: str):
    """
    Download file at provided url.

    Args:
        url: string URL indicating remote file to download.

    Return:
        Bytes from URL.

    """
    try:
        r = httpx.get(url, follow_redirects=True, timeout=10)
        r.raise_for_status()
        return r.content
    except httpx.HTTPError as e:
        print(f"Error while requesting {e.request.url!r}.\n{e}")
        return None


def write_file(content: bytes, local_path: Path) -> str:
    """
    Write the provided bytes to file.

    Args:
        content: bytes to write.
        local_path: pathlib.Path object referencing local write destination.

    Returns:
        string indicating success/failure of write.

    """
    try:
        with open(local_path, "wb") as out_file:
            out_file.write(content)
        return f"Completed write to: {local_path}"
    except IOError as e:
        print(f"Cannot write to file: {e}")
        return f"Failed to write to: {local_path}"


def get_link_data(url: str, contains: str, out_dir: Path) -> Tuple[int]:
    """
    Get SD data for provided years. Years valid 2021 through 2023.

    Args:
        url: valid URL string
        contains: string to filter results, e.g. 't_and_d' in 'cat_and_dog'.
        out_dir: pathlib.Path object referencing local directory to save.

    Returns:
        tuple of integers: files successfully downloaded, and links attempted.
    """
    links = assemble_links(url, contains)
    fnames = [Path(i).name for i in links]
    fcount = 0
    for link, fname in zip(links, fnames):
        dl_bytes = download_file(link)
        if dl_bytes:
            out_path = out_dir.joinpath(fname)
            print(write_file(dl_bytes, out_path))
            fcount = fcount + 1
    return fcount, len(links)


def main(
    downloader: Callable[[str, str, Path], Tuple[int]],
    url: str,
    contains: str,
    out_dir: Path,
) -> None:
    """Create save directory, call downloader, and time it."""
    try:
        out_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        print(f"Output directory ({out_dir.as_posix()}) already exists.")
    t0 = time.perf_counter()
    count, total = downloader(url, contains, out_dir)
    elapsed = time.perf_counter() - t0
    print(f"\n{count} downloads in {elapsed} seconds.")
    print(f"\n{total - count} downloads failed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="valid URL to retrieve links within", type=str)
    parser.add_argument(
        "contains", help="string to filter links containing this input", type=str
    )
    parser.add_argument(
        "out_directory", help="directory to save downloaded SD data", type=str
    )
    args = parser.parse_args()
    url = args.url
    contains = args.contains
    out_dir = Path(args.out_directory)
    main(get_link_data, url, contains, out_dir)


url_prefix = "https://web.archive.org/web/20171017231511/http://math.nyu.edu/student_resources/wwiki/index.php/"  # noqa: E501

url_gfd_cat = url_prefix + "Category:Geophysical_Fluid_Dynamics"

gfd_links = assemble_links(url_gfd_cat, contains="_Problem_")

adv_calc_cat = url_prefix + "Category:Advanced_Calculus"
