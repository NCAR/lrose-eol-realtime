#!/usr/bin/env python3

#=====================================================================
#
# Safely download NEXRAD Level-II radar files from the public AWS archive.
#
#=====================================================================

import argparse
import datetime
import http.client
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import timedelta

BUCKET_NAME = "unidata-nexrad-level2"
BUCKET_URL = f"https://{BUCKET_NAME}.s3.amazonaws.com"


class HelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    """Show defaults while preserving line breaks in descriptions and examples."""


# MDM metadata and other non-volume objects do not match this expression.
VOLUME_RE = re.compile(
    r"^(?P<site>[A-Z0-9]{4})"
    r"(?P<date>\d{8})_"
    r"(?P<time>\d{6})_"
    r"V\d{2}(?:\.gz)?$"
)

RETRIABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}


def main():
    global options, startTime, endTime, archiveMode, fileCount, thisScriptName
    global tmpDir

    fileCount = 0
    thisScriptName = os.path.basename(__file__)
    parseArgs()

    print("=============================================", file=sys.stderr)
    print(f"BEGIN: {thisScriptName} at {datetime.datetime.now()}", file=sys.stderr)
    print("=============================================", file=sys.stderr)

    os.makedirs(options.outputDir, exist_ok=True)
    # Pending LdataWriter markers live under the output tree so they survive a
    # process restart and share the destination filesystem's lifetime.
    tmpDir = os.path.join(options.outputDir, "tmp")
    os.makedirs(tmpDir, exist_ok=True)

    if options.realtimeMode:
        lookback = timedelta(seconds=options.lookbackSecs)
        while True:
            fileCount = 0
            endTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            startTime = endTime - lookback
            try:
                manageRetrieval(startTime, endTime)
            except Exception as exc:
                # A realtime service must survive a fully exhausted request retry
                # cycle. The next poll provides a fresh opportunity to catch up.
                print(f"ERROR retrieval cycle failed: {exc}", file=sys.stderr)
            time.sleep(options.sleepSecs)

    try:
        success = manageRetrieval(startTime, endTime)
    except Exception as exc:
        print(f"ERROR retrieval failed: {exc}", file=sys.stderr)
        return 1

    print("==============================================", file=sys.stderr)
    print(f"END: {thisScriptName} at {datetime.datetime.now()}", file=sys.stderr)
    print("==============================================", file=sys.stderr)
    return 0 if success else 1


def manageRetrieval(start_time, end_time):
    if start_time > end_time:
        raise ValueError("start time is later than end time")

    if options.debug:
        print(f"Retrieving for times: {start_time} to {end_time} UTC", file=sys.stderr)

    success = True
    this_day = start_time.date()
    while this_day <= end_time.date():
        day_start = datetime.datetime.combine(this_day, datetime.time.min)
        day_end = datetime.datetime.combine(this_day, datetime.time.max)
        period_start = max(start_time, day_start)
        period_end = min(end_time, day_end)
        if not getForInterval(options.radarName, this_day, period_start, period_end):
            success = False
        this_day += timedelta(days=1)

    print(f"---->> Num files downloaded: {fileCount}", file=sys.stderr)
    return success


def retryDelay(attempt):
    return options.retryBaseSecs * (2 ** attempt)


def isRetriable(exc):
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in RETRIABLE_HTTP_CODES
    return isinstance(
        exc,
        (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException),
    )


def runWithRetries(description, operation):
    """Run a network or publication operation with bounded exponential backoff."""
    for attempt in range(options.maxRetries + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt >= options.maxRetries or not isRetriable(exc):
                raise RuntimeError(f"{description}: {exc}") from exc
            delay = retryDelay(attempt)
            print(
                f"WARNING {description} failed (attempt {attempt + 1}): {exc}; "
                f"retrying in {delay:g} seconds",
                file=sys.stderr,
            )
            time.sleep(delay)


def list_s3_keys(prefix, start_after=None):
    """Yield keys using ListObjectsV2 pagination and bounded request retries."""
    continuation_token = None

    while True:
        params = {
            "list-type": "2",
            "prefix": prefix,
            "max-keys": "1000",
        }
        if start_after and continuation_token is None:
            # NEXRAD has daily rather than hourly directories. StartAfter keeps
            # each realtime poll from repeatedly returning the whole UTC day.
            params["start-after"] = start_after
        if continuation_token:
            params["continuation-token"] = continuation_token

        list_url = f"{BUCKET_URL}/?{urllib.parse.urlencode(params)}"
        if options.debug:
            print(f"list URL: {list_url}", file=sys.stderr)

        def fetchPage():
            request = urllib.request.Request(
                list_url,
                headers={"User-Agent": f"{thisScriptName}/3.0"},
            )
            with urllib.request.urlopen(request, timeout=options.timeoutSecs) as response:
                return response.read()

        xml_data = runWithRetries(f"cannot list S3 prefix {prefix}", fetchPage)
        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError as exc:
            raise RuntimeError(f"cannot parse S3 listing for {prefix}: {exc}") from exc

        # S3 XML uses a default namespace. Stripping it keeps the traversal
        # independent of the namespace URL returned by a particular endpoint.
        for elem in root.iter():
            if "}" in elem.tag:
                elem.tag = elem.tag.split("}", 1)[1]

        for contents in root.findall("Contents"):
            key = contents.findtext("Key")
            if key:
                yield key

        if root.findtext("IsTruncated", "false").lower() != "true":
            break

        continuation_token = root.findtext("NextContinuationToken")
        if not continuation_token:
            raise RuntimeError("S3 response was truncated but had no continuation token")


def getForInterval(radarName, thisDay, start_time, end_time):
    local_files = set(getLocalFileList(thisDay, radarName))
    prefix = f"{thisDay:%Y/%m/%d}/{radarName}/"
    start_after = (
        f"{prefix}{radarName}{start_time:%Y%m%d_%H%M%S}"
    )

    success = True
    keys_found = 0
    for key in list_s3_keys(prefix, start_after=start_after):
        file_name = key.rsplit("/", 1)[-1]
        match = VOLUME_RE.fullmatch(file_name)
        if not match or match.group("site") != radarName:
            if options.verbose:
                print(f"skipping non-volume entry: {key}", file=sys.stderr)
            continue

        file_time = datetime.datetime.strptime(
            match.group("date") + match.group("time"), "%Y%m%d%H%M%S"
        )
        keys_found += 1

        # NEXRAD volume names sort chronologically within a site/day prefix.
        # Once a valid volume is beyond the interval, later volumes cannot match.
        if file_time > end_time:
            break
        if file_time < start_time:
            continue

        if options.verbose:
            print(f"checking file: {key}, time: {file_time}", file=sys.stderr)

        final_path = getFinalPath(radarName, file_time, file_name)
        if file_name in local_files and not options.force:
            if not retryPendingLdata(radarName, file_time, file_name, final_path):
                success = False
            elif options.debug:
                print(f"file previously downloaded: {file_name}", file=sys.stderr)
            continue

        if not doDownload(radarName, file_time, key, file_name):
            success = False

    if options.debug:
        print(f"valid volume keys found for {prefix}: {keys_found}", file=sys.stderr)
    return success


def getFinalPath(radarName, fileTime, fileName):
    return os.path.join(
        options.outputDir,
        radarName,
        fileTime.strftime("%Y%m%d"),
        fileName,
    )


def getPendingPath(radarName, fileTime, fileName):
    return os.path.join(
        tmpDir,
        "pending_ldata",
        radarName,
        fileTime.strftime("%Y%m%d"),
        fileName + ".pending",
    )


def writePendingMarker(marker_path, final_path):
    os.makedirs(os.path.dirname(marker_path), exist_ok=True)
    marker_tmp = marker_path + f".{os.getpid()}.part"
    try:
        with open(marker_tmp, "w", encoding="utf-8") as out:
            out.write(final_path + "\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(marker_tmp, marker_path)
    finally:
        try:
            os.remove(marker_tmp)
        except FileNotFoundError:
            pass


def retryPendingLdata(radarName, fileTime, fileName, final_path):
    marker_path = getPendingPath(radarName, fileTime, fileName)
    if not os.path.exists(marker_path):
        return True
    if not os.path.exists(final_path):
        return True
    return publishLdata(radarName, fileTime, fileName, marker_path)


def doDownload(radarName, fileTime, key, fileName):
    global fileCount

    url = f"{BUCKET_URL}/{urllib.parse.quote(key, safe='/')}"
    final_path = getFinalPath(radarName, fileTime, fileName)
    marker_path = getPendingPath(radarName, fileTime, fileName)
    transfer_dir = os.path.join(tmpDir, "downloads", radarName)
    tmp_path = os.path.join(transfer_dir, fileName + f".{os.getpid()}.part")

    if options.debug or options.dryRun:
        print(f"Downloading: {url} -> {final_path}", file=sys.stderr)
    if options.dryRun:
        return True

    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    os.makedirs(transfer_dir, exist_ok=True)

    def transfer():
        request = urllib.request.Request(
            url,
            headers={"User-Agent": f"{thisScriptName}/3.0"},
        )
        try:
            with urllib.request.urlopen(request, timeout=options.timeoutSecs) as response, \
                    open(tmp_path, "wb") as out:
                expected_text = response.headers.get("Content-Length")
                expected = int(expected_text) if expected_text is not None else None
                copied = 0
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    out.write(block)
                    copied += len(block)
                out.flush()
                os.fsync(out.fileno())
            if expected is not None and copied != expected:
                raise OSError(
                    f"short transfer for {url}: expected {expected} bytes, got {copied}"
                )
        except Exception:
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass
            raise

    try:
        runWithRetries(f"cannot download {url}", transfer)
        # Record the metadata obligation before making the data visible. If the
        # process dies after os.replace(), a later poll will finish publication.
        writePendingMarker(marker_path, final_path)
        os.replace(tmp_path, final_path)
    except (RuntimeError, OSError) as exc:
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
        print(f"ERROR downloading {url}: {exc}", file=sys.stderr)
        return False

    fileCount += 1
    return publishLdata(radarName, fileTime, fileName, marker_path)


def publishLdata(radarName, fileTime, fileName, marker_path):
    radar_dir = os.path.join(options.outputDir, radarName)
    date_str = fileTime.strftime("%Y%m%d")
    args = [
        "LdataWriter",
        "-dir", radar_dir,
        "-rpath", os.path.join(date_str, fileName),
        "-ltime", fileTime.strftime("%Y%m%d%H%M%S"),
        "-writer", thisScriptName,
        "-dtype", "level2",
    ]

    for attempt in range(options.maxRetries + 1):
        if runCommand(args):
            try:
                os.remove(marker_path)
            except FileNotFoundError:
                pass
            return True
        if attempt < options.maxRetries:
            delay = retryDelay(attempt)
            print(
                f"WARNING LdataWriter failed; retrying in {delay:g} seconds",
                file=sys.stderr,
            )
            time.sleep(delay)

    # Keeping the marker and completed data file avoids another large transfer;
    # the next lookback cycle retries only the missing metadata publication.
    print(f"ERROR LdataWriter retries exhausted for {fileName}", file=sys.stderr)
    return False


def getLocalFileList(date, radarName):
    day_dir = os.path.join(options.outputDir, radarName, date.strftime("%Y%m%d"))
    os.makedirs(day_dir, exist_ok=True)
    return os.listdir(day_dir)


def parse_utc_time(value):
    try:
        return datetime.datetime.strptime(value, "%Y %m %d %H %M %S")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            'time must use format "YYYY MM DD HH MM SS" (UTC)'
        ) from exc


def parseArgs():
    global options, startTime, endTime, archiveMode

    parser = argparse.ArgumentParser(
        description="Safely download NEXRAD Level-II files from AWS.",
        formatter_class=HelpFormatter,
        epilog=(
            'Archive example:\n'
            '  %(prog)s --radarName KFTG '
            '--start "2026 07 20 12 00 00" '
            '--end "2026 07 20 13 00 00"\n\n'
            'Realtime example:\n'
            '  %(prog)s --radarName KFTG --realtime'
        ),
    )

    parser.add_argument("--debug", action="store_true", help="Set debugging on")
    parser.add_argument("--verbose", action="store_true", help="Set verbose debugging on")
    parser.add_argument(
        "--radarName", default="KFTG", metavar="RADAR",
        help="4-character radar name",
    )
    parser.add_argument(
        "--outputDir", default="/tmp/aws", metavar="DIR",
        help="Directory to which files are written",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Transfer even if the file was previously downloaded",
    )
    parser.add_argument(
        "--dryRun", action="store_true",
        help="List files that would be downloaded without downloading",
    )
    parser.add_argument(
        "--realtime", dest="realtimeMode", action="store_true",
        help="Poll continuously using the configured lookback",
    )
    parser.add_argument(
        "--lookbackSecs", type=int, default=1800, metavar="SECS",
        help="Lookback in realtime mode",
    )
    parser.add_argument(
        "--sleepSecs", type=int, default=10, metavar="SECS",
        help="Delay between realtime polling cycles",
    )
    parser.add_argument(
        "--timeoutSecs", type=int, default=60, metavar="SECS",
        help="Timeout for each AWS request",
    )
    parser.add_argument(
        "--maxRetries", type=int, default=3, metavar="COUNT",
        help="Retries after the initial network or LdataWriter attempt",
    )
    parser.add_argument(
        "--retryBaseSecs", type=float, default=2.0, metavar="SECS",
        help="Initial exponential-backoff delay",
    )
    parser.add_argument(
        "--start", dest="startTime", type=parse_utc_time,
        metavar='"YYYY MM DD HH MM SS"', help="Archive start time in UTC",
    )
    parser.add_argument(
        "--end", dest="endTime", type=parse_utc_time,
        metavar='"YYYY MM DD HH MM SS"', help="Archive end time in UTC",
    )

    options = parser.parse_args()
    options.radarName = options.radarName.upper()

    if not re.fullmatch(r"[A-Z0-9]{4}", options.radarName):
        parser.error("--radarName must be exactly four letters/digits")
    if options.lookbackSecs < 0:
        parser.error("--lookbackSecs must be nonnegative")
    if options.sleepSecs <= 0:
        parser.error("--sleepSecs must be greater than zero")
    if options.timeoutSecs <= 0:
        parser.error("--timeoutSecs must be greater than zero")
    if options.maxRetries < 0:
        parser.error("--maxRetries must be nonnegative")
    if options.retryBaseSecs < 0:
        parser.error("--retryBaseSecs must be nonnegative")
    if options.verbose:
        options.debug = True

    if options.realtimeMode:
        if options.startTime is not None or options.endTime is not None:
            parser.error("do not specify --start or --end with --realtime")
        startTime = endTime = None
        archiveMode = False
    else:
        if options.startTime is None or options.endTime is None:
            parser.error("archive mode requires both --start and --end")
        if options.startTime > options.endTime:
            parser.error("--start must not be later than --end")
        startTime = options.startTime
        endTime = options.endTime
        archiveMode = True

    if options.debug:
        print("Options:", file=sys.stderr)
        for name in (
            "debug", "verbose", "force", "dryRun", "radarName", "outputDir",
            "lookbackSecs", "sleepSecs", "timeoutSecs", "maxRetries",
            "retryBaseSecs", "realtimeMode",
        ):
            print(f"  {name}: {getattr(options, name)}", file=sys.stderr)
        print(f"  archiveMode: {archiveMode}", file=sys.stderr)
        print(f"  startTime: {startTime}", file=sys.stderr)
        print(f"  endTime: {endTime}", file=sys.stderr)


def runCommand(args):
    if options.debug:
        print("running command:", " ".join(args), file=sys.stderr)
    try:
        result = subprocess.run(args, check=False)
    except OSError as exc:
        print(f"ERROR executing {' '.join(args)}: {exc}", file=sys.stderr)
        return False
    if result.returncode != 0:
        print(
            f"WARNING command returned status {result.returncode}: {' '.join(args)}",
            file=sys.stderr,
        )
        return False
    return True


if __name__ == "__main__":
    sys.exit(main())
