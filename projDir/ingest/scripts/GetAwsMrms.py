#!/usr/bin/env python3

#=====================================================================
#
# Safely download MRMS GRIB2 products from the NOAA public AWS archive.
#
#=====================================================================

import argparse
import datetime
import gzip
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

BUCKET_NAME = "noaa-mrms-pds"
BUCKET_URL = f"https://{BUCKET_NAME}.s3.amazonaws.com"
RETRIABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}

# Operational MRMS object names use the product directory name verbatim.
# Supporting uncompressed GRIB2 as well makes the parser tolerant of a future
# bucket-side packaging change without weakening product or time validation.
MRMS_FILE_RE = re.compile(
    r"^MRMS_(?P<product>[A-Za-z0-9_.-]+)_"
    r"(?P<date>\d{8})-(?P<time>\d{6})"
    r"\.grib2(?P<gzip>\.gz)?$"
)


class HelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    """Show defaults while preserving line breaks in descriptions and examples."""


def main():
    global options, startTime, endTime, archiveMode, fileCount, thisScriptName
    global stateDir

    fileCount = 0
    thisScriptName = os.path.basename(__file__)
    parseArgs()

    print("=============================================", file=sys.stderr)
    print(f"BEGIN: {thisScriptName} at {datetime.datetime.now()}", file=sys.stderr)
    print("=============================================", file=sys.stderr)

    os.makedirs(options.outputDir, exist_ok=True)
    # State is kept below a hidden directory so it survives restarts without
    # being mistaken for input by directory-scanning GRIB readers.
    stateDir = os.path.join(options.outputDir, ".get_aws_mrms")
    os.makedirs(stateDir, exist_ok=True)

    if options.realtimeMode:
        lookback = timedelta(seconds=options.lookbackSecs)
        while True:
            fileCount = 0
            endTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            startTime = endTime - lookback
            try:
                manageRetrieval(startTime, endTime)
            except Exception as exc:
                # Exhausting one polling cycle must not terminate a long-running
                # realtime service; the overlapping lookback catches up later.
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
        if not getForInterval(this_day, period_start, period_end):
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
        (
            urllib.error.URLError,
            TimeoutError,
            OSError,
            EOFError,
            http.client.HTTPException,
        ),
    )


def runWithRetries(description, operation):
    """Run a transfer operation with bounded exponential backoff."""
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
            # MRMS product directories are daily. StartAfter prevents every
            # realtime poll from returning all earlier products for that day.
            params["start-after"] = start_after
        if continuation_token:
            params["continuation-token"] = continuation_token

        list_url = f"{BUCKET_URL}/?{urllib.parse.urlencode(params)}"
        if options.debug:
            print(f"list URL: {list_url}", file=sys.stderr)

        def fetchPage():
            request = urllib.request.Request(
                list_url,
                headers={"User-Agent": f"{thisScriptName}/1.0"},
            )
            with urllib.request.urlopen(request, timeout=options.timeoutSecs) as response:
                return response.read()

        xml_data = runWithRetries(f"cannot list S3 prefix {prefix}", fetchPage)
        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError as exc:
            raise RuntimeError(f"cannot parse S3 listing for {prefix}: {exc}") from exc

        # S3 XML uses a default namespace. Removing it keeps traversal simple
        # and independent of the namespace URL returned by the endpoint.
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


def getForInterval(thisDay, start_time, end_time):
    date_str = thisDay.strftime("%Y%m%d")
    prefix = f"{options.domain}/{options.product}/{date_str}/"
    start_after = (
        f"{prefix}MRMS_{options.product}_{start_time:%Y%m%d-%H%M%S}"
    )

    success = True
    keys_found = 0
    for key in list_s3_keys(prefix, start_after=start_after):
        source_name = key.rsplit("/", 1)[-1]
        match = MRMS_FILE_RE.fullmatch(source_name)
        if not match or match.group("product") != options.product:
            if options.verbose:
                print(f"skipping unexpected entry: {key}", file=sys.stderr)
            continue

        file_time = datetime.datetime.strptime(
            match.group("date") + match.group("time"), "%Y%m%d%H%M%S"
        )
        keys_found += 1

        # Filenames under one MRMS product/day prefix sort chronologically.
        if file_time > end_time:
            break
        if file_time < start_time:
            continue

        if options.verbose:
            print(f"checking file: {key}, time: {file_time}", file=sys.stderr)

        final_name = getFinalName(source_name)
        final_path = getFinalPath(file_time, final_name)
        if os.path.isfile(final_path) and not options.force:
            if not retryPendingLdata(file_time, final_name, final_path):
                success = False
            elif options.debug:
                print(f"file previously downloaded: {final_name}", file=sys.stderr)
            continue

        if not doDownload(file_time, key, source_name, final_name):
            success = False

    if options.debug:
        print(f"valid MRMS keys found for {prefix}: {keys_found}", file=sys.stderr)
    return success


def getFinalName(source_name):
    if not options.keepGzip and source_name.endswith(".gz"):
        return source_name[:-3]
    return source_name


def getFinalPath(fileTime, finalName):
    return os.path.join(
        options.outputDir,
        fileTime.strftime("%Y%m%d"),
        finalName,
    )


def getPendingPath(fileTime, finalName):
    return os.path.join(
        stateDir,
        "pending_ldata",
        fileTime.strftime("%Y%m%d"),
        finalName + ".pending",
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
        removeIfPresent(marker_tmp)


def retryPendingLdata(fileTime, finalName, final_path):
    marker_path = getPendingPath(fileTime, finalName)
    if not os.path.exists(marker_path) or not os.path.isfile(final_path):
        return True
    return publishLdata(fileTime, finalName, marker_path)


def removeIfPresent(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def doDownload(fileTime, key, sourceName, finalName):
    global fileCount

    url = f"{BUCKET_URL}/{urllib.parse.quote(key, safe='/')}"
    final_path = getFinalPath(fileTime, finalName)
    marker_path = getPendingPath(fileTime, finalName)
    transfer_dir = os.path.join(stateDir, "downloads")
    source_tmp = os.path.join(transfer_dir, sourceName + f".{os.getpid()}.part")
    final_tmp = os.path.join(transfer_dir, finalName + f".{os.getpid()}.ready")

    if options.debug or options.dryRun:
        action = "Downloading"
        if sourceName != finalName:
            action = "Downloading and decompressing"
        print(f"{action}: {url} -> {final_path}", file=sys.stderr)
    if options.dryRun:
        return True

    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    os.makedirs(transfer_dir, exist_ok=True)

    def prepareFile():
        removeIfPresent(source_tmp)
        removeIfPresent(final_tmp)
        downloadFile(url, source_tmp)

        if sourceName.endswith(".gz") and not options.keepGzip:
            decompressGrib(source_tmp, final_tmp)
            removeIfPresent(source_tmp)
            return final_tmp

        if not options.keepGzip:
            validateGrib(source_tmp)
        return source_tmp

    ready_path = None
    try:
        ready_path = runWithRetries(f"cannot prepare {url}", prepareFile)
        # Record the metadata obligation before publishing the GRIB. A crash
        # after os.replace() is repaired by a later overlapping poll.
        writePendingMarker(marker_path, final_path)
        os.replace(ready_path, final_path)
    except (RuntimeError, OSError) as exc:
        print(f"ERROR downloading {url}: {exc}", file=sys.stderr)
        removeIfPresent(source_tmp)
        removeIfPresent(final_tmp)
        return False

    removeIfPresent(source_tmp)
    removeIfPresent(final_tmp)
    fileCount += 1
    return publishLdata(fileTime, finalName, marker_path)


def downloadFile(url, destination):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": f"{thisScriptName}/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=options.timeoutSecs) as response, \
                open(destination, "wb") as out:
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
        removeIfPresent(destination)
        raise


def decompressGrib(source, destination):
    try:
        with gzip.open(source, "rb") as compressed, open(destination, "wb") as out:
            while True:
                block = compressed.read(1024 * 1024)
                if not block:
                    break
                out.write(block)
            out.flush()
            os.fsync(out.fileno())
        validateGrib(destination)
    except Exception:
        removeIfPresent(destination)
        raise


def validateGrib(path):
    # GRIB editions 1 and 2 both begin with this signature. Checking it catches
    # corrupt gzip content and error documents before downstream publication.
    with open(path, "rb") as data:
        if data.read(4) != b"GRIB":
            raise OSError(f"decompressed file does not have a GRIB signature: {path}")


def publishLdata(fileTime, finalName, marker_path):
    args = [
        "LdataWriter",
        "-dir", options.outputDir,
        "-rpath", os.path.join(fileTime.strftime("%Y%m%d"), finalName),
        "-ltime", fileTime.strftime("%Y%m%d%H%M%S"),
        "-writer", thisScriptName,
        "-dtype", options.dataType,
    ]

    for attempt in range(options.maxRetries + 1):
        if runCommand(args):
            removeIfPresent(marker_path)
            return True
        if attempt < options.maxRetries:
            delay = retryDelay(attempt)
            print(
                f"WARNING LdataWriter failed; retrying in {delay:g} seconds",
                file=sys.stderr,
            )
            time.sleep(delay)

    # Preserve both the complete data and marker. The next lookback cycle can
    # repair metadata without downloading and decompressing the object again.
    print(f"ERROR LdataWriter retries exhausted for {finalName}", file=sys.stderr)
    return False


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
        description="Safely download one MRMS GRIB2 product from AWS.",
        formatter_class=HelpFormatter,
        epilog=(
            'Archive example:\n'
            '  %(prog)s --product MergedReflectivityQCComposite_00.50 '
            '--start "2026 07 20 12 00 00" '
            '--end "2026 07 20 13 00 00"\n\n'
            'Realtime example:\n'
            '  %(prog)s --product MergedReflectivityQCComposite_00.50 '
            '--realtime --outputDir /data/raw/mrms/ReflQcComp'
        ),
    )

    parser.add_argument("--debug", action="store_true", help="Set debugging on")
    parser.add_argument("--verbose", action="store_true", help="Set verbose debugging on")
    parser.add_argument(
        "--domain", default="CONUS", metavar="DOMAIN",
        help="MRMS domain at the bucket's top level",
    )
    parser.add_argument(
        "--product", default="MergedReflectivityQCComposite_00.50",
        metavar="PRODUCT", help="Exact MRMS product directory name",
    )
    parser.add_argument(
        "--outputDir", default="/tmp/aws/mrms", metavar="DIR",
        help="Product directory to which YYYYMMDD/file paths are written",
    )
    parser.add_argument(
        "--dataType", default="grib2", metavar="TYPE",
        help="Data type recorded by LdataWriter",
    )
    parser.add_argument(
        "--keepGzip", action="store_true",
        help="Publish the original .grib2.gz instead of decompressing it",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Transfer even if the output file was previously downloaded",
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
        "--lookbackSecs", type=int, default=900, metavar="SECS",
        help="Lookback in realtime mode",
    )
    parser.add_argument(
        "--sleepSecs", type=int, default=15, metavar="SECS",
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
    options.domain = options.domain.upper()

    if not re.fullmatch(r"[A-Z0-9_]+", options.domain):
        parser.error("--domain may contain only uppercase letters, digits, and underscore")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", options.product):
        parser.error("--product contains unsupported characters")
    if not options.dataType or any(char.isspace() for char in options.dataType):
        parser.error("--dataType must be a nonempty value without whitespace")
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
            "debug", "verbose", "force", "dryRun", "domain", "product",
            "outputDir", "dataType", "keepGzip", "lookbackSecs", "sleepSecs",
            "timeoutSecs", "maxRetries", "retryBaseSecs", "realtimeMode",
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
