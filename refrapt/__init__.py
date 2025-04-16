"""Python Debian mirroring tool."""

from sys import stdout, exit as sys_exit
from logging import getLogger, Formatter, StreamHandler
from os import chdir, listdir, remove, makedirs, walk
from os.path import isfile, isdir, islink, getmtime, normpath, join, getsize
from time import perf_counter
from pathlib import Path
from math import floor, log, pow as math_pow
from shutil import copyfile
from datetime import timedelta
from typing import Optional

from tqdm import tqdm
from filelock import FileLock

from refrapt.classes import (
    Repository,
    UrlType,
    Downloader,
    LogFilter,
    Package
)

from refrapt.helpers import SanitiseUri
from refrapt.settings import Settings

logger = getLogger(__name__)
#logFormatter = Formatter("%(asctime)s [%(levelname)8s][%(name)s:%(filename)s:%(lineno)s %(funcName)20s()] - %(message)10s")
#console = StreamHandler()
#console.setFormatter(logFormatter)
#logger.addHandler(console)

repositories = [] # type: list[Repository]
filesToKeep = [] # type : list[str]
appLockFile = "refrapt-lock"

def main(conf: str, test: bool = False, clean: bool = True, no_progress: bool = True):
    """A tool to mirror Debian Repositories for use as a local mirror."""

    global repositories
    global filesToKeep

    startTime = perf_counter()

    logger.info("Starting Refrapt process")

    configData = GetConfig(conf)

    # Parse the configuration file
    Settings.Parse(configData)
    getLogger().setLevel(Settings.LogLevel())

    # Ensure that command line argument for Test overrides if it is set in the configuration file
    if test:
        Settings.EnableTest()

    if Settings.Test():
        logger.info("## Running in Test mode ##\n")

    # Ensure that command line argument for no_progress overrides if it is set in the configuration file
    if no_progress:
        Settings.DisableProgressBars()

    repositories = GetRepositories(configData)

    if not repositories:
        logger.info("No Repositories found in configuration file. Application exiting.")
        sys_exit()

    # Create working directories
    Path(Settings.MirrorPath()).mkdir(parents=True, exist_ok=True)
    Path(Settings.SkelPath()).mkdir(parents=True, exist_ok=True)
    Path(Settings.VarPath()).mkdir(parents=True, exist_ok=True)

    Downloader.Init()

    # Change to the Skel directory for working repository structure
    chdir(Settings.SkelPath())

    # Check for any "-lock" files.
    for file in listdir(Settings.VarPath()):
        if "Download-lock" in file:
            # A download was in progress and interrupted. This means a
            # partial download will be sitting on the drive. Remove
            # it to guarantee that it will be fully downloaded.
            uri = None
            with open(f"{Settings.VarPath()}/{file}") as f:
                uri = f.readline()

            uri = SanitiseUri(uri)
            if isfile(f"{Settings.MirrorPath()}/{uri}"):
                remove(f"{Settings.MirrorPath()}/{uri}")
            elif isfile(f"{Settings.VarPath()}/{uri}"):
                remove(f"{Settings.VarPath()}/{uri}")
            logger.info(f"Removed incomplete download {uri}")
        if appLockFile in file:
            # Refrapt was interrupted during processing.
            # To ensure that files which now may not
            # be marked as Modified due to recently being
            # downloaded, force processing of all files
            logger.info("The previous Refrapt run was interrupted. Full processing will be performed to ensure completeness")
            Settings.SetPreviousRunInterrupted()

    # Delete existing /var files
    logger.info("Removing previous /var files...")
    for item in listdir(Settings.VarPath()):
        remove(f"{Settings.VarPath()}/{item}")

    # Create a lock file for the Application
    with FileLock(f"{Settings.VarPath()}/{appLockFile}.lock"):
        with open(f"{Settings.VarPath()}/{appLockFile}", "w+") as f:
            pass

        if clean:
            PerformClean()
        else:
            PerformMirroring()

    # Lock file no longer required
    remove(f"{Settings.VarPath()}/{appLockFile}")
    if isfile(f"{Settings.VarPath()}/{appLockFile}.lock"):
        # Requires manual deletion on Unix
        remove(f"{Settings.VarPath()}/{appLockFile}.lock")

    logger.info(f"Refrapt completed in {timedelta(seconds=round(perf_counter() - startTime))}")

def PerformClean():
    """Perform the cleaning of files on the local repository."""
    global repositories
    global filesToKeep

    logger.info("## Clean Mode ##")

    cleanRepositories = []

    # 1. Ensure that the Repositories are actually on disk
    for repository in repositories:
        if isdir(f"{Settings.MirrorPath()}/{SanitiseUri(repository.Uri)}/dists/{repository.Distribution}"):
            cleanRepositories.append(repository)
        else:
            logger.debug(f"Repository not found on disk: {SanitiseUri(repository.Uri)} {repository.Distribution}")

    # 2. Get the Release files for each of the Repositories
    releaseFiles = []
    for repository in cleanRepositories:
        releaseFiles += repository.GetReleaseFiles()

    for releaseFile in releaseFiles:
        filesToKeep.append(normpath(SanitiseUri(releaseFile)))

    # 3. Parse the Release files for the list of Index files that are on Disk
    indexFiles = []
    for repository in cleanRepositories:
        indexFiles += repository.ParseReleaseFilesFromLocalMirror()

    for indexFile in indexFiles:
        filesToKeep.append(normpath(SanitiseUri(indexFile)))

    # 4. Generate list of all files on disk according to the Index files
    logger.info("Reading all Packages...")
    fileList = []
    for repository in tqdm(cleanRepositories, position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
        fileList += repository.ParseIndexFilesFromLocalMirror()

    # Packages potentially add duplicates - remove duplicates now
    requiredFiles = [] # type: list[str]
    requiredFiles = list(set(filesToKeep)) + [x.Filename for x in fileList]

    chdir(Settings.MirrorPath())

    Clean(cleanRepositories, requiredFiles)

def PerformMirroring():
    """Perform the main mirroring function of this application."""

    global repositories
    global filesToKeep

    filesToDownload = [] # type: list[Package]
    filesToDownload.clear()

    logger.info(f"Processing {len(repositories)} Repositories...")

    # 1. Get the Release files for each of the Repositories
    releaseFiles = []
    for repository in repositories:
        releaseFiles += repository.GetReleaseFiles()

    logger.debug("Adding Release Files to filesToKeep:")
    for releaseFile in releaseFiles:
        logger.debug(f"\t{SanitiseUri(releaseFile)}")
        filesToKeep.append(normpath(SanitiseUri(releaseFile)))

    logger.info(f"Compiled a list of {len(releaseFiles)} Release files for download")
    Downloader.Download(releaseFiles, UrlType.Release)

    # 1a. Verify after the download that the Repositories actually exist
    allRepos = list(repositories)
    for repository in allRepos:
        if not repository.Exists():
            logger.warning(f"No files were downloaded from Repository '{repository.Uri} {repository.Distribution} {repository.Components}' - Repository will be skipped. Does it actually exist?")
            repositories.remove(repository)

    # 2. Parse the Release files for the list of Index files to download
    indexFiles = []
    for repository in repositories:
        indexFiles += repository.ParseReleaseFilesFromRemote()

    logger.debug("Adding Index Files to filesToKeep:")
    for indexFile in indexFiles:
        logger.debug(f"\t{SanitiseUri(indexFile)}")
        filesToKeep.append(normpath(SanitiseUri(indexFile)))

    logger.info(f"Compiled a list of {len(indexFiles)} Index files for download")
    Downloader.Download(indexFiles, UrlType.Index)

    # Record timestamps of downloaded files to later detemine which files have changed,
    # and therefore need to be processsed
    for repository in repositories:
        repository.Timestamp()

    # 3. Unzip each of the Packages / Sources indices and obtain a list of all files to download
    logger.info("Decompressing Packages / Sources Indices...")
    for repository in tqdm(repositories, position=0, unit=" repo", desc="Repositories ", disable=not Settings.ProgressBarsEnabled()):
        repository.DecompressIndexFiles()

    # 4. Parse all Index files (Package or Source) to collate all files that need to be downloaded
    logger.info("Building file list...")
    for repository in tqdm([x for x in repositories if x.Modified], position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
        filesToDownload += repository.ParseIndexFiles()

    # Packages potentially add duplicate downloads, slowing down the rest
    # of the process. To counteract, remove duplicates now
    filesToKeep = list(set(filesToKeep)) + [x.Filename for x in filesToDownload]

    logger.debug(f"Files to keep: {len(filesToKeep)}")
    for file in filesToKeep:
        logger.debug(f"\t{file}")

    # 5. Perform the main download of Binary and Source files
    downloadSize = ConvertSize(sum([x.Size for x in filesToDownload if not x.Latest]))
    logger.info(f"Compiled a list of {len([x for x in filesToDownload if not x.Latest])} Binary and Source files of size {downloadSize} for download")

    chdir(Settings.MirrorPath())
    if not Settings.Test():
        Downloader.Download([x.Filename for x in filesToDownload if not x.Latest], UrlType.Archive)

    # 6. Copy Skel to Main Archive
    if not Settings.Test():
        logger.info("Copying Skel to Mirror")
        for indexUrl in tqdm(filesToKeep, unit=" files", disable=not Settings.ProgressBarsEnabled()):
            skelFile   = f"{Settings.SkelPath()}/{SanitiseUri(indexUrl)}"
            if isfile(skelFile):
                mirrorFile = f"{Settings.MirrorPath()}/{SanitiseUri(indexUrl)}"
                copy = True
                if isfile(mirrorFile):
                    # Compare files using Timestamp to save moving files that don't need to be
                    skelTimestamp   = getmtime(Path(skelFile))
                    mirrorTimestamp = getmtime(Path(mirrorFile))
                    copy = skelTimestamp > mirrorTimestamp

                if copy:
                    makedirs(Path(mirrorFile).parent.absolute(), exist_ok=True)
                    copyfile(skelFile, mirrorFile)

    # 7. Remove any unused files
    if Settings.CleanEnabled():
        PostMirrorClean()
    else:
        logger.info("Skipping Clean")

    if Settings.Test():
        # Remove Release Files and Index Files added to /skel to ensure normal processing
        # next time the application is run, otherwise the app will think it has all
        # the latest files downloaded, when actually it only has the latest /skel Index files
        chdir(Settings.SkelPath())

        logger.info("Test mode - Removing Release and Index files from /skel")
        for skelFile in releaseFiles + indexFiles:
            file = normpath(f"{Settings.SkelPath()}/{SanitiseUri(skelFile)}")
            if isfile(file):
                remove(file)

def GetConfig(conf: str) -> list:
    """Attempt to read the configuration file using the path provided.

       If the configuration file is not found, a default configuration
       will be written using the path provided, and the application
       will exit.
    """
    if not isfile(conf):
        logger.info("Configuration file not found. Creating default...")
        CreateConfig(conf)
        sys_exit()
    else:
        # Read the configuration file
        with open(conf) as f:
            configData = list(filter(None, f.read().splitlines()))

        logger.debug(f"Read {len(configData)} lines from config")
        return configData

def CreateConfig(conf: str):
    """Create a new configuration file using the default provided.

       If the destination directory for the file does not exist,
       the application will exit.
    """

    path = Path(conf)
    if not isdir(path.parent.absolute()):
        logger.error("Path for configuration file not valid. Application exiting.")
        sys_exit()

    defaultConfigPath = f"~/refrapt/refrapt.conf.example"
    with open(defaultConfigPath, "r") as fIn:
        with open(conf, "w") as f:
            f.writelines(fIn.readlines())

    logger.info(f"Configuration file created for first use at '{conf}'. Add some Repositories and run again. Application exiting.")

def Clean(repos: list, requiredFiles: list):
    """Compiles a list of files to clean, and then removes them from disk"""

    # 5. Determine which files are in the mirror, but not listed in the Index files
    items = [] # type: list[str]
    logger.info("\tCompiling list of files to clean...")
    uris = {repository.Uri.rstrip('/') for repository in repos}

    for uri in tqdm(uris, position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
        walked = [] # type: list[str]
        for root, _, files in tqdm(walk(SanitiseUri(uri)), position=1, unit=" fso", desc="FSO          ", leave=False, delay=0.5, disable=not Settings.ProgressBarsEnabled()):
            for file in tqdm(files, position=2, unit=" file", desc="Files        ", leave=False, delay=0.5, disable=not Settings.ProgressBarsEnabled()):
                walked.append(join(root, file))

        logger.debug(f"{SanitiseUri(uri)}: Walked {len(walked)} items")
        items += [normpath(x) for x in walked if normpath(x) not in requiredFiles and not islink(x)]

    # 5a. Remove any duplicate items
    items = list(set(items))

    logger.debug(f"Found {len(items)} which can be freed")
    for item in items:
        logger.debug(item)

    # 6. Calculate size of items to clean
    if items:
        logger.info("\tCalculating space savings...")
        clearSize = 0
        for file in tqdm(items, unit=" files", leave=False, disable=not Settings.ProgressBarsEnabled()):
            clearSize += getsize(file)
    else:
        logger.info("\tNo files eligible to clean")
        return

    if Settings.Test():
        logger.info(f"\tFound {ConvertSize(clearSize)} in {len(items)} files and directories that could be freed.")
        return

    logger.info(f"\t{ConvertSize(clearSize)} in {len(items)} files and directories will be freed...")

    # 7. Clean files
    for item in items:
        remove(item)

def PostMirrorClean():
    """Clean any files or directories that are not used.

       Determination of whether a file or directory is used
       is based on whether each of the files and directories
       within the path of a given Repository were added to the
       filesToKeep[] variable. If they were not, that means
       based on the current configuration file, the items
       are not required.
    """

    # All Repositories marked as Clean and having been Modified
    cleanRepositories = [x for x in repositories if x.Clean and x.Modified]

    if not cleanRepositories:
        logger.info("Nothing to clean")
        return

    logger.info("Beginning Clean process...")
    logger.debug("Clean Repositories (Modified)")
    for repository in cleanRepositories:
        logger.debug(f"{repository.Uri} [{repository.Distribution}] {repository.Components}")
    # Remaining Repositories with the same URI
    allUriRepositories = []
    for cleanRepository in cleanRepositories:
        allUriRepositories += [x for x in repositories if x.Uri in cleanRepository.Uri]
    # Remove duplicates
    allUriRepositories = list(set(allUriRepositories))

    logger.debug("All Repositories with same URI")
    for repository in allUriRepositories:
        logger.debug(f"{repository.Uri} [{repository.Distribution}] {repository.Components}")

    # In order to not end up removing files that are listed in Indices
    # that were not processed in previous steps, we do need to read the
    # remainder of the Packages and Sources files in for the Repository in order
    # to build a full list of maintained files.
    logger.info("\tProcessing unmodified Indices...")
    umodifiedFiles = [] # type: list[str]
    for repository in tqdm(allUriRepositories, position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
        umodifiedFiles += repository.ParseUnmodifiedIndexFiles()

    # Packages potentially add duplicate downloads, slowing down the rest
    # of the process. To counteract, remove duplicates now
    requiredFiles = [] # type: list[str]
    requiredFiles = list(set(filesToKeep)) + list(set(umodifiedFiles))

    Clean(cleanRepositories, requiredFiles)

def ConvertSize(size: int) -> str:
    """Convert a number of bytes into a number with a suitable unit."""
    if size == 0:
        return "0B"

    sizeName = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(floor(log(size, 1024)))
    p = math_pow(1024, i)
    s = round(size / p, 2)
    return f"{s} {sizeName[i]}"

def GetRepositories(configData: list) -> list:
    """Determine the Repositories listed in the Configuration file."""
    for line in [x for x in configData if x.startswith("deb")]:
        repositories.append(Repository(line, Settings.Architecture()))

    for line in [x for x in configData if x.startswith("clean")]:
        if "False" in line:
            uri = line.split(" ")[1]
            repository = [x for x in repositories if x.Uri == uri]
            repository[0].Clean = False
            logger.debug(f"Not cleaning {uri}")

    return repositories
