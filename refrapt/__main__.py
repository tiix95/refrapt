"""Python Debian mirroring tool."""

from sys import stdout, exit as sys_exit
from logging import getLogger, Formatter, StreamHandler, Logger
from os import chdir, listdir, remove, makedirs, walk
from os.path import isfile, isdir, islink, getmtime, normpath, join, getsize
from time import perf_counter
from pathlib import Path
from math import floor, log, pow as math_pow
from shutil import copyfile
from datetime import timedelta
from typing import Optional, List

from tqdm import tqdm
from filelock import FileLock

from refrapt.classes import (
    Repository,
    UrlType,
    Downloader,
    Package
)

from refrapt.helpers import SanitiseUri
from refrapt.settings import Settings

class Refrapt:

    def __init__(self, log: Logger) -> None:

        self.logger = log
        self.repositories: List[Repository] = []
        self.filesToKeep: List[str] = []
        self.appLockFile = "refrapt-lock"

    def main(self, conf: str, test: bool = False, clean: bool = True, no_progress: bool = True):
        """A tool to mirror Debian Repositories for use as a local mirror."""

        startTime = perf_counter()

        self.logger.info("Starting Refrapt process")

        configData = self.GetConfig(conf)

        # Parse the configuration file
        Settings.Parse(configData, self.logger)
        #getLogger().setLevel(Settings.LogLevel())

        # Ensure that command line argument for Test overrides if it is set in the configuration file
        if test:
            Settings.EnableTest()

        if Settings.Test():
            self.logger.info("## Running in Test mode ##\n")

        # Ensure that command line argument for no_progress overrides if it is set in the configuration file
        if no_progress:
            Settings.DisableProgressBars()

        self.GetRepositories(configData)

        if not self.repositories:
            self.logger.info("No Repositories found in configuration file. Application exiting.")
            sys_exit()

        # Create working directories
        self.logger.info(f"-----------------> Creating {Settings.MirrorPath()}")
        Path(Settings.MirrorPath()).mkdir(parents=True, exist_ok=True)
        self.logger.info(f"-----------------> Creating {Settings.SkelPath()}")
        Path(Settings.SkelPath()).mkdir(parents=True, exist_ok=True)
        self.logger.info(f"-----------------> Creating {Settings.VarPath()}")
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
                self.logger.info(f"Removed incomplete download {uri}")
            if self.appLockFile in file:
                # Refrapt was interrupted during processing.
                # To ensure that files which now may not
                # be marked as Modified due to recently being
                # downloaded, force processing of all files
                self.logger.info("The previous Refrapt run was interrupted. Full processing will be performed to ensure completeness")
                Settings.SetPreviousRunInterrupted()

        # Delete existing /var files
        self.logger.info("Removing previous /var files...")
        for item in listdir(Settings.VarPath()):
            remove(f"{Settings.VarPath()}/{item}")

        # Create a lock file for the Application
        with FileLock(f"{Settings.VarPath()}/{self.appLockFile}.lock"):
            with open(f"{Settings.VarPath()}/{self.appLockFile}", "w+") as f:
                pass

            if clean:
                self.PerformClean()
            else:
                self.PerformMirroring()

        # Lock file no longer required
        remove(f"{Settings.VarPath()}/{self.appLockFile}")
        if isfile(f"{Settings.VarPath()}/{self.appLockFile}.lock"):
            # Requires manual deletion on Unix
            remove(f"{Settings.VarPath()}/{self.appLockFile}.lock")

        self.logger.info(f"Refrapt completed in {timedelta(seconds=round(perf_counter() - startTime))}")

    def PerformClean(self) -> None:
        """Perform the cleaning of files on the local repository."""

        self.logger.info("## Clean Mode ##")

        cleanRepositories = []

        # 1. Ensure that the Repositories are actually on disk
        for repository in self.repositories:
            if isdir(f"{Settings.MirrorPath()}/{SanitiseUri(repository.Uri)}/dists/{repository.Distribution}"):
                cleanRepositories.append(repository)
            else:
                self.logger.debug(f"Repository not found on disk: {SanitiseUri(repository.Uri)} {repository.Distribution}")

        # 2. Get the Release files for each of the Repositories
        releaseFiles = []
        for repository in cleanRepositories:
            releaseFiles += repository.GetReleaseFiles()

        for releaseFile in releaseFiles:
            self.filesToKeep.append(normpath(SanitiseUri(releaseFile)))

        # 3. Parse the Release files for the list of Index files that are on Disk
        indexFiles = []
        for repository in cleanRepositories:
            indexFiles += repository.ParseReleaseFilesFromLocalMirror()

        for indexFile in indexFiles:
            self.filesToKeep.append(normpath(SanitiseUri(indexFile)))

        # 4. Generate list of all files on disk according to the Index files
        self.logger.info("Reading all Packages...")
        fileList = []
        for repository in tqdm(cleanRepositories, position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
            fileList += repository.ParseIndexFilesFromLocalMirror()

        # Packages potentially add duplicates - remove duplicates now
        requiredFiles = [] # type: list[str]
        requiredFiles = list(set(self.filesToKeep)) + [x.Filename for x in fileList]

        chdir(Settings.MirrorPath())

        self.Clean(cleanRepositories, requiredFiles)

    def PerformMirroring(self) -> None:
        """Perform the main mirroring function of this application."""

        filesToDownload = [] # type: list[Package]
        filesToDownload.clear()

        self.logger.info(f"Processing {len(self.repositories)} Repositories...")

        # 1. Get the Release files for each of the Repositories
        releaseFiles = []
        for repository in self.repositories:
            releaseFiles += repository.GetReleaseFiles()

        self.logger.debug("Adding Release Files to filesToKeep:")
        for releaseFile in releaseFiles:
            self.logger.debug(f"\t{SanitiseUri(releaseFile)}")
            self.filesToKeep.append(normpath(SanitiseUri(releaseFile)))

        self.logger.info(f"Compiled a list of {len(releaseFiles)} Release files for download")
        Downloader.Download(releaseFiles, UrlType.Release, self.logger)

        # 1a. Verify after the download that the Repositories actually exist
        allRepos = list(self.repositories)
        for repository in allRepos:
            if not repository.Exists():
                self.logger.warning(f"No files were downloaded from Repository '{repository.Uri} {repository.Distribution} {repository.Components}' - Repository will be skipped. Does it actually exist?")
                self.repositories.remove(repository)

        # 2. Parse the Release files for the list of Index files to download
        indexFiles = []
        for repository in self.repositories:
            indexFiles += repository.ParseReleaseFilesFromRemote()

        self.logger.debug("Adding Index Files to filesToKeep:")
        for indexFile in indexFiles:
            self.logger.debug(f"\t{SanitiseUri(indexFile)}")
            self.filesToKeep.append(normpath(SanitiseUri(indexFile)))

        self.logger.info(f"Compiled a list of {len(indexFiles)} Index files for download")
        Downloader.Download(indexFiles, UrlType.Index, self.logger)

        # Record timestamps of downloaded files to later detemine which files have changed,
        # and therefore need to be processsed
        for repository in self.repositories:
            repository.Timestamp()

        # 3. Unzip each of the Packages / Sources indices and obtain a list of all files to download
        self.logger.info("Decompressing Packages / Sources Indices...")
        for repository in tqdm(self.repositories, position=0, unit=" repo", desc="Repositories ", disable=not Settings.ProgressBarsEnabled()):
            repository.DecompressIndexFiles()

        # 4. Parse all Index files (Package or Source) to collate all files that need to be downloaded
        self.logger.info("Building file list...")
        for repository in tqdm([x for x in self.repositories if x.Modified], position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
            filesToDownload += repository.ParseIndexFiles()

        # Packages potentially add duplicate downloads, slowing down the rest
        # of the process. To counteract, remove duplicates now
        self.filesToKeep = list(set(self.filesToKeep)) + [x.Filename for x in filesToDownload]

        self.logger.debug(f"Files to keep: {len(self.filesToKeep)}")
        for file in self.filesToKeep:
            self.logger.debug(f"\t{file}")

        # 5. Perform the main download of Binary and Source files
        downloadSize = self.ConvertSize(sum([x.Size for x in filesToDownload if not x.Latest]))
        self.logger.info(f"Compiled a list of {len([x for x in filesToDownload if not x.Latest])} Binary and Source files of size {downloadSize} for download")

        chdir(Settings.MirrorPath())
        if not Settings.Test():
            Downloader.Download([x.Filename for x in filesToDownload if not x.Latest], UrlType.Archive, self.logger)

        # 6. Copy Skel to Main Archive
        if not Settings.Test():
            self.logger.info("Copying Skel to Mirror")
            for indexUrl in tqdm(self.filesToKeep, unit=" files", disable=not Settings.ProgressBarsEnabled()):
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
            self.PostMirrorClean()
        else:
            self.logger.info("Skipping Clean")

        if Settings.Test():
            # Remove Release Files and Index Files added to /skel to ensure normal processing
            # next time the application is run, otherwise the app will think it has all
            # the latest files downloaded, when actually it only has the latest /skel Index files
            chdir(Settings.SkelPath())

            self.logger.info("Test mode - Removing Release and Index files from /skel")
            for skelFile in releaseFiles + indexFiles:
                file = normpath(f"{Settings.SkelPath()}/{SanitiseUri(skelFile)}")
                if isfile(file):
                    remove(file)

    def GetConfig(self, conf: str) -> list:
        """Attempt to read the configuration file using the path provided.

           If the configuration file is not found, a default configuration
           will be written using the path provided, and the application
           will exit.
        """
        if not isfile(conf):
            self.logger.info("Configuration file not found. Creating default...")
            CreateConfig(conf)
            sys_exit()
        else:
            # Read the configuration file
            with open(conf) as f:
                configData = list(filter(None, f.read().splitlines()))

            self.logger.debug(f"Read {len(configData)} lines from config")
            return configData

    def CreateConfig(self, conf: str):
        """Create a new configuration file using the default provided.

           If the destination directory for the file does not exist,
           the application will exit.
        """

        path = Path(conf)
        if not isdir(path.parent.absolute()):
            self.logger.error("Path for configuration file not valid. Application exiting.")
            sys_exit()

        defaultConfigPath = f"~/refrapt/refrapt.conf.example"
        with open(defaultConfigPath, "r") as fIn:
            with open(conf, "w") as f:
                f.writelines(fIn.readlines())

        self.logger.info(f"Configuration file created for first use at '{conf}'. Add some Repositories and run again. Application exiting.")

    def Clean(self, repos: list, requiredFiles: list) -> None:
        """Compiles a list of files to clean, and then removes them from disk"""

        # 5. Determine which files are in the mirror, but not listed in the Index files
        items = [] # type: list[str]
        self.logger.info("\tCompiling list of files to clean...")
        uris = {repository.Uri.rstrip('/') for repository in repos}

        for uri in tqdm(uris, position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
            walked = [] # type: list[str]
            for root, _, files in tqdm(walk(SanitiseUri(uri)), position=1, unit=" fso", desc="FSO          ", leave=False, delay=0.5, disable=not Settings.ProgressBarsEnabled()):
                for file in tqdm(files, position=2, unit=" file", desc="Files        ", leave=False, delay=0.5, disable=not Settings.ProgressBarsEnabled()):
                    walked.append(join(root, file))

            self.logger.debug(f"{SanitiseUri(uri)}: Walked {len(walked)} items")
            items += [normpath(x) for x in walked if normpath(x) not in requiredFiles and not islink(x)]

        # 5a. Remove any duplicate items
        items = list(set(items))

        self.logger.debug(f"Found {len(items)} which can be freed")
        for item in items:
            self.logger.debug(item)

        # 6. Calculate size of items to clean
        if items:
            self.logger.info("\tCalculating space savings...")
            clearSize = 0
            for file in tqdm(items, unit=" files", leave=False, disable=not Settings.ProgressBarsEnabled()):
                clearSize += getsize(file)
        else:
            self.logger.info("\tNo files eligible to clean")
            return

        if Settings.Test():
            self.logger.info(f"\tFound {self.ConvertSize(clearSize)} in {len(items)} files and directories that could be freed.")
            return

        self.logger.info(f"\t{self.ConvertSize(clearSize)} in {len(items)} files and directories will be freed...")

        # 7. Clean files
        for item in items:
            remove(item)

    def PostMirrorClean(self) -> None:
        """Clean any files or directories that are not used.

           Determination of whether a file or directory is used
           is based on whether each of the files and directories
           within the path of a given Repository were added to the
           filesToKeep[] variable. If they were not, that means
           based on the current configuration file, the items
           are not required.
        """

        # All Repositories marked as Clean and having been Modified
        cleanRepositories = [x for x in self.repositories if x.Clean and x.Modified]

        if not cleanRepositories:
            self.logger.info("Nothing to clean")
            return

        self.logger.info("Beginning Clean process...")
        self.logger.debug("Clean Repositories (Modified)")
        for repository in cleanRepositories:
            self.logger.debug(f"{repository.Uri} [{repository.Distribution}] {repository.Components}")
        # Remaining Repositories with the same URI
        allUriRepositories = []
        for cleanRepository in cleanRepositories:
            allUriRepositories += [x for x in self.repositories if x.Uri in cleanRepository.Uri]
        # Remove duplicates
        allUriRepositories = list(set(allUriRepositories))

        self.logger.debug("All Repositories with same URI")
        for repository in allUriRepositories:
            self.logger.debug(f"{repository.Uri} [{repository.Distribution}] {repository.Components}")

        # In order to not end up removing files that are listed in Indices
        # that were not processed in previous steps, we do need to read the
        # remainder of the Packages and Sources files in for the Repository in order
        # to build a full list of maintained files.
        self.logger.info("\tProcessing unmodified Indices...")
        umodifiedFiles = [] # type: list[str]
        for repository in tqdm(allUriRepositories, position=0, unit=" repo", desc="Repositories ", leave=False, disable=not Settings.ProgressBarsEnabled()):
            umodifiedFiles += repository.ParseUnmodifiedIndexFiles()

        # Packages potentially add duplicate downloads, slowing down the rest
        # of the process. To counteract, remove duplicates now
        requiredFiles = [] # type: list[str]
        requiredFiles = list(set(self.filesToKeep)) + list(set(umodifiedFiles))

        self.Clean(cleanRepositories, requiredFiles)

    @staticmethod
    def ConvertSize(size: int) -> str:
        """Convert a number of bytes into a number with a suitable unit."""
        if size == 0:
            return "0B"

        sizeName = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(floor(log(size, 1024)))
        p = math_pow(1024, i)
        s = round(size / p, 2)
        return f"{s} {sizeName[i]}"

    def GetRepositories(self, configData: list) -> None:
        """Determine the Repositories listed in the Configuration file."""
        for line in [x for x in configData if x.startswith("deb")]:
            self.repositories.append(Repository(line, Settings.Architecture(), self.logger))

        for line in [x for x in configData if x.startswith("clean")]:
            if "False" in line:
                uri = line.split(" ")[1]
                repository = [x for x in self.repositories if x.Uri == uri]
                repository[0].Clean = False
                self.logger.debug(f"Not cleaning {uri}")
