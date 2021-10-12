import io
import google.oauth2.credentials
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from spire.config import config


GOOGLE_DRIVE = "drive"
GOOGLE_DRIVE_VERSION = "v3"
GOOGLE_SPREADSHEET_FILE_FORMAT = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"  # noqa
)


class GoogleAPIClient:
    def __init__(self, service, service_version):
        creds = self.__load_credentials()
        self.service = build(
            service, service_version, credentials=creds, cache_discovery=False  # noqa
        )

    def __load_credentials(self):
        return google.oauth2.credentials.Credentials(
            token=None,
            refresh_token=config.GOOGLE_REFRESH_TOKEN,
            token_uri=config.GOOGLE_TOKEN_URI,
            client_id=config.GOOGLE_CLIENT_ID,
            client_secret=config.GOOGLE_CLIENT_SECRET,
        )


class GoogleDriveClient(GoogleAPIClient):
    def __init__(self):
        super(GoogleDriveClient, self).__init__(
            GOOGLE_DRIVE, GOOGLE_DRIVE_VERSION
        )  # noqa

    def get_drive_file_id(self, file_name):
        # Launch Google Drive API service
        #   - cache_discovery parameter is used to suppress
        #     import errors from upstream library

        # Search for specific file ID by file name (q="....")
        results = (
            self.service.files()
            .list(
                pageSize=10,
                fields="nextPageToken, files(id, name)",
                includeTeamDriveItems=True,
                q=f'name = "{file_name}"',
                supportsTeamDrives=True,
            )
            .execute()
        )
        items = results.get("files", [])
        try:
            return items[0]["id"]
        except IndexError:
            raise Exception(f"No files match passed file name {file_name}")

    def download_file(self, file_name):

        file_id = self.get_drive_file_id(file_name)

        # Download contents of queried file
        request = self.service.files().export_media(
            fileId=file_id, mimeType=GOOGLE_SPREADSHEET_FILE_FORMAT
        )  # noqa

        try:
            # Download Sheets file to BytesIO object
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()  # noqa
            fh.seek(0)
            return fh
        except HttpError as err:
            if err.resp.status in [403, 500, 503]:
                print(err)
            return None

    def load_excel_spreadsheet_as_pandas(self, file_name, sheet_name):
        excel_sheet = self.download_file(file_name)
        return pd.read_excel(excel_sheet, sheet_name=sheet_name)
