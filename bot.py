from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import logging
import requests
from decouple import config
from urlextract import URLExtract
from dataclasses import dataclass
from typing import Optional, List
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TwitchConfig:
    """Configuration class for Twitch API settings."""
    client_id: str
    client_secret: str
    broadcaster_id: str
    channel_name: str

@dataclass
class StreamInfo:
    """Data class for stream information."""
    title: str
    timestamp: int
    links: List[str]

class TwitchAPI:
    """Handle Twitch API interactions."""
    def __init__(self, config: TwitchConfig):
        self.config = config
        self._access_token = None
        self._token_expiry = 0

    def _refresh_token_if_needed(self) -> None:
        """Refresh the access token if expired."""
        current_time = time.time()
        if not self._access_token or current_time >= self._token_expiry:
            self._get_new_token()

    def _get_new_token(self) -> None:
        """Get a new access token from Twitch."""
        try:
            response = requests.post(
                'https://id.twitch.tv/oauth2/token',
                json={
                    'client_id': self.config.client_id,
                    'client_secret': self.config.client_secret,
                    'grant_type': 'client_credentials'
                }
            )
            response.raise_for_status()
            data = response.json()
            self._access_token = data['access_token']
            self._token_expiry = time.time() + data['expires_in'] - 300  # Refresh 5 minutes early
        except requests.RequestException as e:
            logger.error(f"Failed to get Twitch token: {e}")
            raise

    def get_stream_info(self) -> Optional[str]:
        """Get current stream title."""
        self._refresh_token_if_needed()
        try:
            response = requests.get(
                f'https://api.twitch.tv/helix/channels?broadcaster_id={self.config.broadcaster_id}',
                headers={
                    'Authorization': f'Bearer {self._access_token}',
                    'Client-Id': self.config.client_id
                }
            )
            response.raise_for_status()
            return response.json()['data'][0]['title']
        except requests.RequestException as e:
            logger.error(f"Failed to get stream info: {e}")
            return None

class SheetsAPI:
    """Handle Google Sheets API interactions."""
    def __init__(self, spreadsheet_id: str, credentials_file: str):
        self.spreadsheet_id = spreadsheet_id
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        self.service = build("sheets", "v4", credentials=self.credentials)

    def write_link(self, stream_info: StreamInfo, link: str) -> bool:
        """Write a link and associated information to Google Sheets."""
        try:
            date = datetime.utcfromtimestamp(stream_info.timestamp).strftime('%Y-%m-%d')
            time_str = datetime.utcfromtimestamp(stream_info.timestamp).strftime('%H:%M:%S')
            
            values = [[stream_info.title, date, time_str, link]]
            body = {"values": values}
            
            result = (
                self.service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=self.spreadsheet_id,
                    range="Sheet1!A1:D",
                    valueInputOption="USER_ENTERED",
                    body=body,
                )
                .execute()
            )
            logger.info(f"Successfully updated {result.get('updatedCells')} cells")
            return True
        except HttpError as error:
            logger.error(f"Failed to write to sheets: {error}")
            return False

class LinkAggregator:
    """Main class to handle link aggregation from Twitch chat."""
    def __init__(self, twitch_api: TwitchAPI, sheets_api: SheetsAPI):
        self.twitch_api = twitch_api
        self.sheets_api = sheets_api
        self.url_extractor = URLExtract()

    def process_message(self, message: dict) -> None:
        """Process incoming Twitch chat messages."""
        if not self._is_authorized_user(message):
            return

        chat_message = message['message']
        if not self.url_extractor.has_urls(chat_message):
            return

        stream_title = self.twitch_api.get_stream_info()
        if not stream_title:
            logger.error("Failed to get stream title")
            return

        timestamp = int(message['tmi-sent-ts'][:-3])
        urls = self.url_extractor.find_urls(chat_message)
        
        stream_info = StreamInfo(
            title=stream_title,
            timestamp=timestamp,
            links=urls
        )

        for link in urls:
            if self.sheets_api.write_link(stream_info, link):
                logger.info(f'Successfully wrote {link} to database')
            else:
                logger.error(f'Failed to write {link} to database')

    @staticmethod
    def _is_authorized_user(message: dict) -> bool:
        """Check if the message is from an authorized user."""
        return message['user-type'] == 'mod' or message['display-name'] == 'MLH'

def main():
    """Main function to set up and run the link aggregator."""
    try:
        twitch_config = TwitchConfig(
            client_id=config('CLIENT_ID'),
            client_secret=config('SECRET'),
            broadcaster_id=config('BROADCASTER_ID'),
            channel_name=config('CHANNEL_NAME')
        )

        twitch_api = TwitchAPI(twitch_config)
        sheets_api = SheetsAPI(
            spreadsheet_id=config('SPREADSHEET_ID'),
            credentials_file=config('GOOGLE_CREDENTIALS_FILE')
        )
        
        aggregator = LinkAggregator(twitch_api, sheets_api)
        
        connection = twitch_chat_irc.TwitchChatIRC()
        connection.listen(twitch_config.channel_name, on_message=aggregator.process_message)
    
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()