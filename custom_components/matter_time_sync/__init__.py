"""Matter Time Sync Integration (Native Async)."""
import logging
import asyncio
from datetime import datetime, timedelta, timezone  # <--- FIXED: Added timedelta
from zoneinfo import ZoneInfo

import aiohttp
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers import device_registry as dr
import homeassistant.helpers.config_validation as cv

_LOGGER = logging.getLogger(__name__)

DOMAIN = "matter_time_sync"
SERVICE_SYNC_TIME = "sync_time"

# Matter Constants
CLUSTER_ID_TIME_SYNC = 0x0038
CMD_ID_SET_UTC_TIME = 0x00
CMD_ID_SET_TIME_ZONE = 0x02
CMD_ID_SET_DST_OFFSET = 0x03

MATTER_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
MICROSECONDS_PER_SECOND = 1_000_000

def validate_sync_time_data(data):
    """Validate that either device_id or node_id is provided, but not both."""
    has_device_id = data.get("device_id") is not None
    has_node_id = data.get("node_id") is not None
    
    if not has_device_id and not has_node_id:
        raise vol.Invalid("Either device_id or node_id must be provided")
    if has_device_id and has_node_id:
        raise vol.Invalid("Provide either device_id or node_id, but not both")
    return data

SYNC_TIME_SCHEMA = vol.Schema(
    vol.All(
        {
            vol.Optional("node_id"): cv.positive_int,
            vol.Optional("device_id"): cv.string,
            vol.Optional("endpoint", default=0): cv.positive_int,
        },
        validate_sync_time_data,
    )
)

async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the component via YAML (stub)."""
    return True

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up from a config entry."""
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = entry.data

    async def handle_sync_time(call: ServiceCall) -> None:
        """Handle the sync_time service call."""
        device_id = call.data.get("device_id")
        node_id = call.data.get("node_id")
        endpoint = call.data["endpoint"]

        # Extract node_id from device if device_id is provided
        if device_id and not node_id:
            device_registry = dr.async_get(hass)
            device = device_registry.async_get(device_id)
            
            if not device:
                _LOGGER.error("Device %s not found", device_id)
                raise ValueError(f"Device {device_id} not found")
            
            # Extract Matter node_id from device identifiers
            # Matter devices can have identifiers in different formats:
            # - ('matter', 'deviceid_<fabric_id>-<node_id>-MatterNodeDevice')
            # - ('matter', '<fabric_id>-<node_id>')
            extracted_node_id = None
            for identifier in device.identifiers:
                # Safely check if this is a Matter identifier
                if not isinstance(identifier, (tuple, list)) or len(identifier) < 2:
                    continue

                if identifier[0] == "matter":
                    _LOGGER.debug("Found Matter identifier: %s", identifier)
                    identifier_value = identifier[1]
                    parts = identifier_value.split("-")

                    if len(parts) < 2:
                        _LOGGER.warning(
                            "Matter identifier value does not contain expected format: %s",
                            identifier_value
                        )
                        continue

                    # Try to extract node_id from different positions in the split parts
                    # Format 1: deviceid_<fabric_id>-<node_id>-MatterNodeDevice (parts[1] is node_id)
                    # Format 2: <fabric_id>-<node_id> (parts[-1] is node_id)
                    
                    # Try parsing parts from the second element onwards (skip first as it may be deviceid_xxx)
                    # Matter node IDs are 64-bit unsigned integers (0 to 2^64-1)
                    for i in range(1, len(parts)):
                        try:
                            parsed_value = int(parts[i])
                            # Validate it's a valid node_id (64-bit unsigned integer)
                            # This filters out negative values and values that are too large
                            if 0 <= parsed_value <= 0xFFFFFFFFFFFFFFFF:
                                extracted_node_id = parsed_value
                                _LOGGER.debug(
                                    "Successfully extracted node_id %s from identifier %s (part %d)",
                                    extracted_node_id, identifier_value, i
                                )
                                break
                        except (ValueError, OverflowError):
                            # This part is not a valid integer or too large, try next
                            continue
                    
                    if extracted_node_id:
                        break
                    else:
                        _LOGGER.warning(
                            "Could not find valid node_id in Matter identifier: %s",
                            identifier_value
                        )
            
            if not extracted_node_id:
                _LOGGER.error("Could not extract Matter node_id from device %s", device_id)
                raise ValueError(f"Could not extract Matter node_id from device {device_id}")
            
            node_id = extracted_node_id
            _LOGGER.info("Extracted node_id %s from device %s", node_id, device_id)

        # No need for additional validation here - schema validation ensures
        # exactly one of device_id or node_id is provided

        # Read from config data, fallback to Home Assistant config
        ws_address = entry.data.get("websocket_address", "ws://core-matter-server:5580/ws")
        tz_name = entry.data.get("timezone", hass.config.time_zone)

        _LOGGER.info("Starting sync for node %s (ep %s)", node_id, endpoint)
        
        session = async_get_clientsession(hass)
        syncer = MatterTimeSyncAsync(session, ws_address, tz_name)
        
        try:
            await syncer.run_sync(node_id, endpoint)
            _LOGGER.info("SUCCESS: Time synced for node %s", node_id)
        except Exception as e:
            _LOGGER.error("FAILED: Time sync error: %s", e)
            raise

    hass.services.async_register(
        DOMAIN, SERVICE_SYNC_TIME, handle_sync_time, schema=SYNC_TIME_SCHEMA
    )
    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    hass.services.async_remove(DOMAIN, SERVICE_SYNC_TIME)
    if entry.entry_id in hass.data[DOMAIN]:
        hass.data[DOMAIN].pop(entry.entry_id)
    return True

class MatterTimeSyncAsync:
    """Async Matter time synchronization."""
    def __init__(self, session: aiohttp.ClientSession, ws_address: str, tz_name: str):
        self.session = session
        self.ws_address = ws_address
        self.tz_name = tz_name
        self.message_counter = 1

    async def run_sync(self, node_id, endpoint):
        _LOGGER.debug("Connecting to Matter server at %s", self.ws_address)
        async with self.session.ws_connect(self.ws_address) as ws:
            # 1. Welcome
            welcome = await ws.receive_json()
            _LOGGER.debug("Server Welcome: %s", welcome)

            _LOGGER.debug("Calculating timezone info for %s", self.tz_name)
            tz_info = await self.get_timezone_info(self.tz_name)
            _LOGGER.debug("Timezone info calculated: offset=%s, dst_adjustment=%s", 
                         tz_info["offset_seconds"], tz_info["dst_adjustment_seconds"])

            # 2. Time Zone
            tz_obj = {"offset": tz_info["offset_seconds"], "validAt": 0, "name": self.tz_name}
            _LOGGER.debug("Sending SetTimeZone command for node %s", node_id)
            await self.send_command(ws, node_id, endpoint, CLUSTER_ID_TIME_SYNC, CMD_ID_SET_TIME_ZONE, "SetTimeZone", {"timeZone": [tz_obj]})

            # 3. DST Offset
            dst_obj = {
                "offset": tz_info["dst_adjustment_seconds"],
                "validStarting": self.to_matter_microseconds(tz_info["dst_start"]),
                "validUntil": self.to_matter_microseconds(tz_info["dst_end"]),
            }
            _LOGGER.debug("Sending SetDSTOffset command for node %s", node_id)
            await self.send_command(ws, node_id, endpoint, CLUSTER_ID_TIME_SYNC, CMD_ID_SET_DST_OFFSET, "SetDSTOffset", {"DSTOffset": [dst_obj]})

            # 4. UTC Time
            now_utc = datetime.now(timezone.utc)
            _LOGGER.debug("Sending SetUTCTime command for node %s with time %s", node_id, now_utc)
            await self.send_command(ws, node_id, endpoint, CLUSTER_ID_TIME_SYNC, CMD_ID_SET_UTC_TIME, "SetUTCTime", {"UTCTime": self.to_matter_microseconds(now_utc), "granularity": 4})

    async def send_command(self, ws, node_id, endpoint, cluster_id, command_id, command_name, payload):
        message = {
            "message_id": str(self.message_counter),
            "command": "device_command",
            "args": {
                "endpoint_id": endpoint,
                "node_id": node_id,
                "payload": payload,
                "cluster_id": cluster_id,
                "command_name": command_name,
            },
        }
        _LOGGER.debug("Sending command %s (msg_id: %s) to node %s", command_name, self.message_counter, node_id)
        await ws.send_json(message)
        response = await ws.receive_json()
        _LOGGER.debug("Received response for %s: %s", command_name, response)
        
        if not response: raise Exception("Empty response")
        if "error_code" in response: raise Exception(f"Matter Server Error: {response}")
        if isinstance(response.get("result"), dict) and "error" in response["result"]:
             raise Exception(f"Command failed: {response['result']}")

        self.message_counter += 1

    @staticmethod
    def to_matter_microseconds(dt):
        if not dt: return 0
        delta = dt - MATTER_EPOCH
        return int(delta.total_seconds() * MICROSECONDS_PER_SECOND)

    @staticmethod
    async def get_timezone_info(tz_name):
        return await asyncio.get_running_loop().run_in_executor(None, MatterTimeSyncAsync._calc_tz, tz_name)

    @staticmethod
    def _calc_tz(tz_name):
        tz = ZoneInfo(tz_name)
        now_utc = datetime.now(timezone.utc)
        year = now_utc.year
        monthly_offsets = [datetime(year, m, 1, tzinfo=timezone.utc).astimezone(tz).utcoffset() for m in range(1, 13)]
        standard_offset = min(monthly_offsets)
        standard_seconds = int(standard_offset.total_seconds())
        max_offset = max(monthly_offsets)
        dst_adjustment_seconds = int(max_offset.total_seconds()) - standard_seconds
        dst_start, dst_end = MatterTimeSyncAsync._find_dst_transitions(year, tz, timezone.utc)
        return {"offset_seconds": standard_seconds, "dst_adjustment_seconds": dst_adjustment_seconds, "dst_start": dst_start, "dst_end": dst_end}

    @staticmethod
    def _find_dst_transitions(year, tz, utc):
        """Find DST transition times for a given year.
        
        Uses an optimized algorithm that samples at day resolution first,
        then narrows down to hourly precision only around transitions.
        This is ~28x faster than checking every hour of the year.
        """
        start = datetime(year, 1, 1, tzinfo=utc)
        end = datetime(year + 1, 1, 1, tzinfo=utc)
        dst_start = dst_end = None
        current = start
        prev_off = current.astimezone(tz).utcoffset()
        
        # Sample at day resolution to find approximate transitions
        while current < end:
            curr_off = current.astimezone(tz).utcoffset()
            
            if curr_off != prev_off:
                # Found a transition, narrow it down with hourly precision
                trans_start = current - timedelta(days=1)
                trans_end = current + timedelta(days=1)
                trans_current = trans_start
                
                while trans_current < trans_end:
                    trans_off = trans_current.astimezone(tz).utcoffset()
                    if trans_off != prev_off:
                        if int(trans_off.total_seconds()) > int(prev_off.total_seconds()):
                            dst_start = trans_current
                        else:
                            dst_end = trans_current
                        prev_off = trans_off
                        break
                    trans_current += timedelta(hours=1)
            
            # Update prev_off to track the current day's offset for next iteration
            prev_off = curr_off
            current += timedelta(days=1)
        
        return dst_start, dst_end
