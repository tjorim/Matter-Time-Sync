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
    """Validate that either device_id or node_id is provided."""
    if not data.get("device_id") and not data.get("node_id"):
        raise vol.Invalid("Either device_id or node_id must be provided")
    return data

SYNC_TIME_SCHEMA = vol.Schema(
    vol.All(
        {
            vol.Optional("device_id"): cv.string,
            vol.Optional("node_id"): cv.positive_int,
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
            # Matter devices have identifiers like ('matter', 'fabric_id-node_id')
            node_id = None
            for identifier in device.identifiers:
                if identifier[0] == "matter":
                    _LOGGER.debug("Found Matter identifier: %s", identifier)
                    # Format is typically ('matter', 'fabric_id-node_id')
                    if len(identifier) < 2:
                        _LOGGER.warning("Matter identifier has unexpected format: %s", identifier)
                        continue
                    
                    identifier_value = identifier[1]
                    parts = identifier_value.split("-")
                    
                    if len(parts) < 2:
                        _LOGGER.warning(
                            "Matter identifier value does not contain expected format 'fabric_id-node_id': %s",
                            identifier_value
                        )
                        continue
                    
                    try:
                        # Take the last part as node_id
                        node_id = int(parts[-1])
                        _LOGGER.debug("Successfully extracted node_id %s from identifier %s", node_id, identifier_value)
                        break
                    except ValueError:
                        _LOGGER.warning(
                            "Could not parse node_id from Matter identifier: %s",
                            identifier_value
                        )
                        continue
            
            if not node_id:
                _LOGGER.error("Could not extract Matter node_id from device %s", device_id)
                raise ValueError(f"Could not extract Matter node_id from device {device_id}")
            
            _LOGGER.info("Extracted node_id %s from device %s", node_id, device_id)
        
        # If we reach here and still don't have node_id, it's a validation error
        # (schema should have caught this, but double-check for safety)
        if not node_id:
            _LOGGER.error("No valid node_id could be determined")
            raise ValueError("Either device_id with valid Matter device or node_id must be provided")
        
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
        async with self.session.ws_connect(self.ws_address) as ws:
            # 1. Welcome
            welcome = await ws.receive_json()
            _LOGGER.debug("Server Welcome: %s", welcome)

            tz_info = await self.get_timezone_info(self.tz_name)

            # 2. Time Zone
            tz_obj = {"offset": tz_info["offset_seconds"], "validAt": 0, "name": self.tz_name}
            await self.send_command(ws, node_id, endpoint, CLUSTER_ID_TIME_SYNC, CMD_ID_SET_TIME_ZONE, "SetTimeZone", {"timeZone": [tz_obj]})

            # 3. DST Offset
            dst_obj = {
                "offset": tz_info["dst_adjustment_seconds"],
                "validStarting": self.to_matter_microseconds(tz_info["dst_start"]),
                "validUntil": self.to_matter_microseconds(tz_info["dst_end"]),
            }
            await self.send_command(ws, node_id, endpoint, CLUSTER_ID_TIME_SYNC, CMD_ID_SET_DST_OFFSET, "SetDSTOffset", {"DSTOffset": [dst_obj]})

            # 4. UTC Time
            now_utc = datetime.now(timezone.utc)
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
        await ws.send_json(message)
        response = await ws.receive_json()
        
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
        start = datetime(year, 1, 1, tzinfo=utc)
        end = datetime(year + 1, 1, 1, tzinfo=utc)
        dst_start = dst_end = None
        current = start
        prev_off = current.astimezone(tz).utcoffset()
        while current < end:
            curr_off = current.astimezone(tz).utcoffset()
            if curr_off != prev_off:
                if int(curr_off.total_seconds()) > int(prev_off.total_seconds()): dst_start = current
                else: dst_end = current
                prev_off = curr_off
            current += timedelta(hours=1)
        return dst_start, dst_end
