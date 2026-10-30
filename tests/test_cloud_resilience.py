"""Resilience of the Cloud coordinator against transient Airzone Cloud errors.

Covers: per-zone stale data, consecutive-failure tolerance, 429 back-off
(Retry-After), single retry on 5xx / timeouts, inventory cache and the
option that disables the read-only Cloud climate entities.
"""
import asyncio
import importlib
import pathlib
import sys
import time
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
cc = importlib.import_module("custom_components.airzone_control.coordinator_cloud")
base_coord = importlib.import_module("custom_components.airzone_control.coordinator")
const = importlib.import_module("custom_components.airzone_control.const")
climate = importlib.import_module("custom_components.airzone_control.climate")

INST = "inst1"
ZONES = {"z1": 1, "z2": 2, "z3": 3}


class FakeResponse:
    def __init__(self, status, payload=None, headers=None):
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def json(self, content_type=None):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeCloud:
    """Minimal Airzone Cloud: one installation, 3 zones + 1 IAQ sensor."""

    def __init__(self):
        self.calls = []
        self.fail = {}  # path -> list of status codes / exceptions to return first

    def request(self, method, url, **kwargs):
        path = url.split("/api/v1", 1)[1]
        self.calls.append(path)
        queue = self.fail.get(path)
        if queue:
            item = queue.pop(0)
            if isinstance(item, Exception):
                raise item
            status, headers = item if isinstance(item, tuple) else (item, {})
            return FakeResponse(status, {"_id": "boom"}, headers)
        if path == "/installations":
            return FakeResponse(200, {"installations": [{"installation_id": INST, "ws_ids": []}], "total": 1})
        if path == f"/installations/{INST}":
            devices = [
                {"device_id": did, "type": "az_zone", "name": f"Zone {n}",
                 "meta": {"system_number": 1, "zone_number": n}}
                for did, n in ZONES.items()
            ]
            return FakeResponse(200, {"installation_id": INST, "groups": [{"devices": devices}]})
        if path.startswith("/devices/"):
            return FakeResponse(200, {"power": True, "mode": 3, "local_temp": {"celsius": 21.0},
                                      "setpoint_air_heat": {"celsius": 20.0}, "humidity": 50})
        return FakeResponse(404, {"_id": "not_found"})

    def count(self, prefix):
        return sum(1 for c in self.calls if c.startswith(prefix))


@pytest.fixture
def coordinator():
    hass = MagicMock()
    entry = MagicMock()
    entry.entry_id = "test"
    entry.options = {}
    cloud = FakeCloud()
    with patch.object(cc, "async_get_clientsession", return_value=cloud), \
         patch.object(base_coord, "async_get_clientsession", return_value=cloud), \
         patch.object(cc, "Store", MagicMock()), \
         patch("homeassistant.helpers.frame.report_usage"):
        coord = cc.AirzoneCloudCoordinator(hass, config_entry=entry, email="a@b.c", password="x", scan_interval=60)
    coord._token = "tok"
    coord._fake = cloud
    return coord


async def _fast_sleep(_delay):
    return None


def run(coro):
    with patch.object(cc.asyncio, "sleep", _fast_sleep):
        return asyncio.run(coro)


async def _poll(coord):
    data = await coord._async_update_data()
    coord.data = data
    return data


def test_default_cloud_interval_is_60s():
    assert const.DEFAULT_CLOUD_SCAN_INTERVAL == 60
    assert const.DEFAULT_CLOUD_CREATE_CLIMATES is True


def test_single_zone_failure_keeps_last_zone_until_threshold(coordinator):
    async def scenario():
        first = await _poll(coordinator)
        assert len(first) == 3
        z2 = "/devices/z2/status"
        # 2 failed polls (each with its single retry) -> stale zone kept
        for n in (1, 2):
            coordinator._fake.fail[z2] = [500, 500]
            data = await _poll(coordinator)
            assert len(data) == 3, n
            assert coordinator.cloud_stale_zones == {"z2"}
        # 3rd consecutive failure -> the zone is dropped, others stay available
        coordinator._fake.fail[z2] = [500, 500]
        data = await _poll(coordinator)
        assert len(data) == 2
        # recovery
        data = await _poll(coordinator)
        assert len(data) == 3 and coordinator.cloud_stale_zones == set()
    run(scenario())


def test_all_zones_failing_becomes_unavailable_only_after_threshold(coordinator):
    async def scenario():
        await _poll(coordinator)
        for n in range(2):
            for did in ZONES:
                coordinator._fake.fail[f"/devices/{did}/status"] = [500, 500]
            # all device status failed -> UpdateFailed internally -> stale data
            data = await _poll(coordinator)
            assert len(data) == 3
        for did in ZONES:
            coordinator._fake.fail[f"/devices/{did}/status"] = [500, 500]
        with pytest.raises(UpdateFailed):
            await _poll(coordinator)
    run(scenario())


def test_5xx_is_retried_once_but_timeout_is_not(coordinator):
    async def scenario():
        await _poll(coordinator)
        coordinator._fake.fail["/devices/z1/status"] = [503]
        coordinator._fake.fail["/devices/z2/status"] = [asyncio.TimeoutError()]
        before = coordinator._fake.count("/devices/")
        data = await _poll(coordinator)
        assert len(data) == 3
        # z1 recovered by its retry, z2 (timeout, no retry) served from cache
        assert coordinator.cloud_stale_zones == {"z2"}
        assert coordinator._fake.count("/devices/") - before == 4  # 3 + 1 retry
    run(scenario())


def test_alternating_failures_respect_zone_allowance(coordinator):
    """Whole-poll failures count against each zone's stale allowance."""
    async def scenario():
        await _poll(coordinator)
        cloud = coordinator._fake

        def fail_all_zones():
            for did in ZONES:
                cloud.fail[f"/devices/{did}/status"] = [500, 500]

        def fail_z1():
            cloud.fail["/devices/z1/status"] = [500, 500]

        # miss 1: whole poll fails (all device statuses) -> previous data
        fail_all_zones()
        assert len(await _poll(coordinator)) == 3
        # miss 2: only z1 fails, others OK -> z1 still served from cache
        fail_z1()
        data = await _poll(coordinator)
        assert len(data) == 3 and coordinator.cloud_stale_zones == {"z1"}
        # miss 3: whole poll fails again -> z1 must be gone from the stale data
        fail_all_zones()
        data = await _poll(coordinator)
        assert (1, 1) not in data and len(data) == 2

    run(scenario())


def test_all_cached_zones_is_not_a_recovery(coordinator):
    async def scenario():
        await _poll(coordinator)
        for did in ZONES:
            coordinator._fake.fail[f"/devices/{did}/status"] = [500, 500]
        await _poll(coordinator)  # whole-poll failure
        assert coordinator._consecutive_failures == 1
        # add an IAQ-like success is not modelled; zones failing individually
        # while the poll "succeeds" must not reset the failure counter
        coordinator._zone_misses = {did: 1 for did in ZONES}
        with patch.object(coordinator, "_async_fetch_cloud_data", return_value=coordinator.data):
            await coordinator._async_update_data()
        assert coordinator._consecutive_failures == 1
    run(scenario())


def test_poll_has_a_global_timeout(coordinator):
    async def scenario():
        await _poll(coordinator)
        never = asyncio.Event()

        async def hang():
            await never.wait()

        coordinator._poll_timeout = lambda: 0.05
        with patch.object(coordinator, "_async_fetch_cloud_data", hang):
            data = await coordinator._async_update_data()
        assert len(data) == 3 and coordinator._consecutive_failures == 1

    asyncio.run(scenario())  # real sleep needed for the timeout


def test_poll_timeout_follows_scan_interval(coordinator):
    assert coordinator._poll_timeout() == 60
    coordinator._base_update_interval = timedelta(seconds=15)
    assert coordinator._poll_timeout() == const.CLOUD_MIN_POLL_TIMEOUT


def test_429_starts_backoff_and_skips_requests(coordinator):
    async def scenario():
        await _poll(coordinator)
        base = coordinator.update_interval
        coordinator._fake.fail["/devices/z1/status"] = [(429, {"Retry-After": "120"})]
        before = len(coordinator._fake.calls)
        data = await _poll(coordinator)
        assert len(data) == 3  # z1 served from the stale cache
        # no further request once the 429 was received
        assert len(coordinator._fake.calls) - before == 1
        assert coordinator._backoff_active()
        assert coordinator.update_interval == min(base * 2, timedelta(seconds=const.CLOUD_MAX_BACKOFF))
        before = len(coordinator._fake.calls)
        assert await _poll(coordinator) is data
        assert len(coordinator._fake.calls) == before
        # back-off expires -> normal polling and base interval restored
        coordinator._backoff_until = time.monotonic() - 1
        data = await _poll(coordinator)
        assert len(data) == 3
        assert coordinator.update_interval == base
    run(scenario())


def test_retry_after_parsing():
    p = cc.AirzoneCloudCoordinator._parse_retry_after
    assert p("30") == 30
    assert p(None) == const.CLOUD_DEFAULT_RETRY_AFTER
    assert p("garbage") == const.CLOUD_DEFAULT_RETRY_AFTER
    assert p("100000") <= const.CLOUD_MAX_BACKOFF


def test_inventory_is_cached(coordinator):
    async def scenario():
        await _poll(coordinator)
        await _poll(coordinator)
        await _poll(coordinator)
        assert coordinator._fake.count("/installations") == 2  # list + detail, once
        # inventory failure falls back to cache once it is expired
        coordinator._inventory_cache = (*coordinator._inventory_cache[:2], 0.0)
        coordinator._fake.fail["/installations"] = [500, 500]
        data = await _poll(coordinator)
        assert len(data) == 3
    run(scenario())


def test_climate_option_disables_cloud_climates():
    hass = MagicMock()
    entry = MagicMock()
    entry.entry_id = "e1"
    entry.options = {const.CONF_CLOUD_CREATE_CLIMATES: False}
    coord = MagicMock(spec=climate.AirzoneCoordinator)
    coord.connection_type = const.CONNECTION_TYPE_CLOUD
    coord.data = {(1, 1): {"systemID": 1, "zoneID": 1}}
    hass.data = {const.DOMAIN: {"e1": {"coordinator": coord}}}
    add = MagicMock()
    with patch.object(climate, "_async_remove_climate_entities") as remove:
        asyncio.run(climate.async_setup_entry(hass, entry, add))
    remove.assert_called_once()
    add.assert_not_called()
