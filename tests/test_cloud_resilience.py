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

_real_sleep = asyncio.sleep
INST = "inst1"
ZONES = {"z1": 1, "z2": 2, "z3": 3}


class FakeResponse:
    def __init__(self, status, payload=None, headers=None, delay=0.0):
        self.delay = delay
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def json(self, content_type=None):
        return self._payload

    async def __aenter__(self):
        if self.delay:
            await _real_sleep(self.delay)
        return self

    async def __aexit__(self, *exc):
        return False


class FakeCloud:
    """Minimal Airzone Cloud: one installation, 3 zones + 1 IAQ sensor."""

    def __init__(self):
        self.calls = []
        self.fail = {}  # path -> list of status codes / exceptions to return first
        self.delay = {}  # path prefix -> seconds per request (default: all)
        self.default_delay = 0.0

    def request(self, method, url, **kwargs):
        resp = self._request(method, url, **kwargs)
        if isinstance(resp, FakeResponse):
            path = url.split("/api/v1", 1)[1]
            resp.delay = self.delay.get(path, self.default_delay)
        return resp

    def _request(self, method, url, **kwargs):
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

        coordinator._hard_poll_timeout = lambda: 0.05
        with patch.object(coordinator, "_async_fetch_cloud_data", hang):
            data = await coordinator._async_update_data()
        assert len(data) == 3 and coordinator._consecutive_failures == 1

    asyncio.run(scenario())  # real sleep needed for the timeout


def test_poll_budget_scales_with_request_count(coordinator):
    rt = const.CLOUD_REQUEST_TIMEOUT
    assert coordinator._poll_budget(1) == const.CLOUD_MIN_POLL_TIMEOUT
    assert coordinator._poll_budget(8) == 8 * rt + const.CLOUD_POLL_MARGIN
    assert coordinator._poll_budget(1000) == const.CLOUD_MAX_POLL_TIMEOUT
    assert coordinator._hard_poll_timeout() > const.CLOUD_MAX_POLL_TIMEOUT


def _tiny_budget(monkeypatch, request_timeout):
    """Scale the budget constants down so the test runs in milliseconds."""
    monkeypatch.setattr(cc, "CLOUD_REQUEST_TIMEOUT", request_timeout)
    monkeypatch.setattr(cc, "CLOUD_MIN_POLL_TIMEOUT", 0)
    monkeypatch.setattr(cc, "CLOUD_POLL_MARGIN", 0)
    monkeypatch.setattr(cc, "CLOUD_MAX_POLL_TIMEOUT", 10)


def test_slow_but_valid_requests_exceeding_scan_interval_are_kept(coordinator, monkeypatch):
    """Each request is within its own timeout, the sum exceeds the interval."""
    _tiny_budget(monkeypatch, request_timeout=0.05)

    async def scenario():
        await _poll(coordinator)  # learns the request count (2 inventory + 3 zones)
        coordinator._base_update_interval = timedelta(seconds=0.05)
        coordinator.update_interval = timedelta(seconds=0.05)
        coordinator._fake.default_delay = 0.03  # < 0.05 per request, ~0.15 s total
        coordinator._inventory_cache = None
        for _ in range(3):
            started = time.monotonic()
            data = await _poll(coordinator)
            assert time.monotonic() - started > 0.05  # longer than the interval
            assert len(data) == 3
            assert coordinator.cloud_stale_zones == set()
            assert coordinator._consecutive_failures == 0

    asyncio.run(scenario())


def test_budget_overrun_publishes_zones_already_read(coordinator, monkeypatch):
    _tiny_budget(monkeypatch, request_timeout=0.02)

    async def scenario():
        await _poll(coordinator)
        cloud = coordinator._fake
        # z2 answers late (budget spent while it is in flight) -> z3 not started
        cloud.delay["/devices/z2/status"] = 0.3
        before = cloud.count("/devices/z3/status")
        data = await _poll(coordinator)
        assert cloud.count("/devices/z3/status") == before  # never requested
        assert len(data) == 3
        assert coordinator.cloud_stale_zones == {"z3"}  # z1/z2 fresh, z3 cached
        assert coordinator._zone_misses["z1"] == 0 and coordinator._zone_misses["z2"] == 0
        assert coordinator._zone_misses["z3"] == 1

    asyncio.run(scenario())


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


class ManyInstallationsCloud(FakeCloud):
    """13 installations; the zones live in the last one."""

    N = 13

    def _request(self, method, url, **kwargs):
        path = url.split("/api/v1", 1)[1]
        if path == "/installations":
            self.calls.append(path)
            page = (kwargs.get("params") or {}).get("page", 0)
            ids = [f"i{n}" for n in range(self.N)][page * 10:(page + 1) * 10]
            return FakeResponse(200, {"installations": [{"installation_id": i, "ws_ids": []} for i in ids],
                                      "total": self.N})
        if path.startswith("/installations/i"):
            self.calls.append(path)
            iid = path.rsplit("/", 1)[1]
            devices = []
            if iid == f"i{self.N - 1}":
                devices = [
                    {"device_id": did, "type": "az_zone", "name": f"Zone {n}",
                     "meta": {"system_number": 1, "zone_number": n}}
                    for did, n in ZONES.items()
                ]
            return FakeResponse(200, {"installation_id": iid, "groups": [{"devices": devices}]})
        return super()._request(method, url, **kwargs)


def test_budget_is_resized_before_installation_details(coordinator, monkeypatch):
    _tiny_budget(monkeypatch, request_timeout=0.02)
    cloud = ManyInstallationsCloud()
    cloud.default_delay = 0.012  # each request well within its own timeout
    coordinator._session = cloud
    coordinator._fake = cloud
    coordinator._request_estimate = 2  # stale small estimate from a previous poll

    async def scenario():
        data = await _poll(coordinator)
        assert cloud.count("/installations/i") == ManyInstallationsCloud.N
        assert len(data) == 3  # zones of the last installation are present

    asyncio.run(scenario())
