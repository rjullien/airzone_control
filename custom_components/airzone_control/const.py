from __future__ import annotations

DOMAIN = "airzone_control"
DEFAULT_PORT = 3000
# No default host: cada usuario debe introducir la IP real del controlador Airzone.
DEFAULT_HOST = ""
DEFAULT_CLOUD_BASE_URL = "https://m.airzonecloud.com/api/v1"

# User options
CONF_HOST = "host"
CONF_PORT = "port"
CONF_SCAN_INTERVAL = "scan_interval"
CONF_GROUPS = "groups"  # Grupos/Zonas lógicas definidas por el usuario
CONF_CONNECTION_TYPE = "connection_type"
CONF_EMAIL = "email"
CONF_PASSWORD = "password"
CONF_USER_ID = "user_id"
CONF_CLOUD_PROFILE = "cloud_profile"
CONF_CLOUD_INCLUDE_CATEGORIES = "cloud_include_categories"
CONF_CLOUD_INCLUDE_DEVICE_IDS = "cloud_include_device_ids"
CONF_CLOUD_INCLUDE_BOUND_IAQS = "cloud_include_bound_iaqs"
CONF_CLOUD_EXCLUDE_IAQ_NAMES = "cloud_exclude_iaq_names"
CONF_CLOUD_CREATE_CLIMATES = "cloud_create_climates"
CONF_VERIFY_SSL = "verify_ssl"
CONF_REGISTER_INTEGRATION_DRIVER = "register_integration_driver"

CONNECTION_TYPE_LOCAL = "local"
CONNECTION_TYPE_CLOUD = "cloud"

CLOUD_PROFILE_FULL = "full"
CLOUD_PROFILE_COMPLEMENT_LOCAL = "complement_local"
CLOUD_PROFILE_CUSTOM = "custom"

CLOUD_CATEGORY_CLIMATE_ZONES = "climate_zones"
CLOUD_CATEGORY_IAQ = "iaq"
CLOUD_CATEGORY_ENERGY = "energy"
CLOUD_CATEGORY_ACS = "acs"
CLOUD_CATEGORY_AUX = "aux"

DEFAULT_CLOUD_INCLUDE_CATEGORIES = [
    CLOUD_CATEGORY_CLIMATE_ZONES,
    CLOUD_CATEGORY_ENERGY,
    CLOUD_CATEGORY_IAQ,
    CLOUD_CATEGORY_ACS,
    CLOUD_CATEGORY_AUX,
]

DEFAULT_CLOUD_INCLUDE_BOUND_IAQS = True
DEFAULT_CLOUD_INCLUDE_DEVICE_IDS: list[str] = []
DEFAULT_CLOUD_EXCLUDE_IAQ_NAMES = ""
# Cloud climates are read-only; allow skipping them when another integration
# (e.g. core airzone_cloud) already provides controllable thermostats.
DEFAULT_CLOUD_CREATE_CLIMATES = True
DEFAULT_CLOUD_PROFILE = CLOUD_PROFILE_FULL
DEFAULT_VERIFY_SSL = True
DEFAULT_REGISTER_INTEGRATION_DRIVER = False

CLOUD_CATEGORY_LABELS: dict[str, str] = {
    CLOUD_CATEGORY_ENERGY: "Energy",
    CLOUD_CATEGORY_IAQ: "IAQ",
    CLOUD_CATEGORY_CLIMATE_ZONES: "Thermostats / zones",
    CLOUD_CATEGORY_ACS: "DHW",
    CLOUD_CATEGORY_AUX: "Auxiliary",
}

CLOUD_PROFILE_LABELS: dict[str, str] = {
    CLOUD_PROFILE_FULL: "Use all Cloud API devices",
    CLOUD_PROFILE_COMPLEMENT_LOCAL: "Complement Local API",
    CLOUD_PROFILE_CUSTOM: "Custom",
}

# Intervalo de sondeo por defecto (segundos). Cambiable en Opciones.
DEFAULT_SCAN_INTERVAL = 5
DEFAULT_CLOUD_SCAN_INTERVAL = 60

# Cloud resilience: a missing reading is tolerated for this many consecutive
# polls (last valid data is kept) before entities become unavailable.
CLOUD_MAX_CONSECUTIVE_FAILURES = 3
# Installations/detail inventory barely changes: refresh it at most this often.
CLOUD_INVENTORY_TTL = 600
# Device status requests are sent one at a time to avoid Cloud bursts.
CLOUD_STATUS_CONCURRENCY = 1
# Retry-After fallback and upper bound (seconds) for HTTP 429 back-off.
CLOUD_DEFAULT_RETRY_AFTER = 60
CLOUD_MAX_BACKOFF = 300
# Minimum delay between two identical Cloud warnings in the log (seconds).
CLOUD_WARNING_INTERVAL = 900
# Per-request timeout (s) for Cloud status reads; timeouts are not retried.
CLOUD_REQUEST_TIMEOUT = 10
# Lower bound (s) for the whole-poll timeout (the scan interval is used if larger).
CLOUD_MIN_POLL_TIMEOUT = 30

# Códigos numéricos de la Local API -> etiquetas
# 0/1=Stop, 2=Cooling, 3=Heating, 4=Fan, 5=Dry, 7=Auto
MODE_LABELS: dict[int, str] = {
    0: "Stop",
    1: "Stop",
    2: "Cooling",
    3: "Heating",
    4: "Fan",
    5: "Dry",
    7: "Auto",
}

MODE_CODES: dict[str, int] = {v: k for k, v in MODE_LABELS.items()}
