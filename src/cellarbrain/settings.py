"""Load and merge cellarbrain configuration from TOML files.

Provides frozen dataclasses for every configurable value in the pipeline,
a TOML loader with merge semantics, and convenience helpers for agent
section look-ups.

Precedence (highest → lowest):
    1. CLI arguments (``--data-dir``, ``--config``)
    2. Environment variables (``CELLARBRAIN_DATA_DIR``, ``CELLARBRAIN_CONFIG``)
    3. Config file (``cellarbrain.toml``)
    4. Built-in defaults (in this module)
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import tomllib
from dataclasses import dataclass, field
from dataclasses import fields as dataclass_fields

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sub-config dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PathsConfig:
    data_dir: str = "output"
    raw_dir: str = "raw"
    wines_subdir: str = "wines"
    cellar_subdir: str = "cellar"
    archive_subdir: str = "archive"
    wines_filename: str = "export-wines.csv"
    bottles_filename: str = "export-bottles-stored.csv"
    bottles_gone_filename: str = "export-bottles-gone.csv"


@dataclass(frozen=True)
class CsvConfig:
    encoding: str = "utf-16"
    delimiter: str = "\t"


@dataclass(frozen=True)
class PriceTier:
    label: str
    max: float | None = None


@dataclass(frozen=True)
class QueryConfig:
    row_limit: int = 200
    search_limit: int = 10
    pending_limit: int = 20


@dataclass(frozen=True)
class DisplayConfig:
    null_char: str = "\u2014"
    separator: str = "\u00b7"
    date_format: str = "%d.%m.%Y"
    tasting_date_format: str = "%d %B %Y"
    timestamp_format: str = "%Y-%m-%d %H:%M UTC"


@dataclass(frozen=True)
class DrinkingWindowConfig:
    too_young: str = "Too young"
    drinkable: str = "Drinkable, not yet optimal"
    optimal: str = "In optimal window"
    past_optimal: str = "Past optimal, still drinkable"
    past_window: str = "Past drinking window"
    unknown: str = "No drinking window data"


@dataclass(frozen=True)
class DossierConfig:
    filename_format: str = "{wine_id:04d}-{slug}.md"
    slug_max_length: int = 60
    max_full_name_length: int = 80
    output_encoding: str = "utf-8"


@dataclass(frozen=True)
class AgentSection:
    key: str
    heading: str
    tag: str
    mixed: bool = False


@dataclass(frozen=True)
class CurrencyConfig:
    default: str = "CHF"
    rates: dict[str, float] = field(
        default_factory=lambda: {
            "EUR": 0.93,
            "USD": 0.88,
            "GBP": 1.11,
            "AUD": 0.56,
            "CAD": 0.62,
            "RON": 0.18,
        }
    )


@dataclass(frozen=True)
class EtlConfig:
    default_mode: str = "full"
    etl_fence_start: str = "<!-- source: etl \u2014 do not edit below this line -->"
    etl_fence_end: str = "<!-- source: etl \u2014 end -->"


@dataclass(frozen=True)
class IdentityConfig:
    enable_fuzzy_match: bool = True
    rename_threshold: float = 0.85


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "WARNING"
    log_file: str | None = None
    max_bytes: int = 5_242_880
    backup_count: int = 3
    format: str = "%(asctime)s %(levelname)-8s %(name)s \u2014 %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    turn_gap_seconds: float = 2.0
    slow_threshold_ms: float = 2000.0
    log_db: str | None = None
    retention_days: int = 90
    ingest_retention_days: int = 90
    ingest_poll_retention_days: int = 7


@dataclass(frozen=True)
class WishlistConfig:
    sections: tuple[str, ...] = (
        "producer_deep_dive",
        "vintage_tracker",
        "buying_guide",
        "price_tracker",
    )
    scan_cadence_days: int = 7
    alert_window_days: int = 30
    price_drop_alert_pct: float = 10.0
    wishlist_subdir: str = "tracked"
    retailers: dict[str, str] = field(
        default_factory=lambda: {
            "gerstl": "gerstl.ch",
            "martel": "martel.ch",
            "flaschenpost": "flaschenpost.ch",
            "moevenpick": "moevenpick-wein.com",
            "weinauktion": "weinauktion.ch",
            "wine_ch": "wine.ch",
            "juan_sanchez": "juan-sanchez.ch",
            "globalwine": "globalwine.ch",
            "divo": "divo.ch",
            "schuler": "schuler.ch",
        }
    )
    bottle_sizes: dict[str, int] = field(
        default_factory=lambda: {
            "half": 375,
            "standard": 750,
            "magnum": 1500,
            "double_magnum": 3000,
            "jeroboam": 5000,
            "imperial": 6000,
        }
    )


@dataclass(frozen=True)
class SearchConfig:
    synonyms: dict[str, str] = field(
        default_factory=lambda: _default_search_synonyms(),
    )


@dataclass(frozen=True)
class SommelierConfig:
    enabled: bool = False
    model_dir: str = "models/sommelier/model"
    base_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    food_catalogue: str = "models/sommelier/food_catalogue.parquet"
    pairing_dataset: str = "models/sommelier/pairing_dataset.parquet"
    food_index: str = "models/sommelier/food.index"
    food_ids: str = "models/sommelier/food_ids.json"
    wine_index_dir: str = "sommelier"
    default_limit: int = 10
    min_score: float = 0.0
    training_epochs: int = 10
    training_batch_size: int = 32
    warmup_ratio: float = 0.1
    eval_split: float = 0.1
    auto_retrain_threshold: int = 100
    auto_food_tags: bool = True


@dataclass(frozen=True)
class CellarRule:
    """A single cellar classification rule.

    Matches cellar names using ``fnmatch.fnmatchcase`` glob patterns.
    Exact names work as patterns too (they match only themselves).
    """

    pattern: str
    classification: str  # "onsite" | "offsite" | "in_transit"


CELLAR_CLASSIFICATIONS = frozenset({"onsite", "offsite", "in_transit"})


@dataclass(frozen=True)
class BackupConfig:
    """Backup and restore configuration."""

    backup_dir: str = "bkp"
    max_backups: int = 5
    include_sommelier: bool = False
    include_logs: bool = False


@dataclass(frozen=True)
class OutputConfig:
    """Output formatting configuration."""

    default_format: str = "markdown"


@dataclass(frozen=True)
class DashboardConfig:
    """Dashboard configuration."""

    port: int = 8017
    workbench_read_only: bool = True
    workbench_allow: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class IngestConfig:
    """IMAP email ingestion daemon configuration."""

    imap_host: str = "imap.mail.me.com"
    imap_port: int = 993
    use_ssl: bool = True
    mailbox: str = "INBOX"
    subject_filter: str = "[VinoCell] CSV file"
    sender_filter: str = ""
    sender_whitelist: tuple[str, ...] = ()
    poll_interval: int = 60
    batch_window: int = 300
    expected_files: tuple[str, ...] = (
        "export-wines.csv",
        "export-bottles-stored.csv",
        "export-bottles-gone.csv",
    )
    processed_action: str = "flag"
    processed_folder: str = "VinoCell/Processed"
    processed_color: str = "orange"
    etl_timeout: int = 300
    max_etl_retries: int = 3
    max_backoff_interval: int = 600
    max_attachment_bytes: int = 10_485_760
    heartbeat_interval: int = 10
    imap_timeout: int = 60
    reaper_enabled: bool = True
    stale_threshold: int = 0
    dedup_strategy: str = "latest"
    dead_letter_folder: str = ""
    max_uptime: int = 86400


# ---------------------------------------------------------------------------
# Default builders (called once per Settings construction)
# ---------------------------------------------------------------------------


def _default_classification_short() -> dict[str, str]:
    return {
        # France
        "1er Cru": "1er Cru",
        "1er Cru Sup\u00e9rieur": "1er Cru Sup\u00e9rieur",
        "1er Grand Cru Class\u00e9": "1er Grand Cru Class\u00e9",
        "1er Grand Cru Class\u00e9 A": "1er Grand Cru Class\u00e9 A",
        "2\u00e8me Grand Cru Class\u00e9": "2\u00e8me Grand Cru Class\u00e9",
        "3\u00e8me Grand Cru Class\u00e9": "3\u00e8me Grand Cru Class\u00e9",
        "4\u00e8me Grand Cru Class\u00e9": "4\u00e8me Grand Cru Class\u00e9",
        "5\u00e8me Grand Cru Class\u00e9": "5\u00e8me Grand Cru Class\u00e9",
        "Grand Cru Class\u00e9": "Grand Cru Class\u00e9",
        "Grand Cru": "Grand Cru",
        "Cru Bourgeois": "Cru Bourgeois",
        "Cru Bourgeois Sup\u00e9rieur": "Cru Bourgeois Sup\u00e9rieur",
        "Cru Bourgeois Exceptionnel": "Cru Bourgeois Exceptionnel",
        "Cru Artisan": "Cru Artisan",
        "Cru Class\u00e9": "Cru Class\u00e9",
        # Italy
        "DOP / DOC Riserva": "Riserva",
        "DOP / DOC Superiore": "Superiore",
        "DOP / DOC Superiore Riserva": "Superiore Riserva",
        "DOCG Riserva": "Riserva",
        "DOCG Superiore": "Superiore",
        "DOCG Superiore Riserva": "Superiore Riserva",
        "DOCG Dolce Naturale": "Dolce Naturale",
        "Gran Selezione": "Gran Selezione",
        # Spain
        "DOCa Crianza": "Crianza",
        "DOCa Reserva": "Reserva",
        "DOCa Gran Reserva": "Gran Reserva",
        "DOCa Joven": "Joven",
        "DO Crianza": "Crianza",
        "DO Reserva": "Reserva",
        "DO Gran Reserva": "Gran Reserva",
        "DO Joven": "Joven",
        "DOP / DO de Pago": "Pago",
        "DOP / DO de Pago Calificado": "Pago Calificado",
        "Vino de Pago": "Pago",
        "VOS": "VOS",
        "VORS": "VORS",
        # Germany — Pr\u00e4dikat levels
        "Kabinett": "Kabinett",
        "Sp\u00e4tlese": "Sp\u00e4tlese",
        "Auslese": "Auslese",
        "Beerenauslese": "Beerenauslese",
        "Eiswein": "Eiswein",
        "Trockenbeerenauslese": "Trockenbeerenauslese",
        "Erstes Gew\u00e4chs": "Erstes Gew\u00e4chs",
        "Einzellage": "Einzellage",
        # Germany — VDP
        "VDP.Gro\u00dfe Lage": "Gro\u00dfe Lage",
        "VDP.Erste Lage": "Erste Lage",
        "VDP.Gro\u00dfes Gew\u00e4chs": "Gro\u00dfes Gew\u00e4chs",
        "VDP.Gutswein": "Gutswein",
        "VDP.Ortswein": "Ortswein",
        "VDP.Aus Ersten Lagen": "Aus Ersten Lagen",
        "VDP.Sekt": "Sekt",
        "VDP.Sekt Prestige": "Sekt Prestige",
        # Austria — Wachau
        "Steinfeder": "Steinfeder",
        "Federspiel": "Federspiel",
        "Smaragd": "Smaragd",
        # Austria — DAC & vineyard classifications
        "DAC - Reserve": "DAC Reserve",
        "Gro\u00dfe STK Lage": "Gro\u00dfe STK Lage",
        "Erste STK Lage": "Erste STK Lage",
        "\u00d6TW Erste Lage": "\u00d6TW Erste Lage",
        "Ortswein": "Ortswein",
        "Riedenwein": "Riedenwein",
        # Austria — sweet wine & sparkling
        "Ausbruch": "Ausbruch",
        "Strohwein / Schilfwein": "Strohwein",
        "Sekt - Klassik": "Sekt Klassik",
        "Sekt - Reserve": "Sekt Reserve",
        "Sekt - Gro\u00dfe Reserve": "Sekt Gro\u00dfe Reserve",
        # Switzerland
        "Premier Cru": "Premier Cru",
        "Premier Grand Cru": "Premier Grand Cru",
        # New World (Argentina, Chile)
        "Reserva": "Reserva",
        "Gran Reserva": "Gran Reserva",
        "Reserva Especial": "Reserva Especial",
        "Reserva Privada": "Reserva Privada",
    }


def _default_agent_sections() -> tuple[AgentSection, ...]:
    return (
        AgentSection("producer_profile", "Producer Profile", "agent:research"),
        AgentSection("vintage_report", "Vintage Report", "agent:research"),
        AgentSection("wine_description", "Wine Description", "agent:research"),
        AgentSection("market_availability", "Market & Availability", "agent:research"),
        AgentSection("similar_wines", "Similar Wines", "agent:recommendation"),
        AgentSection("agent_log", "Agent Log", "agent"),
        AgentSection("ratings_reviews", "From Research", "agent:research", mixed=True),
        AgentSection("tasting_notes", "Community Tasting Notes", "agent:research", mixed=True),
        AgentSection("food_pairings", "Recommended Pairings", "agent:research", mixed=True),
    )


def _default_companion_sections() -> tuple[AgentSection, ...]:
    return (
        AgentSection("producer_deep_dive", "Producer Deep Dive", "agent:research"),
        AgentSection("vintage_tracker", "Vintage Tracker", "agent:research"),
        AgentSection("buying_guide", "Buying Guide", "agent:research"),
        AgentSection("price_tracker", "Price Tracker", "agent:price"),
    )


def _default_price_tiers() -> tuple[PriceTier, ...]:
    return (
        PriceTier("budget", 15),
        PriceTier("everyday", 27),
        PriceTier("premium", 40),
        PriceTier("fine", None),
    )


def _default_search_synonyms() -> dict[str, str]:
    """Built-in search synonym dict for query token normalisation.

    Keys are lowercase query tokens; values are replacement text.
    Multi-word values (e.g. "Pinot Noir") are re-tokenised during search.
    Empty string values act as stopwords — the token is dropped.
    """
    return {
        # -- Countries: German → English -----------------------------------
        "schweiz": "Switzerland",
        "frankreich": "France",
        "italien": "Italy",
        "spanien": "Spain",
        "deutschland": "Germany",
        "österreich": "Austria",
        "portugal": "Portugal",
        "argentinien": "Argentina",
        "chile": "Chile",
        "südafrika": "South Africa",
        "neuseeland": "New Zealand",
        "australien": "Australia",
        "armenien": "Armenia",
        "kanada": "Canada",
        "kroatien": "Croatia",
        "usa": "United States",
        "amerika": "United States",
        # -- Country adjective forms → country name ------------------------
        "french": "France",
        "italian": "Italy",
        "spanish": "Spain",
        "german": "Germany",
        "austrian": "Austria",
        "portuguese": "Portugal",
        "swiss": "Switzerland",
        "argentinian": "Argentina",
        "australian": "Australia",
        "american": "United States",
        "chilean": "Chile",
        "croatian": "Croatia",
        "italienisch": "Italy",
        "französisch": "France",
        "spanisch": "Spain",
        "deutsch": "Germany",
        "österreichisch": "Austria",
        "portugiesisch": "Portugal",
        "schweizerisch": "Switzerland",
        "chilenisch": "Chile",
        "argentinisch": "Argentina",
        "südafrikanisch": "South Africa",
        "kroatisch": "Croatia",
        "armenisch": "Armenia",
        "kanadisch": "Canada",
        "amerikanisch": "United States",
        "neuseeländisch": "New Zealand",
        # -- Categories: German → English ----------------------------------
        "rotwein": "red",
        "weisswein": "white",
        "weißwein": "white",
        "roséwein": "rosé",
        "rot": "red",
        "weiss": "white",
        "weiß": "white",
        # -- Wine styles: German → concept keyword -------------------------
        "schaumwein": "sparkling",
        "sekt": "sparkling",
        "perlwein": "sparkling",
        "süsswein": "dessert",
        "süßwein": "dessert",
        "dessertwein": "dessert",
        "likörwein": "fortified",
        "süss": "sweet",
        "süß": "sweet",
        "portwein": "port",
        "orangewein": "orange_wine",
        "champagner": "Champagne",
        "trocken": "dry",
        "lieblich": "sweet",
        "edelsüss": "sweet",
        "edelsüß": "sweet",
        "eiswein": "ice_wine",
        "naturwein": "natural",
        # -- Regions: German → stored name ---------------------------------
        "wallis": "Valais",
        "waadt": "Vaud",
        "tessin": "Ticino",
        "genf": "Geneva",
        "neuenburg": "Neuchâtel",
        "graubünden": "Graubünden",
        "burgund": "Bourgogne",
        "bordeaux": "Bordeaux",
        "toskana": "Toscana",
        "piemont": "Piemonte",
        "elsass": "Alsace",
        "sizilien": "Sicilia",
        "sardinien": "Sardegna",
        "apulien": "Puglia",
        "venetien": "Veneto",
        "abruzzen": "Abruzzo",
        "friaul": "Friuli",
        "südtirol": "Alto Adige",
        "kalifornien": "California",
        "loiretal": "Vallée de la Loire",
        "rhonetal": "Vallée du Rhône",
        "rioja": "Rioja",
        "navarra": "Navarra",
        # -- Grape synonyms: German → international name -------------------
        "spätburgunder": "Pinot Noir",
        "blauburgunder": "Pinot Noir",
        "grauburgunder": "Pinot Gris",
        "weissburgunder": "Pinot Blanc",
        "weißburgunder": "Pinot Blanc",
        "riesling": "Riesling",
        "silvaner": "Silvaner",
        "trollinger": "Schiava",
        "lemberger": "Blaufränkisch",
        "schwarzriesling": "Pinot Meunier",
        # -- Grape shortcuts: common abbreviations -------------------------
        "cab": "Cabernet",
        "chard": "Chardonnay",
        "sauv": "Sauvignon",
        "zin": "Zinfandel",
        # -- Grape cross-references: alternate names in data ---------------
        "gutedel": "Chasselas",
        "vernatsch": "Schiava",
        "rotburger": "Zweigelt",
        "rivaner": "Müller-Thurgau",
        # -- Intent triggers: German → English intent phrases ----------------
        "trinkreif": "ready to drink",
        "trinkbereit": "ready to drink",
        "günstig": "budget",
        "billig": "budget",
        "preiswert": "budget",
        "teuer": "premium",
        "hochwertig": "premium",
        "exklusiv": "premium",
        "expensive": "premium",
        # -- Stopwords (value="") — dropped from query ---------------------
        "weingut": "",
        "domaine": "",
        "château": "",  # e.g. "Château Mouton" → just "Mouton"
        "bodega": "",
        "cantina": "",
        "tenuta": "",
        "jahrgang": "",
        "wein": "",
        "wine": "",
        "vin": "",
        "vino": "",
        "zum": "",
        "für": "",
        "vom": "",
        "aus": "",
        "der": "",
        "die": "",
        "das": "",
        "ein": "",
        "eine": "",
        "the": "",
        "for": "",
        "from": "",
        # -- Command stopwords: query noise in DE/EN -----------------------
        "suche": "",
        "zeige": "",
        "finde": "",
        "bitte": "",
        "mir": "",
        "ich": "",
        "möchte": "",
        "gib": "",
        "welche": "",
        "welcher": "",
        "welches": "",
        "empfehlung": "",
        "empfehle": "",
    }


# ---------------------------------------------------------------------------
# Top-level Settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    paths: PathsConfig = field(default_factory=PathsConfig)
    csv: CsvConfig = field(default_factory=CsvConfig)
    price_tiers: tuple[PriceTier, ...] = field(default_factory=_default_price_tiers)
    query: QueryConfig = field(default_factory=QueryConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)
    drinking_window: DrinkingWindowConfig = field(
        default_factory=DrinkingWindowConfig,
    )
    dossier: DossierConfig = field(default_factory=DossierConfig)
    agent_sections: tuple[AgentSection, ...] = field(
        default_factory=_default_agent_sections,
    )
    classification_short: dict[str, str] = field(
        default_factory=_default_classification_short,
    )
    offsite_cellars: tuple[str, ...] = ()
    in_transit_cellars: tuple[str, ...] = ()
    cellar_rules: tuple[CellarRule, ...] = ()
    currency: CurrencyConfig = field(default_factory=CurrencyConfig)
    etl: EtlConfig = field(default_factory=EtlConfig)
    identity: IdentityConfig = field(default_factory=IdentityConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    wishlist: WishlistConfig = field(default_factory=WishlistConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    sommelier: SommelierConfig = field(default_factory=SommelierConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    ingest: IngestConfig = field(default_factory=IngestConfig)
    backup: BackupConfig = field(default_factory=BackupConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    companion_sections: tuple[AgentSection, ...] = field(
        default_factory=_default_companion_sections,
    )
    config_source: str | None = None

    # -- convenience helpers ------------------------------------------------

    def agent_section_keys(self) -> frozenset[str]:
        """All agent section keys (equivalent to ``ALLOWED_SECTIONS``)."""
        return frozenset(s.key for s in self.agent_sections)

    def pure_agent_sections(self) -> tuple[AgentSection, ...]:
        """Agent sections that are fully agent-owned (``mixed=False``)."""
        return tuple(s for s in self.agent_sections if not s.mixed)

    def mixed_agent_sections(self) -> tuple[AgentSection, ...]:
        """Agent sections that contain both ETL and agent content."""
        return tuple(s for s in self.agent_sections if s.mixed)

    def agent_section_by_key(self, key: str) -> AgentSection:
        """Look up an agent section by its key.

        Raises ``KeyError`` if the key is not found.
        """
        for s in self.agent_sections:
            if s.key == key:
                return s
        raise KeyError(key)

    def heading_to_key(self) -> dict[str, str]:
        """Map heading → key for all agent sections (pure and mixed)."""
        return {s.heading: s.key for s in self.agent_sections}

    def companion_section_keys(self) -> frozenset[str]:
        """All companion dossier section keys."""
        return frozenset(s.key for s in self.companion_sections)

    def companion_section_by_key(self, key: str) -> AgentSection:
        """Look up a companion section by its key."""
        for s in self.companion_sections:
            if s.key == key:
                return s
        raise KeyError(key)


# ---------------------------------------------------------------------------
# TOML loading
# ---------------------------------------------------------------------------


def _validate_keys(
    section_name: str,
    raw: dict,
    dataclass_type: type,
) -> None:
    """Raise ValueError if *raw* dict has keys not in the dataclass."""
    valid = {f.name for f in dataclass_fields(dataclass_type)}
    unknown = set(raw) - valid
    if unknown:
        raise ValueError(
            f"Unknown key(s) in [{section_name}] config: "
            f"{', '.join(sorted(unknown))}. "
            f"Valid keys: {', '.join(sorted(valid))}"
        )


def _anchor(path_str: str, root: pathlib.Path) -> str:
    """Resolve *path_str* against *root* if it is relative; absolute paths pass through."""
    p = pathlib.Path(path_str)
    if p.is_absolute():
        return path_str
    return str((root / p).resolve())


def _resolve_relative_paths(paths: PathsConfig, config_root: pathlib.Path) -> PathsConfig:
    """Return a new PathsConfig with data_dir and raw_dir anchored to *config_root*."""
    return PathsConfig(
        data_dir=_anchor(paths.data_dir, config_root),
        raw_dir=_anchor(paths.raw_dir, config_root),
        wines_subdir=paths.wines_subdir,
        cellar_subdir=paths.cellar_subdir,
        archive_subdir=paths.archive_subdir,
        wines_filename=paths.wines_filename,
        bottles_filename=paths.bottles_filename,
        bottles_gone_filename=paths.bottles_gone_filename,
    )


def _resolve_backup_paths(cfg: BackupConfig, config_root: pathlib.Path) -> BackupConfig:
    """Return a new BackupConfig with backup_dir anchored to *config_root*."""
    return BackupConfig(
        backup_dir=_anchor(cfg.backup_dir, config_root),
        max_backups=cfg.max_backups,
        include_sommelier=cfg.include_sommelier,
        include_logs=cfg.include_logs,
    )


def _resolve_config_path(
    config_path: str | pathlib.Path | None,
) -> pathlib.Path | None:
    """Find the config file using the precedence chain."""
    if config_path is not None:
        p = pathlib.Path(config_path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")
        return p

    env = os.environ.get("CELLARBRAIN_CONFIG")
    if env:
        p = pathlib.Path(env)
        if not p.exists():
            raise FileNotFoundError(
                f"CELLARBRAIN_CONFIG points to missing file: {p}",
            )
        return p

    # Prefer local (gitignored) override over the committed template
    local = pathlib.Path("cellarbrain.local.toml")
    if local.exists():
        return local

    default = pathlib.Path("cellarbrain.toml")
    if default.exists():
        return default

    logger.warning(
        "No cellarbrain.toml found in CWD (%s) — using built-in defaults. "
        "Set CELLARBRAIN_CONFIG or use -c to specify a config file.",
        pathlib.Path.cwd(),
    )
    return None


def _anchor(path_str: str, base: pathlib.Path) -> str:
    """Resolve *path_str* against *base* unless it is already absolute."""
    p = pathlib.Path(path_str)
    if p.is_absolute():
        return path_str
    return str(base / p)


def _resolve_sommelier_paths(
    cfg: SommelierConfig,
    data_dir: pathlib.Path,
) -> SommelierConfig:
    """Anchor mutable sommelier paths to *data_dir*.

    Paths that are already absolute pass through unchanged.
    ``base_model`` is left unanchored (HuggingFace model ID).
    ``food_catalogue`` is resolved to the bundled package-data path when
    the configured value is a relative path.
    """
    from .sommelier.seed import bundled_food_catalogue

    food_cat = cfg.food_catalogue
    if not pathlib.Path(food_cat).is_absolute():
        food_cat = str(bundled_food_catalogue())

    return SommelierConfig(
        enabled=cfg.enabled,
        model_dir=_anchor(cfg.model_dir, data_dir),
        base_model=cfg.base_model,
        food_catalogue=food_cat,
        pairing_dataset=_anchor(cfg.pairing_dataset, data_dir),
        food_index=_anchor(cfg.food_index, data_dir),
        food_ids=_anchor(cfg.food_ids, data_dir),
        wine_index_dir=cfg.wine_index_dir,
        default_limit=cfg.default_limit,
        min_score=cfg.min_score,
        training_epochs=cfg.training_epochs,
        training_batch_size=cfg.training_batch_size,
        warmup_ratio=cfg.warmup_ratio,
        eval_split=cfg.eval_split,
        auto_retrain_threshold=cfg.auto_retrain_threshold,
        auto_food_tags=cfg.auto_food_tags,
    )


def _parse_price_tiers(raw: list[dict]) -> tuple[PriceTier, ...]:
    tiers: list[PriceTier] = []
    for entry in raw:
        label = entry.get("label")
        if not label:
            raise ValueError("Each price_tiers entry must have a 'label'")
        tiers.append(PriceTier(label=label, max=entry.get("max")))
    return tuple(tiers)


def _parse_agent_sections(raw: list[dict]) -> tuple[AgentSection, ...]:
    sections: list[AgentSection] = []
    for entry in raw:
        key = entry.get("key")
        heading = entry.get("heading")
        tag = entry.get("tag")
        if not key or not heading or not tag:
            raise ValueError(
                "Each agent_sections entry must have 'key', 'heading', and 'tag'",
            )
        sections.append(
            AgentSection(
                key=key,
                heading=heading,
                tag=tag,
                mixed=entry.get("mixed", False),
            ),
        )
    return tuple(sections)


def _parse_cellar_rules(raw: list[dict]) -> tuple[CellarRule, ...]:
    """Parse ``[[cellar_rules]]`` TOML entries into ``CellarRule`` tuples."""
    rules: list[CellarRule] = []
    for i, entry in enumerate(raw):
        pattern = entry.get("pattern")
        classification = entry.get("classification")
        if not pattern or not classification:
            raise ValueError(
                f"cellar_rules[{i}]: each entry must have 'pattern' and 'classification'",
            )
        rules.append(CellarRule(pattern=pattern, classification=classification))
    return tuple(rules)


def _legacy_to_rules(
    offsite: tuple[str, ...],
    in_transit: tuple[str, ...],
) -> tuple[CellarRule, ...]:
    """Convert legacy flat cellar lists to rules (exact names are valid glob patterns)."""
    rules: list[CellarRule] = []
    for name in offsite:
        rules.append(CellarRule(pattern=name, classification="offsite"))
    for name in in_transit:
        rules.append(CellarRule(pattern=name, classification="in_transit"))
    return tuple(rules)


def _validate_cellar_rules(rules: tuple[CellarRule, ...]) -> None:
    """Validate cellar rules at load time."""
    import fnmatch as _fnmatch

    for i, rule in enumerate(rules):
        if rule.classification not in CELLAR_CLASSIFICATIONS:
            raise ValueError(
                f"cellar_rules[{i}]: invalid classification "
                f"'{rule.classification}' — must be one of "
                f"{', '.join(sorted(CELLAR_CLASSIFICATIONS))}",
            )
        try:
            _fnmatch.translate(rule.pattern)
        except Exception as exc:
            raise ValueError(
                f"cellar_rules[{i}]: invalid glob pattern '{rule.pattern}': {exc}",
            ) from exc


_CURRENCY_SIDECAR_FILE = "currency-rates.json"


def _load_currency_sidecar(data_dir: str) -> dict[str, float]:
    """Load agent-managed currency rates from the sidecar JSON file."""
    path = pathlib.Path(data_dir) / _CURRENCY_SIDECAR_FILE
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_settings(
    config_path: str | pathlib.Path | None = None,
) -> Settings:
    """Load settings with precedence: CLI path > env var > ./cellarbrain.toml > defaults.

    Merge strategy:
    - Scalars: TOML value overrides the default.
    - Tables (``classification_short``): merged — TOML entries override/add
      to the defaults; keys absent from TOML keep their defaults.
    - Arrays (``price_tiers``, ``agent_sections``, ``offsite_cellars``):
      replaced entirely when present in TOML.
    """
    resolved = _resolve_config_path(config_path)

    raw: dict = {}
    if resolved is not None:
        with open(resolved, "rb") as f:
            try:
                raw = tomllib.load(f)
            except tomllib.TOMLDecodeError as exc:
                raise ValueError(f"Invalid TOML in {resolved}: {exc}") from exc

    # -- Build sub-configs from TOML sections, falling back to defaults -----

    paths_raw = raw.get("paths", {})
    csv_raw = raw.get("csv", {})
    query_raw = raw.get("query", {})
    display_raw = raw.get("display", {})
    dw_raw = raw.get("drinking_window", {})
    dossier_raw = raw.get("dossier", {})
    etl_raw = raw.get("etl", {})
    identity_raw = raw.get("identity", {})
    logging_raw = raw.get("logging", {})
    wishlist_raw = raw.get("wishlist", {})
    search_raw = raw.get("search", {})

    if paths_raw:
        _validate_keys("paths", paths_raw, PathsConfig)
    if csv_raw:
        _validate_keys("csv", csv_raw, CsvConfig)
    if query_raw:
        _validate_keys("query", query_raw, QueryConfig)
    if display_raw:
        _validate_keys("display", display_raw, DisplayConfig)
    if dw_raw:
        _validate_keys("drinking_window", dw_raw, DrinkingWindowConfig)
    if dossier_raw:
        _validate_keys("dossier", dossier_raw, DossierConfig)
    if etl_raw:
        _validate_keys("etl", etl_raw, EtlConfig)
    if identity_raw:
        _validate_keys("identity", identity_raw, IdentityConfig)
    if logging_raw:
        _validate_keys("logging", logging_raw, LoggingConfig)
    if wishlist_raw:
        _validate_keys("wishlist", wishlist_raw, WishlistConfig)
    # search — synonyms sub-table handled separately (like currency.rates)
    search_scalars = {k: v for k, v in search_raw.items() if k != "synonyms"}
    if search_scalars:
        _validate_keys("search", search_scalars, SearchConfig)

    paths = PathsConfig(**paths_raw) if paths_raw else PathsConfig()
    csv_cfg = CsvConfig(**csv_raw) if csv_raw else CsvConfig()
    query = QueryConfig(**query_raw) if query_raw else QueryConfig()
    display = DisplayConfig(**display_raw) if display_raw else DisplayConfig()
    drinking_window = DrinkingWindowConfig(**dw_raw) if dw_raw else DrinkingWindowConfig()
    dossier = DossierConfig(**dossier_raw) if dossier_raw else DossierConfig()
    etl = EtlConfig(**etl_raw) if etl_raw else EtlConfig()
    identity = IdentityConfig(**identity_raw) if identity_raw else IdentityConfig()
    logging_cfg = LoggingConfig(**logging_raw) if logging_raw else LoggingConfig()
    if wishlist_raw:
        wl_kw: dict = dict(wishlist_raw)
        if "sections" in wl_kw:
            wl_kw["sections"] = tuple(wl_kw["sections"])
        wishlist = WishlistConfig(**wl_kw)
    else:
        wishlist = WishlistConfig()

    # Arrays — replace entirely if present
    if "price_tiers" in raw:
        price_tiers = _parse_price_tiers(raw["price_tiers"])
    else:
        price_tiers = _default_price_tiers()

    if "agent_sections" in raw:
        agent_sections = _parse_agent_sections(raw["agent_sections"])
    else:
        agent_sections = _default_agent_sections()

    if "companion_sections" in raw:
        companion_sections = _parse_agent_sections(raw["companion_sections"])
    else:
        companion_sections = _default_companion_sections()

    if "offsite_cellars" in raw:
        offsite_cellars = tuple(raw["offsite_cellars"])
    else:
        offsite_cellars = ()

    if "in_transit_cellars" in raw:
        in_transit_cellars = tuple(raw["in_transit_cellars"])
    else:
        in_transit_cellars = ()

    # Cellar rules — new rule-based system with legacy fallback
    has_rules = "cellar_rules" in raw
    has_legacy = "offsite_cellars" in raw or "in_transit_cellars" in raw
    if has_rules and has_legacy:
        raise ValueError(
            "Config error: 'cellar_rules' and legacy 'offsite_cellars' / "
            "'in_transit_cellars' cannot both be present. "
            "Use cellar_rules or the legacy flat lists, not both."
        )
    if has_rules:
        cellar_rules = _parse_cellar_rules(raw["cellar_rules"])
        _validate_cellar_rules(cellar_rules)
    elif has_legacy:
        cellar_rules = _legacy_to_rules(offsite_cellars, in_transit_cellars)
    else:
        cellar_rules = ()

    # Tables — merge
    classification_short = _default_classification_short()
    if "classification_short" in raw:
        classification_short.update(raw["classification_short"])

    # Search synonyms — table merge
    search_synonyms = _default_search_synonyms()
    if "synonyms" in search_raw:
        search_synonyms.update(search_raw["synonyms"])
    search = SearchConfig(synonyms=search_synonyms)

    # Sommelier — simple scalar config
    sommelier_raw = raw.get("sommelier", {})
    if sommelier_raw:
        _validate_keys("sommelier", sommelier_raw, SommelierConfig)
    sommelier = SommelierConfig(**sommelier_raw) if sommelier_raw else SommelierConfig()

    # Dashboard — simple scalar config
    dashboard_raw = raw.get("dashboard", {})
    if dashboard_raw:
        _validate_keys("dashboard", dashboard_raw, DashboardConfig)
    dashboard = DashboardConfig(**dashboard_raw) if dashboard_raw else DashboardConfig()

    # Ingest — scalar config with tuple conversion for expected_files/sender_whitelist
    ingest_raw = raw.get("ingest", {})
    if ingest_raw:
        ingest_kw: dict = dict(ingest_raw)
        if "expected_files" in ingest_kw:
            ingest_kw["expected_files"] = tuple(ingest_kw["expected_files"])
        if "sender_whitelist" in ingest_kw:
            ingest_kw["sender_whitelist"] = tuple(ingest_kw["sender_whitelist"])
        _validate_keys("ingest", ingest_kw, IngestConfig)
        ingest = IngestConfig(**ingest_kw)
    else:
        ingest = IngestConfig()

    # Backup — simple scalar config
    backup_raw = raw.get("backup", {})
    if backup_raw:
        _validate_keys("backup", backup_raw, BackupConfig)
    backup = BackupConfig(**backup_raw) if backup_raw else BackupConfig()

    # Output — simple scalar config
    output_raw = raw.get("output", {})
    if output_raw:
        _validate_keys("output", output_raw, OutputConfig)
    output = OutputConfig(**output_raw) if output_raw else OutputConfig()

    # Currency — table merge for rates
    currency_raw = raw.get("currency", {})
    currency_default = currency_raw.get("default", "CHF")
    currency_rates = CurrencyConfig().rates.copy()
    if "rates" in currency_raw:
        currency_rates.update(currency_raw["rates"])

    # -- Anchor relative paths to config file location ---------------------
    # Relative paths in the TOML (e.g. data_dir = "output") are resolved
    # against the config file's parent directory so that the config is
    # self-contained regardless of the process CWD.
    config_root = resolved.parent.resolve() if resolved is not None else pathlib.Path.cwd().resolve()
    paths = _resolve_relative_paths(paths, config_root)
    backup = _resolve_backup_paths(backup, config_root)

    # Merge agent-managed sidecar rates (highest priority)
    data_dir_for_sidecar = os.environ.get("CELLARBRAIN_DATA_DIR") or paths.data_dir
    sidecar = _load_currency_sidecar(data_dir_for_sidecar)
    currency_rates.update(sidecar)

    currency = CurrencyConfig(default=currency_default, rates=currency_rates)

    # -- Env var post-override ----------------------------------------------
    env_data_dir = os.environ.get("CELLARBRAIN_DATA_DIR")
    if env_data_dir:
        paths = PathsConfig(
            data_dir=str(pathlib.Path(env_data_dir).resolve()),
            raw_dir=paths.raw_dir,
            wines_subdir=paths.wines_subdir,
            cellar_subdir=paths.cellar_subdir,
            archive_subdir=paths.archive_subdir,
        )

    # Anchor mutable sommelier paths to the final data_dir
    sommelier = _resolve_sommelier_paths(sommelier, pathlib.Path(paths.data_dir))

    return Settings(
        paths=paths,
        csv=csv_cfg,
        price_tiers=price_tiers,
        query=query,
        display=display,
        drinking_window=drinking_window,
        dossier=dossier,
        agent_sections=agent_sections,
        classification_short=classification_short,
        offsite_cellars=offsite_cellars,
        in_transit_cellars=in_transit_cellars,
        cellar_rules=cellar_rules,
        currency=currency,
        etl=etl,
        identity=identity,
        logging=logging_cfg,
        wishlist=wishlist,
        search=search,
        sommelier=sommelier,
        dashboard=dashboard,
        ingest=ingest,
        backup=backup,
        output=output,
        companion_sections=companion_sections,
        config_source=str(resolved) if resolved else None,
    )
