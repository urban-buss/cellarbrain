"""Tests for the guided wine education module."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cellarbrain import markdown
from cellarbrain.education import (
    FrameworkLevel,
    LearningFramework,
    LearningPath,
    _assign_level,
    _build_curriculum,
    _compute_progress,
    _fetch_education_wines,
    _identify_purchase_gaps,
    _infer_hierarchy,
    _load_frameworks,
    _map_wines_to_levels,
    _match_framework,
    _resolve_match_type,
    build_learning_path,
    format_learning_path,
)
from cellarbrain.query import get_agent_connection
from dataset_factory import (
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_pro_rating,
    make_provider,
    make_tasting,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)

# ---------------------------------------------------------------------------
# Fixture: multi-region dataset with classification hierarchy
# ---------------------------------------------------------------------------


def _build_education_dataset(tmp_path):
    """Create a dataset spanning Burgundy hierarchy levels for education tests."""
    appellations = [
        make_appellation(
            appellation_id=1,
            country="France",
            region="Burgundy",
            subregion="Bourgogne",
            classification=None,
        ),
        make_appellation(
            appellation_id=2,
            country="France",
            region="Burgundy",
            subregion="Gevrey-Chambertin",
            classification=None,
        ),
        make_appellation(
            appellation_id=3,
            country="France",
            region="Burgundy",
            subregion="Chambolle-Musigny",
            classification="1er Cru",
        ),
        make_appellation(
            appellation_id=4,
            country="France",
            region="Burgundy",
            subregion="Gevrey-Chambertin",
            classification="Grand Cru",
        ),
        make_appellation(
            appellation_id=5,
            country="Italy",
            region="Piedmont",
            subregion="Barolo",
            classification=None,
        ),
        make_appellation(
            appellation_id=6,
            country="Spain",
            region="Rioja",
            subregion=None,
            classification="Reserva",
        ),
    ]
    wineries = [
        make_winery(winery_id=1, name="Domaine Regional"),
        make_winery(winery_id=2, name="Domaine Village"),
        make_winery(winery_id=3, name="Domaine Premier"),
        make_winery(winery_id=4, name="Domaine Grand"),
        make_winery(winery_id=5, name="Azienda Piemontese"),
        make_winery(winery_id=6, name="Bodega Riojana"),
    ]
    grapes = [
        make_grape(grape_id=1, name="Pinot Noir"),
        make_grape(grape_id=2, name="Nebbiolo"),
        make_grape(grape_id=3, name="Tempranillo"),
    ]
    wines = [
        # Burgundy Level 1 — Regional Bourgogne
        make_wine(
            wine_id=1,
            winery_id=1,
            winery_name="Domaine Regional",
            name="Bourgogne Rouge",
            vintage=2021,
            appellation_id=1,
            primary_grape="Pinot Noir",
            grape_type="varietal",
            drinking_status="drinkable",
        ),
        # Burgundy Level 2 — Village (Gevrey-Chambertin)
        make_wine(
            wine_id=2,
            winery_id=2,
            winery_name="Domaine Village",
            name="Gevrey-Chambertin",
            vintage=2020,
            appellation_id=2,
            primary_grape="Pinot Noir",
            grape_type="varietal",
            drinking_status="optimal",
        ),
        # Burgundy Level 3 — Premier Cru
        make_wine(
            wine_id=3,
            winery_id=3,
            winery_name="Domaine Premier",
            name="Chambolle-Musigny 1er Cru Les Amoureuses",
            vintage=2019,
            appellation_id=3,
            primary_grape="Pinot Noir",
            grape_type="varietal",
            drinking_status="optimal",
            list_price=Decimal("150.00"),
            classification="1er Cru",
        ),
        # Burgundy Level 4 — Grand Cru
        make_wine(
            wine_id=4,
            winery_id=4,
            winery_name="Domaine Grand",
            name="Chambertin Grand Cru",
            vintage=2018,
            appellation_id=4,
            primary_grape="Pinot Noir",
            grape_type="varietal",
            drinking_status="optimal",
            list_price=Decimal("400.00"),
            classification="Grand Cru",
        ),
        # Piedmont (non-Burgundy, for topic resolution tests)
        make_wine(
            wine_id=5,
            winery_id=5,
            winery_name="Azienda Piemontese",
            name="Barolo",
            vintage=2017,
            appellation_id=5,
            primary_grape="Nebbiolo",
            grape_type="varietal",
            drinking_status="drinkable",
        ),
        # Rioja (for inferred hierarchy tests)
        make_wine(
            wine_id=6,
            winery_id=6,
            winery_name="Bodega Riojana",
            name="Rioja Reserva",
            vintage=2018,
            appellation_id=6,
            primary_grape="Tempranillo",
            grape_type="varietal",
            drinking_status="drinkable",
            classification="Reserva",
        ),
    ]
    wine_grapes = [
        make_wine_grape(wine_id=1, grape_id=1),
        make_wine_grape(wine_id=2, grape_id=1),
        make_wine_grape(wine_id=3, grape_id=1),
        make_wine_grape(wine_id=4, grape_id=1),
        make_wine_grape(wine_id=5, grape_id=2),
        make_wine_grape(wine_id=6, grape_id=3),
    ]
    bottles = [
        make_bottle(bottle_id=1, wine_id=1),
        make_bottle(bottle_id=2, wine_id=2),
        make_bottle(bottle_id=3, wine_id=3),
        make_bottle(bottle_id=4, wine_id=4),
        make_bottle(bottle_id=5, wine_id=5),
        make_bottle(bottle_id=6, wine_id=6),
    ]
    tastings = [
        # Wine 2 has been tasted, others not
        make_tasting(tasting_id=1, wine_id=2),
    ]
    entities = {
        "winery": wineries,
        "appellation": appellations,
        "grape": grapes,
        "wine": wines,
        "wine_grape": wine_grapes,
        "bottle": bottles,
        "cellar": [make_cellar()],
        "provider": [make_provider()],
        "tasting": tastings,
        "pro_rating": [
            make_pro_rating(rating_id=1, wine_id=3, score=95.0),
            make_pro_rating(rating_id=2, wine_id=4, score=98.0),
        ],
        "etl_run": [make_etl_run()],
        "change_log": [make_change_log()],
    }
    write_dataset(tmp_path, entities)
    markdown.generate_dossiers(entities, tmp_path, current_year=2026)
    return tmp_path


@pytest.fixture()
def data_dir(tmp_path):
    return _build_education_dataset(tmp_path)


@pytest.fixture()
def con(data_dir):
    return get_agent_connection(data_dir)


# ---------------------------------------------------------------------------
# Fixture: curated Burgundy framework
# ---------------------------------------------------------------------------


@pytest.fixture()
def burgundy_framework():
    """A minimal Burgundy framework for unit tests."""
    return LearningFramework(
        key="burgundy",
        name="Burgundy (Côte d'Or)",
        match_regions=["Burgundy"],
        match_country="France",
        description="Terroir-driven Pinot Noir & Chardonnay",
        key_grapes=["Pinot Noir", "Chardonnay"],
        levels=[
            FrameworkLevel(
                level=1,
                name="Regional",
                description="Broad regional blends",
                match_classifications=[],
                match_subregions=[],
                match_subregion_contains="Bourgogne",
                key_lesson="Set the baseline",
                contrast_prompt="Notice the simplicity",
            ),
            FrameworkLevel(
                level=2,
                name="Village",
                description="Named village wines",
                match_classifications=[],
                match_subregions=["Gevrey-Chambertin", "Chambolle-Musigny"],
                match_subregion_contains="",
                key_lesson="Each village has a signature",
                contrast_prompt="Compare tannin texture",
            ),
            FrameworkLevel(
                level=3,
                name="Premier Cru",
                description="Named vineyard sites",
                match_classifications=["1er Cru"],
                match_subregions=[],
                match_subregion_contains="",
                key_lesson="A single vineyard speaks louder",
                contrast_prompt="How does Premier Cru differ?",
            ),
            FrameworkLevel(
                level=4,
                name="Grand Cru",
                description="Exceptional terroir",
                match_classifications=["Grand Cru"],
                match_subregions=[],
                match_subregion_contains="",
                key_lesson="Power and finesse combined",
                contrast_prompt="Can you feel the mineral backbone?",
            ),
        ],
    )


# ---------------------------------------------------------------------------
# TestLoadFrameworks
# ---------------------------------------------------------------------------


class TestLoadFrameworks:
    def test_loads_builtin_frameworks(self):
        frameworks = _load_frameworks()
        assert len(frameworks) >= 4
        names = {fw.key for fw in frameworks}
        assert "burgundy" in names
        assert "piedmont" in names
        assert "bordeaux" in names
        assert "germany_vdp" in names

    def test_each_framework_has_levels(self):
        frameworks = _load_frameworks()
        for fw in frameworks:
            assert len(fw.levels) >= 2, f"{fw.key} has fewer than 2 levels"
            # Levels should be sorted ascending
            level_nums = [l.level for l in fw.levels]
            assert level_nums == sorted(level_nums), f"{fw.key} levels not sorted"

    def test_missing_file_returns_empty(self, tmp_path):
        result = _load_frameworks(tmp_path / "nonexistent.toml")
        assert result == []


# ---------------------------------------------------------------------------
# TestMatchFramework
# ---------------------------------------------------------------------------


class TestMatchFramework:
    def test_match_by_region(self):
        frameworks = _load_frameworks()
        fw = _match_framework("Burgundy", frameworks)
        assert fw is not None
        assert fw.key == "burgundy"

    def test_match_by_country(self):
        frameworks = _load_frameworks()
        fw = _match_framework("France", frameworks)
        assert fw is not None
        assert fw.key == "burgundy"

    def test_match_case_insensitive(self):
        frameworks = _load_frameworks()
        fw = _match_framework("burgundy", frameworks)
        assert fw is not None

    def test_no_match(self):
        frameworks = _load_frameworks()
        fw = _match_framework("Narnia", frameworks)
        assert fw is None


# ---------------------------------------------------------------------------
# TestResolveMatchType
# ---------------------------------------------------------------------------


class TestResolveMatchType:
    def test_region_match(self, con):
        assert _resolve_match_type(con, "Burgundy") == "region"

    def test_country_match(self, con):
        assert _resolve_match_type(con, "France") == "country"

    def test_subregion_match(self, con):
        assert _resolve_match_type(con, "Barolo") == "subregion"

    def test_grape_match(self, con):
        assert _resolve_match_type(con, "Nebbiolo") == "grape"

    def test_no_match(self, con):
        assert _resolve_match_type(con, "Narnia") is None


# ---------------------------------------------------------------------------
# TestAssignLevel
# ---------------------------------------------------------------------------


class TestAssignLevel:
    def test_regional_by_subregion_contains(self, burgundy_framework):
        wine = {"classification": "", "subregion": "Bourgogne"}
        assert _assign_level(wine, burgundy_framework) == 1

    def test_village_by_subregion(self, burgundy_framework):
        wine = {"classification": "", "subregion": "Gevrey-Chambertin"}
        assert _assign_level(wine, burgundy_framework) == 2

    def test_premier_cru_by_classification(self, burgundy_framework):
        wine = {"classification": "1er Cru", "subregion": "Chambolle-Musigny"}
        assert _assign_level(wine, burgundy_framework) == 3

    def test_grand_cru_by_classification(self, burgundy_framework):
        wine = {"classification": "Grand Cru", "subregion": "Gevrey-Chambertin"}
        assert _assign_level(wine, burgundy_framework) == 4

    def test_unknown_defaults_to_lowest(self, burgundy_framework):
        wine = {"classification": "", "subregion": "SomeRandomPlace"}
        assert _assign_level(wine, burgundy_framework) == 1


# ---------------------------------------------------------------------------
# TestMapWinesToLevels
# ---------------------------------------------------------------------------


class TestMapWinesToLevels:
    def test_maps_correctly(self, burgundy_framework):
        wines = [
            {"wine_id": 1, "classification": "", "subregion": "Bourgogne"},
            {"wine_id": 2, "classification": "", "subregion": "Gevrey-Chambertin"},
            {"wine_id": 3, "classification": "1er Cru", "subregion": "Chambolle-Musigny"},
            {"wine_id": 4, "classification": "Grand Cru", "subregion": "Gevrey-Chambertin"},
        ]
        by_level = _map_wines_to_levels(wines, burgundy_framework)
        assert len(by_level[1]) == 1
        assert len(by_level[2]) == 1
        assert len(by_level[3]) == 1
        assert len(by_level[4]) == 1


# ---------------------------------------------------------------------------
# TestInferHierarchy
# ---------------------------------------------------------------------------


class TestInferHierarchy:
    def test_infers_from_classifications(self):
        wines = [
            {"classification": "Reserva", "subregion": "Rioja", "region": "Rioja"},
            {"classification": "Gran Reserva", "subregion": "Rioja", "region": "Rioja"},
            {"classification": None, "subregion": "Rioja", "region": "Rioja"},
        ]
        fw = _infer_hierarchy(wines)
        assert fw.key == "inferred"
        # Should have an "Unclassified" level + 2 classification levels
        assert len(fw.levels) == 3

    def test_infers_from_subregions_when_no_classifications(self):
        wines = [
            {"classification": None, "subregion": "SubA", "region": "TestRegion"},
            {"classification": None, "subregion": "SubB", "region": "TestRegion"},
        ]
        fw = _infer_hierarchy(wines)
        assert fw.key == "inferred"
        level_names = {l.name for l in fw.levels}
        assert "SubA" in level_names
        assert "SubB" in level_names


# ---------------------------------------------------------------------------
# TestBuildCurriculum
# ---------------------------------------------------------------------------


class TestBuildCurriculum:
    def test_selects_one_per_level(self, burgundy_framework):
        wines_by_level = {
            1: [
                {
                    "wine_id": 1,
                    "wine_name": "Regional",
                    "vintage": 2021,
                    "winery_name": "W1",
                    "subregion": "Bourgogne",
                    "classification": "",
                    "drinking_status": "drinkable",
                    "tasting_count": 0,
                    "best_pro_score": None,
                }
            ],
            2: [
                {
                    "wine_id": 2,
                    "wine_name": "Village",
                    "vintage": 2020,
                    "winery_name": "W2",
                    "subregion": "Gevrey-Chambertin",
                    "classification": "",
                    "drinking_status": "optimal",
                    "tasting_count": 1,
                    "best_pro_score": 90,
                }
            ],
            3: [
                {
                    "wine_id": 3,
                    "wine_name": "1er Cru",
                    "vintage": 2019,
                    "winery_name": "W3",
                    "subregion": "Chambolle-Musigny",
                    "classification": "1er Cru",
                    "drinking_status": "optimal",
                    "tasting_count": 0,
                    "best_pro_score": 95,
                }
            ],
            4: [
                {
                    "wine_id": 4,
                    "wine_name": "Grand Cru",
                    "vintage": 2018,
                    "winery_name": "W4",
                    "subregion": "Gevrey-Chambertin",
                    "classification": "Grand Cru",
                    "drinking_status": "optimal",
                    "tasting_count": 0,
                    "best_pro_score": 98,
                }
            ],
        }
        curriculum = _build_curriculum(wines_by_level, burgundy_framework, lesson_size=4)
        assert len(curriculum) == 4
        # Positions should be sequential
        assert [c.position for c in curriculum] == [1, 2, 3, 4]
        # Levels should be ascending
        assert [c.level for c in curriculum] == [1, 2, 3, 4]

    def test_respects_lesson_size(self, burgundy_framework):
        wines_by_level = {
            i: [
                {
                    "wine_id": i,
                    "wine_name": f"Wine {i}",
                    "vintage": 2020,
                    "winery_name": f"W{i}",
                    "subregion": "Test",
                    "classification": "",
                    "drinking_status": "drinkable",
                    "tasting_count": 0,
                    "best_pro_score": None,
                }
            ]
            for i in range(1, 5)
        }
        curriculum = _build_curriculum(wines_by_level, burgundy_framework, lesson_size=2)
        assert len(curriculum) == 2

    def test_empty_levels_skipped(self, burgundy_framework):
        wines_by_level = {
            1: [
                {
                    "wine_id": 1,
                    "wine_name": "Regional",
                    "vintage": 2021,
                    "winery_name": "W1",
                    "subregion": "Bourgogne",
                    "classification": "",
                    "drinking_status": "drinkable",
                    "tasting_count": 0,
                    "best_pro_score": None,
                }
            ],
            # Levels 2 and 3 are empty
            4: [
                {
                    "wine_id": 4,
                    "wine_name": "Grand Cru",
                    "vintage": 2018,
                    "winery_name": "W4",
                    "subregion": "Gevrey-Chambertin",
                    "classification": "Grand Cru",
                    "drinking_status": "optimal",
                    "tasting_count": 0,
                    "best_pro_score": 98,
                }
            ],
        }
        curriculum = _build_curriculum(wines_by_level, burgundy_framework, lesson_size=4)
        assert len(curriculum) == 2
        assert curriculum[0].level == 1
        assert curriculum[1].level == 4


# ---------------------------------------------------------------------------
# TestComputeProgress
# ---------------------------------------------------------------------------


class TestComputeProgress:
    def test_progress_covers_all_levels(self, burgundy_framework):
        wines_by_level = {
            1: [{"tasting_count": 0, "subregion": "Bourgogne"}],
            2: [{"tasting_count": 1, "subregion": "Gevrey-Chambertin"}],
        }
        progress = _compute_progress(wines_by_level, burgundy_framework)
        assert len(progress) == 4  # All 4 levels included
        assert progress[0].wines_owned == 1
        assert progress[0].wines_tasted == 0
        assert progress[1].wines_owned == 1
        assert progress[1].wines_tasted == 1
        assert progress[2].wines_owned == 0  # Level 3 empty
        assert progress[3].wines_owned == 0  # Level 4 empty


# ---------------------------------------------------------------------------
# TestIdentifyPurchaseGaps
# ---------------------------------------------------------------------------


class TestIdentifyPurchaseGaps:
    def test_identifies_empty_levels(self, burgundy_framework):
        wines_by_level = {
            1: [{"subregion": "Bourgogne"}],
            # Levels 2, 3, 4 empty
        }
        gaps = _identify_purchase_gaps(wines_by_level, burgundy_framework)
        level_gaps = [g for g in gaps if g.dimension == "level"]
        assert len(level_gaps) == 3

    def test_identifies_missing_subregions(self, burgundy_framework):
        wines_by_level = {
            2: [{"subregion": "Gevrey-Chambertin"}],
        }
        gaps = _identify_purchase_gaps(wines_by_level, burgundy_framework)
        sub_gaps = [g for g in gaps if g.dimension == "subregion"]
        # Level 2 defines Gevrey-Chambertin and Chambolle-Musigny
        # One is covered, one missing
        assert len(sub_gaps) == 1
        assert sub_gaps[0].value == "Chambolle-Musigny"


# ---------------------------------------------------------------------------
# TestFetchEducationWines
# ---------------------------------------------------------------------------


class TestFetchEducationWines:
    def test_fetch_by_region(self, con):
        wines = _fetch_education_wines(con, "Burgundy")
        assert len(wines) == 4  # All Burgundy wines
        assert all(w["region"] == "Burgundy" for w in wines)

    def test_fetch_by_grape(self, con):
        wines = _fetch_education_wines(con, "Nebbiolo")
        assert len(wines) == 1
        assert wines[0]["primary_grape"] == "Nebbiolo"

    def test_fetch_no_match(self, con):
        wines = _fetch_education_wines(con, "Narnia")
        assert wines == []


# ---------------------------------------------------------------------------
# TestBuildLearningPath
# ---------------------------------------------------------------------------


class TestBuildLearningPath:
    def test_burgundy_curated_framework(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        assert path.framework_name == "Burgundy (Côte d'Or)"
        assert path.match_type == "region"
        assert path.total_wines == 4
        assert len(path.progress) == 4
        assert len(path.curriculum) >= 2

    def test_curriculum_levels_ascending(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        levels = [c.level for c in path.curriculum]
        assert levels == sorted(levels)

    def test_inferred_framework_for_unknown_region(self, con, data_dir):
        path = build_learning_path(con, "Rioja", data_dir)
        assert path is not None
        assert path.match_type == "inferred"

    def test_no_wines_returns_none(self, con, data_dir):
        path = build_learning_path(con, "Narnia", data_dir)
        assert path is None

    def test_lesson_size_clamped(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir, lesson_size=2)
        assert path is not None
        assert len(path.curriculum) <= 2

    def test_purchase_suggestions_present(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        # Burgundy village level has match_subregions defined — at least
        # some subregions will be missing from the test data
        sub_gaps = [s for s in path.purchase_suggestions if s.dimension == "subregion"]
        assert len(sub_gaps) >= 1

    def test_no_purchases_flag(self, con, data_dir):
        path = build_learning_path(
            con,
            "Burgundy",
            data_dir,
            include_purchases=False,
        )
        assert path is not None
        assert path.purchase_suggestions == []

    def test_tasting_progress(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        # Wine 2 (Village) has a tasting record
        assert path.total_tasted >= 1


# ---------------------------------------------------------------------------
# TestFormatLearningPath
# ---------------------------------------------------------------------------


class TestFormatLearningPath:
    def test_contains_header(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        text = format_learning_path(path)
        assert "## Learning Path:" in text

    def test_contains_progress_table(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        text = format_learning_path(path)
        assert "### Your Progress" in text
        assert "| Level | Coverage |" in text

    def test_contains_tasting_lesson(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        text = format_learning_path(path)
        assert "### Tasting Lesson" in text
        assert "What to notice" in text

    def test_contains_purchase_suggestions(self, con, data_dir):
        path = build_learning_path(con, "Burgundy", data_dir)
        assert path is not None
        text = format_learning_path(path)
        assert "### Purchase Suggestions" in text

    def test_empty_path_formats_cleanly(self):
        path = LearningPath(
            topic="Test",
            framework_name="Test",
            framework_description="Testing",
            match_type="region",
        )
        text = format_learning_path(path)
        assert "## Learning Path: Test" in text
