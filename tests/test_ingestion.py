"""Tests for data ingestion pipeline (CSV, XML, directory, backfill)."""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from grandpa_joe.brain.racing_brain import RacingBrain


def _write_test_csv(path: str):
    """Write a minimal Equibase-style CSV for testing."""
    with open(path, "w") as f:
        f.write(
            "Track,Date,Race,Horse,Jockey,Trainer,PP,ML,Finish,Odds,"
            "Speed,Surface,Dist,Condition,Weight,BL,Comment\n"
            "SAR,2023-08-01,1,Thunder Road,Irad Ortiz Jr.,Chad Brown,3,5-2,1,2.8,"
            "95,D,6.0,FT,126,0,led throughout\n"
            "SAR,2023-08-01,1,Night Owl,Joel Rosario,Todd Pletcher,5,4-1,2,3.9,"
            "91,D,6.0,FT,122,2.5,rallied late\n"
            "SAR,2023-08-01,1,Fast Lane,John Velazquez,Bill Mott,1,8-1,3,7.5,"
            "88,D,6.0,FT,124,4.0,pressed pace\n"
        )


def _write_test_xml(path: str):
    """Write a minimal Equibase-style XML chart for testing."""
    with open(path, "w") as f:
        f.write("""<?xml version="1.0" encoding="UTF-8"?>
<Charts>
  <Race>
    <TrackCode>CD</TrackCode>
    <TrackName>Churchill Downs</TrackName>
    <RaceDate>2023-05-06</RaceDate>
    <RaceNumber>12</RaceNumber>
    <RaceName>Kentucky Derby</RaceName>
    <RaceType>stakes</RaceType>
    <Grade>G1</Grade>
    <Surface>Dirt</Surface>
    <DistanceFurlongs>10.0</DistanceFurlongs>
    <Purse>5000000</Purse>
    <TrackCondition>Fast</TrackCondition>
    <Starter>
      <HorseName>Mage</HorseName>
      <Jockey>Javier Castellano</Jockey>
      <Trainer>Gustavo Delgado</Trainer>
      <PostPosition>8</PostPosition>
      <MorningLineOdds>15-1</MorningLineOdds>
      <Weight>126</Weight>
      <FinishPosition>1</FinishPosition>
      <FinalOdds>15.8</FinalOdds>
      <SpeedFigure>97</SpeedFigure>
      <BeatenLengths>0</BeatenLengths>
      <FinalTime>2:01.57</FinalTime>
      <Comment>stalked pace rallied</Comment>
      <Sire>Good Magic</Sire>
      <Dam>Puca</Dam>
      <Sex>C</Sex>
      <Age>3</Age>
    </Starter>
    <Starter>
      <HorseName>Two Phil's</HorseName>
      <Jockey>Jareth Loveberry</Jockey>
      <Trainer>Larry Rivelli</Trainer>
      <PostPosition>12</PostPosition>
      <MorningLineOdds>30-1</MorningLineOdds>
      <Weight>126</Weight>
      <FinishPosition>2</FinishPosition>
      <FinalOdds>32.4</FinalOdds>
      <SpeedFigure>95</SpeedFigure>
      <BeatenLengths>1.0</BeatenLengths>
      <Comment>closed well from far back</Comment>
      <Sire>Hard Spun</Sire>
      <Dam>Phil's Dream</Dam>
      <Sex>C</Sex>
      <Age>3</Age>
    </Starter>
  </Race>
</Charts>
""")


def test_csv_ingestion():
    """Test CSV ingestion with Equibase-style columns."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        csv_path = os.path.join(tmpdir, "test.csv")
        _write_test_csv(csv_path)

        from grandpa_joe.brain.ingestion import ingest_csv
        counts = ingest_csv(brain, csv_path)

        assert counts["rows_processed"] == 3
        assert counts["entries"] == 3
        assert counts["results"] == 3
        assert counts["past_performances"] == 3

        stats = brain.get_memory_stats()
        assert stats["horses"] == 3
        assert stats["entries"] == 3
        assert stats["results"] == 3
        print("PASS: csv_ingestion")


def test_xml_ingestion():
    """Test XML chart ingestion."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        xml_path = os.path.join(tmpdir, "test.xml")
        _write_test_xml(xml_path)

        from grandpa_joe.brain.equibase_xml import ingest_xml
        counts = ingest_xml(brain, xml_path)

        assert counts["races"] == 1
        assert counts["entries"] == 2
        assert counts["results"] == 2
        assert counts["past_performances"] == 2

        # Verify race data
        stats = brain.get_memory_stats()
        assert stats["horses"] == 2
        assert stats["races"] == 1

        # Check horse pedigree was stored
        assert "Mage" in brain.horse_cache

        # Check track was created
        assert "CD" in brain.track_cache
        print("PASS: xml_ingestion")


def test_directory_ingestion():
    """Test ingesting a directory with CSV and XML files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)

        # Create data dir with both formats
        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)
        _write_test_csv(os.path.join(data_dir, "saratoga.csv"))
        _write_test_xml(os.path.join(data_dir, "derby.xml"))

        from grandpa_joe.brain.equibase_fetch import ingest_directory
        counts = ingest_directory(brain, data_dir)

        assert counts["files_processed"] == 2
        assert counts.get("files_failed", 0) == 0

        stats = brain.get_memory_stats()
        assert stats["horses"] == 5  # 3 from CSV + 2 from XML
        assert stats["races"] == 2   # 1 from CSV + 1 from XML
        print("PASS: directory_ingestion")


def test_backfill_days_since():
    """Test days_since_prev_race backfill."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)

        # Add two PPs for same horse on different dates
        brain.store_past_performance(
            "Speed Demon", "2023-07-01", "SAR",
            surface="dirt", distance_furlongs=6.0,
            finish_position=2, speed_figure=88,
        )
        brain.store_past_performance(
            "Speed Demon", "2023-07-15", "SAR",
            surface="dirt", distance_furlongs=6.0,
            finish_position=1, speed_figure=92,
        )
        brain.store_past_performance(
            "Speed Demon", "2023-08-10", "SAR",
            surface="dirt", distance_furlongs=6.0,
            finish_position=3, speed_figure=85,
        )

        from grandpa_joe.brain.equibase_fetch import compute_days_since_previous
        updated = compute_days_since_previous(brain)

        # Should update 2 records (the 2nd and 3rd PPs)
        assert updated == 2

        # Verify the computed values
        horse_id = brain.horse_cache["Speed Demon"]
        pps = brain.get_horse_pps(horse_id)
        # Most recent first: Aug 10, Jul 15, Jul 1
        assert pps[0]["days_since_prev_race"] == 26  # Aug 10 - Jul 15
        assert pps[1]["days_since_prev_race"] == 14  # Jul 15 - Jul 1
        print("PASS: backfill_days_since")


def test_xml_directory_ingestion():
    """Test ingesting a directory of XML files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        xml_dir = os.path.join(tmpdir, "xmls")
        os.makedirs(xml_dir)
        _write_test_xml(os.path.join(xml_dir, "chart1.xml"))

        from grandpa_joe.brain.equibase_xml import ingest_xml_directory
        counts = ingest_xml_directory(brain, xml_dir)

        assert counts.get("files_processed", 0) == 1
        assert counts["races"] == 1
        print("PASS: xml_directory_ingestion")


def test_fetcher_status():
    """Test EquibaseFetcher status without API key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        from grandpa_joe.brain.equibase_fetch import EquibaseFetcher
        fetcher = EquibaseFetcher(download_dir=tmpdir)
        status = fetcher.get_status()

        assert status["api_key_set"] is False
        assert status["local_files"] == 0
        print("PASS: fetcher_status")


def test_odds_parsing():
    """Test various odds format parsing."""
    from grandpa_joe.brain.ingestion import _safe_float

    # Standard fractional odds
    assert _safe_float("5-2") == 2.5
    assert _safe_float("3-1") == 3.0
    assert _safe_float("1-1") == 1.0

    # Decimal odds
    assert _safe_float("3.50") == 3.5

    # Time parsing
    assert _safe_float("1:10.2") == 70.2

    # Money
    assert _safe_float("$5,000") == 5000.0

    # Invalid
    assert _safe_float("N/A") is None
    assert _safe_float("") is None
    print("PASS: odds_parsing")


if __name__ == "__main__":
    test_odds_parsing()
    test_csv_ingestion()
    test_xml_ingestion()
    test_directory_ingestion()
    test_backfill_days_since()
    test_xml_directory_ingestion()
    test_fetcher_status()
    print("\nAll ingestion tests passed!")
