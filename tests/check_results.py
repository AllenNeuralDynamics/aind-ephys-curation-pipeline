"""Checks the curated asset written by the curation pipeline against the processed asset it came from."""
import argparse
import json
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

RAW_STAGE = "Raw data"


def read_shape(array_folder: Path) -> list:
    """Shape of a zarr array (v2 or v3) from its metadata, without zarr."""
    for metadata_file in (".zarray", "zarr.json"):
        if (array_folder / metadata_file).is_file():
            return json.loads((array_folder / metadata_file).read_text())["shape"]
    raise FileNotFoundError(f"No zarr array at {array_folder}")


def read_json(url_or_path: str) -> dict:
    """Reads a local JSON file or a public s3:// one."""
    if url_or_path.startswith("s3://"):
        bucket, key = url_or_path[len("s3://"):].split("/", 1)
        url_or_path = f"https://{bucket}.s3.amazonaws.com/{urllib.parse.quote(key)}"
        with urllib.request.urlopen(url_or_path) as response:
            return json.load(response)
    return json.loads(Path(url_or_path).read_text())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-path", required=True)
    parser.add_argument("--sorted-url", required=True, help="processed asset the pipeline ran on (local or s3://)")
    args = parser.parse_args()
    results = Path(args.results_path)

    # synthetic test data (tests/sample_dataset/create_test_data.py) comes with the expected curation result
    expected_file = Path(args.sorted_url) / "test_expected.json"
    expected = json.loads(expected_file.read_text()) if expected_file.is_file() else {}

    # curated analyzers and the curation applied to them
    analyzers = sorted((results / "postprocessed").glob("*.zarr"))
    assert len(analyzers) > 0, "No curated analyzers in postprocessed/"
    if expected:
        assert sorted(a.name[: -len(".zarr")] for a in analyzers) == sorted(expected), "Unexpected streams"
    num_curated_units = 0
    for analyzer in analyzers:
        recording_name = analyzer.name[: -len(".zarr")]
        num_units = read_shape(analyzer / "sorting" / "unit_ids")[0]
        num_curated_units += num_units
        curation_file = results / "curated" / recording_name / "curation.json"
        assert curation_file.is_file(), f"Missing {curation_file}"
        curation = json.loads(curation_file.read_text())
        result = dict(units=num_units, removed=len(curation["removed"]), merges=len(curation["merges"]))
        print(f"{recording_name}: {result}")
        if recording_name in expected:
            assert result == expected[recording_name], f"Expected {expected[recording_name]} for {recording_name}"

    # QC: raw-data metrics inherited unchanged, processing metrics recomputed
    qc = read_json(str(results / "quality_control.json"))
    input_qc = read_json(f"{args.sorted_url.rstrip('/')}/quality_control.json")
    raw_metrics = [m for m in qc["metrics"] if m["stage"] == RAW_STAGE]
    input_raw_metrics = [m for m in input_qc["metrics"] if m["stage"] == RAW_STAGE]
    assert raw_metrics == input_raw_metrics, "Raw-data QC metrics differ from the processed asset"
    stages = Counter(m["stage"] for m in qc["metrics"])
    assert stages.get("Processing", 0) > 0, "No processing QC metrics"
    for metric in qc["metrics"]:
        reference = metric.get("reference")
        if reference and not reference.startswith("http"):
            assert (results / reference).is_file(), f"Missing QC figure {reference}"
    print(f"QC metrics: {dict(stages)}")

    # NWB: units table replaced with the curated units
    nwb_files = sorted((results / "nwb").glob("*.nwb*"))
    assert len(nwb_files) > 0, "No NWB files in nwb/"
    num_nwb_units = 0
    for nwb_file in nwb_files:
        assert nwb_file.is_dir(), f"Only zarr NWB files are checked: {nwb_file.name}"
        num_units = read_shape(nwb_file / "units" / "id")[0] if (nwb_file / "units").is_dir() else 0
        num_nwb_units += num_units
        print(f"{nwb_file.name}: {num_units} units")
    assert num_nwb_units == num_curated_units, (
        f"NWB units ({num_nwb_units}) don't match the curated analyzers ({num_curated_units})"
    )
    print("All checks passed")


if __name__ == "__main__":
    main()
