#!/usr/bin/env python3
"""
Creates a raw session and a processed asset to test the curation pipeline.

raw/sample.nwb: 2 streams of synthetic ground-truth data (like aind-ephys-pipeline's create_test_nwb.py).
processed/: the asset the aind-ephys-pipeline would write for it, with known curation work to do. Per stream:
- NUM_OVERSPLIT ground-truth units are oversplit in two halves, merged by curated/{stream}/curation.json
- NUM_NOISE random units are labeled noise by both UnitRefine and Bombcell (removed with "unitrefine&bombcell")
- one good unit is labeled noise by UnitRefine only (removed only with "unitrefine|bombcell")
- one curation merge pairs a noise unit with a good unit, and must be dropped with the noise unit
- nwb/ has a units table (to replace) and quality_control.json has raw-data metrics (to keep)
processed/test_expected.json has the expected curation result of each stream, read by tests/check_results.py.

Run in the aind-ephys-pipeline-nwb image (spikeinterface, neuroconv, pynwb, hdmf-zarr, aind-data-schema).
"""

import json
from pathlib import Path

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import spikeinterface as si
import spikeinterface.extractors as se
from pynwb import NWBHDF5IO
from pynwb.testing.mock.file import mock_NWBFile, mock_Subject
from hdmf_zarr import NWBZarrIO
from neuroconv.tools.spikeinterface import add_recording_to_nwbfile, add_electrodes_to_nwbfile, add_sorting_to_nwbfile
from aind_data_schema.core.quality_control import QualityControl, QCMetric, QCStatus, Status, Stage
from aind_data_schema_models.modalities import Modality

this_folder = Path(__file__).parent
raw_folder = this_folder / "raw"
processed_folder = this_folder / "processed"

SEED = 2308
NUM_CHANNELS = 32
NUM_UNITS = 10
NUM_OVERSPLIT = 3
NUM_NOISE = 3
DURATION = 60
# ElectrodeGroup names must not contain underscores (see aind-ephys-pipeline's create_test_nwb.py)
STREAMS = [
    dict(es_name="main", device="NP1ProbeA", location="VISp", seed_offset=0),
    dict(es_name="second", device="NP1ProbeB", location="VISl", seed_offset=1),
]
EXTENSIONS = [
    "random_spikes", "noise_levels", "waveforms", "templates", "spike_amplitudes", "unit_locations",
    "template_similarity", "correlograms", "isi_histograms", "template_metrics",
]
QUALITY_METRICS = [
    "num_spikes", "firing_rate", "presence_ratio", "snr", "isi_violation", "rp_violation",
    "amplitude_cutoff", "amplitude_median",
]


def make_metadata(stream):
    es_key = f"ElectricalSeries{stream['es_name'].capitalize()}"
    device = dict(name=stream["device"], manufacturer="imec", description=f"Neuropixels 1.0 - {stream['device']}")
    return es_key, dict(
        Ecephys={
            "Device": [device],
            "ElectrodeGroup": [
                dict(name=stream["device"], description=f"Electrodes of {stream['device']}",
                     location=stream["location"], device=stream["device"])
            ],
            es_key: dict(name=stream["es_name"], description=f"{stream['es_name']} recording"),
        }
    )


def make_ground_truth(stream):
    recording, sorting = si.generate_ground_truth_recording(
        num_channels=NUM_CHANNELS, num_units=NUM_UNITS, durations=[DURATION], seed=SEED + stream["seed_offset"]
    )
    recording.set_property("rel_x", recording.get_channel_locations()[:, 0])
    recording.set_property("rel_y", recording.get_channel_locations()[:, 1])
    recording.set_channel_groups(np.array([stream["device"]] * NUM_CHANNELS, dtype=object))
    return recording, sorting


def make_sorting_to_curate(gt_sorting, seed):
    """Ground truth with oversplit and noise units, the UnitRefine/Bombcell labels and the curation."""
    rng = np.random.default_rng(seed)
    num_samples = gt_sorting.to_spike_vector()["sample_index"].max() + 1
    spike_trains, oversplit_pairs = {}, []
    for index, gt_unit_id in enumerate(gt_sorting.unit_ids):
        spike_train = gt_sorting.get_unit_spike_train(gt_unit_id)
        unit_id = len(spike_trains)
        if index < NUM_OVERSPLIT:
            first_half = rng.random(spike_train.size) < 0.5
            spike_trains[unit_id], spike_trains[unit_id + 1] = spike_train[first_half], spike_train[~first_half]
            oversplit_pairs.append([unit_id, unit_id + 1])
        else:
            spike_trains[unit_id] = spike_train
    good_unit_ids = list(spike_trains)
    noise_unit_ids = []
    for _ in range(NUM_NOISE):
        unit_id = len(spike_trains)
        spike_trains[unit_id] = np.sort(rng.choice(num_samples, size=200, replace=False))
        noise_unit_ids.append(unit_id)
    sorting = si.NumpySorting.from_unit_dict([spike_trains], sampling_frequency=gt_sorting.sampling_frequency)

    unitrefine_only_noise_id = good_unit_ids[-1]
    unitrefine_label = np.array(["sua"] * sorting.get_num_units(), dtype="U10")
    bombcell_label = np.array(["good"] * sorting.get_num_units(), dtype="U10")
    unitrefine_label[noise_unit_ids + [unitrefine_only_noise_id]] = "noise"
    bombcell_label[noise_unit_ids] = "noise"
    sorting.set_property("unitrefine_label", unitrefine_label)
    sorting.set_property("bombcell_label", bombcell_label)
    sorting.set_property("default_qc", bombcell_label != "noise")

    merges = oversplit_pairs + [[noise_unit_ids[0], good_unit_ids[-2]]]
    curation = {
        "format_version": "2",
        "unit_ids": [int(u) for u in sorting.unit_ids],
        "label_definitions": {"quality": {"name": "quality", "label_options": ["good", "MUA", "noise"], "exclusive": True}},
        "manual_labels": [],
        "removed": [],
        "merges": [{"unit_ids": [int(u) for u in merge], "new_unit_id": None} for merge in merges],
        "splits": [],
    }
    # with "unitrefine&bombcell": noise units removed, their merge dropped, oversplit pairs merged
    expected = dict(
        units=sorting.get_num_units() - NUM_NOISE - NUM_OVERSPLIT, removed=NUM_NOISE, merges=NUM_OVERSPLIT
    )
    return sorting, curation, expected


def save_figure(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(2, 2))
    ax.plot([0, 1], [0, 1])
    fig.savefig(path)
    plt.close(fig)


def make_qc_metrics(probe):
    """Raw-data metrics (kept by the curation pipeline) and a processing metric (replaced)."""
    status = [QCStatus(evaluator="test", status=Status.PASS, timestamp="2026-01-01T00:00:00Z")]
    metrics = []
    for name, stage, figure in [("PSD", Stage.RAW, "psd.png"), ("RMS", Stage.RAW, "rms.png"),
                                ("Unit yield", Stage.PROCESSING, "unit_yield.png")]:
        reference = f"quality_control/{probe}/{figure}"
        save_figure(processed_folder / reference)
        metrics.append(QCMetric(
            name=f"{name} - {probe}", modality=Modality.ECEPHYS, stage=stage, value=1.0,
            status_history=status, reference=reference, tags={"probe": probe},
        ))
    return metrics


def main():
    raw_folder.mkdir(parents=True, exist_ok=True)
    processed_folder.mkdir(parents=True, exist_ok=True)

    # raw session
    raw_nwbfile = mock_NWBFile()
    raw_nwbfile.subject = mock_Subject()
    ground_truth = {}
    for stream in STREAMS:
        recording, gt_sorting = make_ground_truth(stream)
        es_key, metadata = make_metadata(stream)
        add_recording_to_nwbfile(recording, nwbfile=raw_nwbfile, metadata=metadata, es_key=es_key)
        ground_truth[stream["es_name"]] = (recording, gt_sorting, metadata)
    with NWBHDF5IO(raw_folder / "sample.nwb", mode="w") as io:
        io.write(raw_nwbfile)

    # processed asset
    asset_nwbfile = mock_NWBFile()
    asset_nwbfile.subject = mock_Subject()
    qc_metrics, expected = [], {}
    for stream in STREAMS:
        recording, gt_sorting, metadata = ground_truth[stream["es_name"]]
        # same names as the aind-ephys-pipeline outputs of an NWB input
        recording_name = f"block0_acquisition-{stream['es_name']}_recording1"
        sorting, curation, expected[recording_name] = make_sorting_to_curate(gt_sorting, SEED + stream["seed_offset"])

        # the analyzer is built on the raw recording, as read back by the pipeline
        raw_recording = se.read_nwb_recording(raw_folder / "sample.nwb", electrical_series_path=f"acquisition/{stream['es_name']}")
        analyzer = si.create_sorting_analyzer(
            sorting, raw_recording, format="zarr", folder=processed_folder / "postprocessed" / f"{recording_name}.zarr"
        )
        analyzer.compute(EXTENSIONS)
        analyzer.compute("quality_metrics", metric_names=QUALITY_METRICS)

        curation_folder = processed_folder / "curated" / recording_name
        curation_folder.mkdir(parents=True)
        (curation_folder / "curation.json").write_text(json.dumps(curation, indent=4))
        sorter_folder = processed_folder / "spikesorted" / recording_name
        sorter_folder.mkdir(parents=True)
        (sorter_folder / "spikeinterface_log.json").write_text(json.dumps({"sorter_name": "kilosort4"}))

        # the asset NWB has electrodes and the pre-curation units
        add_electrodes_to_nwbfile(recording, nwbfile=asset_nwbfile, metadata=metadata)
        add_sorting_to_nwbfile(sorting, nwbfile=asset_nwbfile)

        qc_metrics.extend(make_qc_metrics(f"block0_acquisition-{stream['es_name']}"))

    (processed_folder / "nwb").mkdir()
    with NWBZarrIO(str(processed_folder / "nwb" / "sample_block0_recording1.nwb"), mode="w") as io:
        io.write(asset_nwbfile)
    QualityControl(metrics=qc_metrics, default_grouping=["probe", "stage"]).write_standard_file(
        output_directory=processed_folder
    )
    (processed_folder / "test_expected.json").write_text(json.dumps(expected, indent=4))
    print(f"Expected curation: {expected}")


if __name__ == "__main__":
    main()
