"""Offline request, file-handling, and annual ERA5 regression tests."""
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

import xarray as xr
import yaml

import downloader_ERA5 as dl

ROOT = Path(dl.__file__).resolve().parent
LAND = "reanalysis-era5-land"
SINGLE = "reanalysis-era5-single-levels"
PRESSURE = "reanalysis-era5-pressure-levels"


class LandDownloaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        # Fail closed if a test accidentally constructs a real CDS client.
        self.client_guard = patch.object(dl.cdsapi, "Client", side_effect=AssertionError("Network forbidden"))
        self.client_guard.start()
        self.addCleanup(self.client_guard.stop)

    def config(self, **extra):
        raw = dict(years=[2024], variables=["2m_temperature"], dataset=LAND)
        raw.update(extra)
        path = self.root / "request.yaml"
        path.write_text(yaml.safe_dump(raw))
        return dl.load_config_from_yaml(path)

    def tasks(self, **extra):
        args = dict(years=[2024], variables=["2m_temperature"], dataset=LAND,
                    path_pattern=str(self.root / "{short_name}" / dl.default_name_pattern(LAND)),
                    short_names={"2m_temperature": "t2m"})
        args.update(extra)
        return dl.build_request_tasks(**args)

    def parts(self, **extra):
        return [part for task in self.tasks(**extra) for part in dl.iter_request_parts(task)]

    def target(self, task):
        return Path(dl.build_target_path(
            task.path_pattern, year=task.year, variable=task.variable,
            dataset=task.dataset, short_name=task.short_name,
            pressure_level=task.pressure_level, month=task.month))

    def local_result(self, value=280.0):
        # CDS download transfers existing bytes; create its source outside workers.
        fixture = self.root / f"fixture-{uuid.uuid4().hex}.nc"
        xr.Dataset({"t2m": (("time", "latitude", "longitude"), [[[value]]])},
                   coords={"time": [0], "latitude": [0.0], "longitude": [0.0]}).to_netcdf(fixture)
        result = Mock()
        result.download.side_effect = lambda target: shutil.copyfile(fixture, target)
        return result

    def test_all_datasets_share_annual_configuration(self):
        for dataset in (LAND, SINGLE, PRESSURE):
            with self.subTest(dataset=dataset):
                config = self.config(dataset=dataset)
                self.assertEqual(config["years"], [2024])
                self.assertNotIn("months", config)

    def test_example_configuration(self):
        config = dl.load_config_from_yaml(ROOT / "era5_land_request.yaml")
        self.assertEqual(config["years"], [2024])
        self.assertNotIn("months", config)
        self.assertEqual(config["variables"], ["2m_temperature"])
        self.assertEqual(config["short_names"], {"2m_temperature": "t2m"})

    def test_land_january_request_matches_template(self):
        task = self.parts()[0]
        client = Mock()
        outcome = dl.submit_request(client, task)
        client.retrieve.assert_called_once_with(LAND, {
            "variable": ["2m_temperature"], "year": "2024", "month": "01",
            "day": [f"{day:02d}" for day in range(1, 32)],
            "time": [f"{hour:02d}:00" for hour in range(24)],
            "data_format": "netcdf", "download_format": "unarchived",
        })
        self.assertEqual(Path(outcome.target).name,
                         "era5-land.reanalysis.t2m.1hr.0p1deg.global.202401.nc")

    def test_calendar_days(self):
        for year, month, days in ((2024, 2, 29), (2023, 2, 28), (2024, 4, 30), (2024, 12, 31)):
            with self.subTest(year=year, month=month):
                client = Mock()
                dl.submit_request(client, self.parts(years=[year])[month - 1])
                request = client.retrieve.call_args.args[1]
                self.assertEqual(request["day"], [f"{d:02d}" for d in range(1, days + 1)])

    def test_one_annual_task_expands_to_twelve_distinct_outputs(self):
        tasks = self.tasks()
        self.assertEqual(len(tasks), 1)
        self.assertIsNone(tasks[0].month)
        parts = list(dl.iter_request_parts(tasks[0]))
        self.assertEqual([part.month for part in parts], list(range(1, 13)))
        self.assertEqual(len({self.target(part) for part in parts}), 12)
        self.assertTrue(all(part.pressure_level is None for part in parts))
        self.assertEqual(len(self.tasks(years=[2023, 2024],
                                       variables=["2m_temperature", "surface_pressure"])), 4)

    def test_path_collision_and_month_in_folder(self):
        with self.assertRaisesRegex(ValueError, "same output path"):
            self.tasks(path_pattern=str(self.root / "{year}.{short_name}.nc"))
        tasks = self.parts(path_pattern=str(self.root / "{month}" / "{year}.nc"))[:2]
        self.assertEqual([self.target(t).parent.name for t in tasks], ["01", "02"])

    def test_skip_and_request_failure_keep_month(self):
        january, february = self.parts()[:2]
        self.target(january).parent.mkdir(parents=True)
        self.local_result().download(str(self.target(january)))
        report = dl.ReportCollector()
        client = Mock()
        self.assertIsNone(dl.submit_request(client, january, report=report))
        client.retrieve.assert_not_called()
        client.retrieve.side_effect = RuntimeError("request rejected")
        with self.assertLogs(dl.logger, level="ERROR"):
            self.assertIsNone(dl.submit_request(client, february, report=report))
        self.assertEqual([(e["status"], e["month"]) for e in report.entries],
                         [("skipped", 1), ("failed", 2)])
        log = Mock()
        report.print_summary(log)
        self.assertTrue(any("2024-02" in str(call) for call in log.info.call_args_list))

    def test_real_netcdf_automatic_rename_preserves_month_and_values(self):
        report = dl.ReportCollector()
        for task in self.parts(short_names=None)[:2]:
            original = self.target(task)
            result = self.local_result(value=280.0 + task.month)
            self.assertTrue(dl.perform_download(dl.DownloadTask(result, str(original), task), report=report))
            final = self.target(replace(task, short_name="t2m"))
            self.assertFalse(original.exists())
            self.assertTrue(final.exists())
            with xr.open_dataset(final) as data:
                self.assertEqual(data.t2m.item(), 280.0 + task.month)
        self.assertEqual([(e["status"], e["month"]) for e in report.entries],
                         [("success", 1), ("success", 2)])

    def test_download_failures_keep_month(self):
        for fallback in (False, True):
            with self.subTest(fallback=fallback):
                task = self.parts()[1]
                result = Mock(spec=["download", "location"] if fallback else ["download"])
                result.download.side_effect = RuntimeError("download failed")
                if fallback:
                    result.location = "https://example.invalid/test.nc"
                report = dl.ReportCollector()
                with patch.object(dl, "download_file_with_urllib3", return_value=False), \
                     patch.object(dl.time, "sleep"), self.assertLogs(dl.logger):
                    self.assertFalse(dl.perform_download(
                        dl.DownloadTask(result, str(self.target(task)), task), report=report))
                self.assertEqual(report.entries[0]["month"], 2)
                self.assertEqual(report.entries[0]["stage"],
                                 "fallback_download" if fallback else "default_download")

    def test_fallback_success_keeps_month(self):
        task = self.parts()[2]
        result = Mock()
        result.download.side_effect = RuntimeError("use fallback")
        result.location = "https://example.invalid/test.nc"
        def local_fallback(url, target):
            self.local_result().download(target)
            return True
        report = dl.ReportCollector()
        with patch.object(dl, "download_file_with_urllib3", side_effect=local_fallback), \
             self.assertLogs(dl.logger):
            self.assertTrue(dl.perform_download(
                dl.DownloadTask(result, str(self.target(task)), task), report=report))
        self.assertEqual((report.entries[0]["month"], report.entries[0]["via"]), (3, "urllib3"))

    def test_existing_annual_requests_and_filenames(self):
        for dataset, levels in ((SINGLE, None), (PRESSURE, [500, 850])):
            with self.subTest(dataset=dataset):
                tasks = self.tasks(dataset=dataset, pressure_levels=levels,
                                   path_pattern=dl.default_name_pattern(dataset))
                self.assertEqual(len(tasks), len(levels or [None]))
                for task in tasks:
                    self.assertIsNone(task.month)
                    self.assertEqual(list(dl.iter_request_parts(task)), [task])
                    client = Mock()
                    outcome = dl.submit_request(client, replace(task, skip_existing=False))
                    expected = {
                        "product_type": ["reanalysis"], "variable": ["2m_temperature"],
                        "year": ["2024"], "month": [f"{m:02d}" for m in range(1, 13)],
                        "day": [f"{d:02d}" for d in range(1, 32)],
                        "time": [f"{h:02d}:00" for h in range(24)],
                        "data_format": "netcdf", "download_format": "unarchived",
                    }
                    pressure = ""
                    if levels:
                        expected["pressure_level"] = [task.pressure_level]
                        pressure = f"{task.pressure_level}hpa."
                    client.retrieve.assert_called_once_with(dataset, expected)
                    self.assertEqual(outcome.target,
                                     f"era5.reanalysis.t2m.{pressure}1hr.0p25deg.global.2024.nc")

    def test_annual_queue_streams_monthly_results_to_download_workers(self):
        annual_task = self.tasks()[0]
        task_queue = queue.Queue()
        task_queue.put(annual_task)
        task_queue.put(None)
        report = dl.ReportCollector()
        first_downloaded = threading.Event()
        results = [self.local_result(value=280.0 + month) for month in range(1, 13)]
        first_download = results[0].download.side_effect
        def signal_first_download(target):
            first_download(target)
            first_downloaded.set()
        results[0].download.side_effect = signal_first_download
        requests = []
        def retrieve(dataset, request):
            requests.append(request)
            if request["month"] == "02":
                self.assertTrue(first_downloaded.wait(timeout=10),
                                "Download queue was not fed while annual task was in progress")
            return results[int(request["month"]) - 1]
        clients = []
        def client_factory(**kwargs):
            client = Mock()
            client.retrieve.side_effect = retrieve
            clients.append(client)
            return client
        with patch.object(dl.cdsapi, "Client", side_effect=client_factory):
            threads = dl.start_key_workers("offline-test", task_queue,
                                           download_workers=1, concurrent_requests=1, report=report)
            for thread in threads:
                thread.join(timeout=15)
                self.assertFalse(thread.is_alive(), "Annual pipeline did not terminate")
        self.assertEqual(len(clients), 12)
        self.assertTrue(all(client.retrieve.call_count == 1 for client in clients))
        self.assertEqual([request["month"] for request in requests],
                         [f"{month:02d}" for month in range(1, 13)])
        self.assertEqual(sorted((e["month"], e["status"]) for e in report.entries),
                         [(month, "success") for month in range(1, 13)])
        for part in dl.iter_request_parts(annual_task):
            with xr.open_dataset(self.target(part)) as data:
                self.assertEqual(data.t2m.item(), 280.0 + part.month)

    def test_annual_worker_continues_after_skipped_and_failed_months(self):
        task = self.tasks()[0]
        january = next(dl.iter_request_parts(task))
        self.target(january).parent.mkdir(parents=True)
        self.local_result().download(str(self.target(january)))
        task_queue = queue.Queue()
        task_queue.put(task)
        task_queue.put(None)
        results_queue = queue.Queue()
        report = dl.ReportCollector()
        client = Mock()
        def retrieve(dataset, request):
            if request["month"] == "02":
                raise RuntimeError("February rejected")
            return Mock()
        client.retrieve.side_effect = retrieve
        with patch.object(dl.cdsapi, "Client", return_value=client), self.assertLogs(dl.logger):
            dl.key_request_thread("offline-test", task_queue, results_queue, "test", report)
        self.assertEqual(client.retrieve.call_count, 11)
        self.assertEqual([(e["month"], e["status"]) for e in report.entries],
                         [(1, "skipped"), (2, "failed")])
        self.assertEqual(results_queue.qsize(), 10)
        self.assertEqual([results_queue.get_nowait().request.month for _ in range(10)],
                         list(range(3, 13)))

    def test_cli_rejects_monthly_path_collisions_before_credentials(self):
        raw = dict(dataset=LAND, years=[2024], variables=["2m_temperature"],
                   api_keys_file=str(self.root / "nonexistent-keys.json"),
                   name_pattern="{year}.nc")
        path = self.root / "cli.yaml"
        path.write_text(yaml.safe_dump(raw))
        result = subprocess.run([sys.executable, str(ROOT / "downloader_ERA5.py"),
                                 "--file", str(path)], cwd=self.root,
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 1)
        self.assertIn("same output path", result.stdout + result.stderr)
        self.assertNotIn("API keys file not found", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
