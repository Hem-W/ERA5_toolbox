"""Offline check that ERA5-Land 00 UTC yearly files give the same daily totals as hourly monthly files."""
import importlib.util
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("resampler_ERA5", ROOT / "utils" / "resampler_ERA5.py")
rs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rs)


def land_series(start, end, step_type="accum"):
    """Hourly ERA5-Land-like accumulation: each hour adds 1 + day-of-year/10, restarting after 00 UTC."""
    t = pd.date_range(start, end, freq="h")
    hourly = 1.0 + (t - pd.Timedelta(hours=1)).dayofyear / 10.0
    day = (t - pd.Timedelta(hours=1)).normalize()   # 00 UTC closes the previous day
    acc = pd.Series(hourly).groupby(day).cumsum().to_numpy()
    data = np.broadcast_to(acc[:, None, None], (len(t), 2, 2)).astype("float32")
    da = xr.DataArray(data, dims=("valid_time", "latitude", "longitude"),
                      coords={"valid_time": t, "latitude": [10.0, 9.9], "longitude": [100.0, 100.1]},
                      attrs={"GRIB_stepType": step_type})
    return xr.Dataset({"tp": da})


class Land00utcTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def write_inputs(self, step_type="accum"):
        full = land_series("2023-01-01 00:00", "2024-12-31 23:00", step_type)
        hour, utc = self.root / "hour", self.root / "00utc"
        hour.mkdir(), utc.mkdir()
        for year, month in [(2023, m) for m in range(1, 13)] + [(2024, 1)]:
            sel = full.sel(valid_time=f"{year}-{month:02d}")
            sel.to_netcdf(rs.input_file_path(hour, "tp", year, month, "era5-land"))
        for year in (2023, 2024):
            sel = full.sel(valid_time=str(year))
            sel = sel.isel(valid_time=sel.valid_time.dt.hour == 0)
            sel.to_netcdf(rs.input_file_path(utc, "tp", year, dataset="era5-land-00utc"))
        return hour, utc

    def test_00utc_daily_totals_match_hourly_route(self):
        hour, utc = self.write_inputs()
        outputs = {}
        for name, dataset, input_dir in (("hour", "era5-land", hour), ("utc", "era5-land-00utc", utc)):
            out = self.root / f"day_{name}"
            rs.process_year(2023, variable="tp", input_dir=str(input_dir), output_dir=str(out),
                            chunk_size=2, method="sum", dataset=dataset)
            outputs[name] = Path(rs.output_file_path(out, "tp", 2023, "sum", dataset))
        self.assertEqual(outputs["hour"].name, outputs["utc"].name)
        with xr.open_dataset(outputs["hour"]) as a, xr.open_dataset(outputs["utc"]) as b:
            self.assertEqual(b.sizes["valid_time"], 365)
            np.testing.assert_array_equal(a.valid_time.values, b.valid_time.values)
            np.testing.assert_array_equal(a.tp.values, b.tp.values)
            # Label D holds the 24 hourly increments of day D
            self.assertEqual(pd.Timestamp(b.valid_time.values[0]), pd.Timestamp("2023-01-01"))
            np.testing.assert_allclose(b.tp.values[:, 0, 0],
                                       24 * (1.0 + np.arange(1, 366) / 10.0), rtol=1e-6)

    def test_00utc_rejects_non_accumulated_variable(self):
        _, utc = self.write_inputs(step_type="instant")
        with self.assertRaisesRegex(ValueError, "not an accumulated variable"):
            rs.process_year(2023, variable="tp", input_dir=str(utc), output_dir=str(self.root / "day"),
                            chunk_size=2, method="mean", dataset="era5-land-00utc")


if __name__ == "__main__":
    unittest.main()
