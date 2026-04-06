from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipelines import monthly_ridership_update


def build_project_root(base_dir: Path) -> None:
    paths = [
        base_dir / "scripts" / "api",
        base_dir / "data" / "api" / "baseline",
        base_dir / "data" / "api" / "ridership",
        base_dir / "data" / "api" / "processed",
        base_dir / "data" / "production",
    ]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


class MonthlyRidershipUpdateTests(unittest.TestCase):
    def test_parse_args_rejects_month_without_year(self):
        with self.assertRaises(SystemExit):
            monthly_ridership_update.parse_args(["--month", "2"])

    @patch("pipelines.monthly_ridership_update.subprocess.run")
    def test_skips_baseline_when_files_exist(self, mocked_run):
        mocked_run.return_value.returncode = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            build_project_root(project_root)
            for path in monthly_ridership_update.baseline_file_paths(project_root):
                path.write_text("ready\n", encoding="utf-8")

            exit_code = monthly_ridership_update.main([], project_root=project_root)

            commands = [call.args[0] for call in mocked_run.call_args_list]
            command_text = "\n".join(" ".join(cmd) for cmd in commands)
            self.assertEqual(exit_code, 0)
            self.assertNotIn("calculate_baseline.py", command_text)
            self.assertIn("calculate_final.py", command_text)

    @patch("pipelines.monthly_ridership_update.subprocess.run")
    def test_builds_baseline_when_missing(self, mocked_run):
        mocked_run.return_value.returncode = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            build_project_root(project_root)

            exit_code = monthly_ridership_update.main([], project_root=project_root)

            commands = [call.args[0] for call in mocked_run.call_args_list]
            command_text = "\n".join(" ".join(cmd) for cmd in commands)
            self.assertEqual(exit_code, 0)
            self.assertIn("calculate_baseline.py", command_text)

    @patch("pipelines.monthly_ridership_update.subprocess.run")
    def test_rebuild_baseline_forces_baseline_step(self, mocked_run):
        mocked_run.return_value.returncode = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            build_project_root(project_root)
            for path in monthly_ridership_update.baseline_file_paths(project_root):
                path.write_text("ready\n", encoding="utf-8")

            exit_code = monthly_ridership_update.main(
                ["--rebuild-baseline"],
                project_root=project_root,
            )

            commands = [call.args[0] for call in mocked_run.call_args_list]
            command_text = "\n".join(" ".join(cmd) for cmd in commands)
            self.assertEqual(exit_code, 0)
            self.assertIn("calculate_baseline.py", command_text)

    @patch("pipelines.monthly_ridership_update.subprocess.run")
    def test_forwards_year_and_month_to_station_step(self, mocked_run):
        mocked_run.return_value.returncode = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            build_project_root(project_root)
            for path in monthly_ridership_update.baseline_file_paths(project_root):
                path.write_text("ready\n", encoding="utf-8")

            exit_code = monthly_ridership_update.main(
                ["--year", "2025", "--month", "2"],
                project_root=project_root,
            )

            first_cmd = mocked_run.call_args_list[0].args[0]
            self.assertEqual(exit_code, 0)
            self.assertIn("--year", first_cmd)
            self.assertIn("--month", first_cmd)
            self.assertEqual(first_cmd[-4:], ["--year", "2025", "--month", "2"])

    @patch("pipelines.monthly_ridership_update.subprocess.run")
    def test_forwards_full_refresh_to_station_step(self, mocked_run):
        mocked_run.return_value.returncode = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            build_project_root(project_root)

            exit_code = monthly_ridership_update.main(
                ["--full-refresh"],
                project_root=project_root,
            )

            first_cmd = mocked_run.call_args_list[0].args[0]
            self.assertEqual(exit_code, 0)
            self.assertEqual(first_cmd[-1], "--full-refresh")


if __name__ == "__main__":
    unittest.main()
