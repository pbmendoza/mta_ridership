import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import run_pipeline


class RunPipelinePathTests(unittest.TestCase):
    def test_repo_venv_python_path_for_windows(self):
        base_dir = Path("/tmp/example")
        self.assertEqual(
            run_pipeline.repo_venv_python(base_dir, os_name="nt"),
            base_dir / ".venv" / "Scripts" / "python.exe",
        )

    def test_repo_venv_python_path_for_posix(self):
        base_dir = Path("/tmp/example")
        self.assertEqual(
            run_pipeline.repo_venv_python(base_dir, os_name="posix"),
            base_dir / ".venv" / "bin" / "python",
        )


class RunPipelineBootstrapTests(unittest.TestCase):
    def test_needs_dependency_install_when_stamp_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            (base_dir / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
            self.assertTrue(run_pipeline.needs_dependency_install(base_dir))

    def test_skips_reinstall_when_install_stamp_is_current(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            pyproject = base_dir / "pyproject.toml"
            stamp = run_pipeline.install_stamp_path(base_dir)
            stamp.parent.mkdir(parents=True, exist_ok=True)
            pyproject.write_text("[project]\nname='demo'\n", encoding="utf-8")
            stamp.write_text("ok\n", encoding="utf-8")
            future = pyproject.stat().st_mtime + 10
            os.utime(stamp, (future, future))
            self.assertFalse(run_pipeline.needs_dependency_install(base_dir))

    def test_reinstalls_when_pyproject_is_newer_than_stamp(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            pyproject = base_dir / "pyproject.toml"
            stamp = run_pipeline.install_stamp_path(base_dir)
            stamp.parent.mkdir(parents=True, exist_ok=True)
            stamp.write_text("old\n", encoding="utf-8")
            pyproject.write_text("[project]\nname='demo'\n", encoding="utf-8")
            future = stamp.stat().st_mtime + 10
            os.utime(pyproject, (future, future))
            self.assertTrue(run_pipeline.needs_dependency_install(base_dir))

    @patch("run_pipeline.subprocess.run")
    def test_creates_virtualenv_when_missing(self, mocked_run):
        mocked_run.return_value.returncode = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            (base_dir / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
            venv_python = run_pipeline.repo_venv_python(base_dir)
            with patch.object(run_pipeline, "same_interpreter", return_value=False):
                exit_code = run_pipeline.main(["--help"], base_dir=base_dir)

            create_call = mocked_run.call_args_list[0]
            self.assertIn("-m", create_call.args[0])
            self.assertIn("venv", create_call.args[0])
            self.assertEqual(exit_code, 0)
            self.assertFalse(venv_python.exists())

    @patch("run_pipeline.subprocess.run")
    def test_relaunches_with_original_args_inside_repo_venv(self, mocked_run):
        mocked_run.return_value.returncode = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            (base_dir / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
            venv_python = run_pipeline.repo_venv_python(base_dir)
            venv_python.parent.mkdir(parents=True, exist_ok=True)
            venv_python.write_text("", encoding="utf-8")
            run_pipeline.touch_install_stamp(base_dir)

            with patch.object(run_pipeline, "same_interpreter", return_value=False):
                exit_code = run_pipeline.main(["--year", "2025", "--month", "2"], base_dir=base_dir)

            relaunch_cmd = mocked_run.call_args_list[-1].args[0]
            self.assertEqual(Path(relaunch_cmd[0]).resolve(), venv_python.resolve())
            self.assertEqual(Path(relaunch_cmd[1]).resolve(), (base_dir / "run_pipeline.py").resolve())
            self.assertEqual(relaunch_cmd[2:], ["--year", "2025", "--month", "2"])
            self.assertEqual(exit_code, 0)


if __name__ == "__main__":
    unittest.main()
