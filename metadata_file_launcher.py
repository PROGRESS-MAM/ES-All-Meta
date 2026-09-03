#!/usr/bin/env python3
"""Launcher fuer metadata_file/metadata_file.py.

Zwei Betriebsarten:
  1) Flow Automation (Standard, wenn editshare_helpers verfuegbar ist):
     Der Launcher ist selbst das AutomationScript. Er liest die Flow-JSON-Eingabe,
     schreibt Messages ins Job-Log und liefert den [[BEGIN]]/[[END]]-Block zurueck.
  2) Standalone (cron / manueller Test):
     ./metadata_file_launcher.py --standalone
     Gibt Fortschritt auf stdout aus und liefert Exit-Code 0/1.

Installation auf dem Flow-Server:
  /var/flow/automation/scripts/metadata_file_launcher.py
  /var/flow/automation/scripts/metadata_file/metadata_file.py
  /var/flow/automation/scripts/metadata_file/toolbox/__init__.py
  /var/flow/automation/scripts/metadata_file/toolbox/toolbox.py
  /var/flow/automation/scripts/metadata_file/cred.env
  chmod +x metadata_file_launcher.py metadata_file/metadata_file.py
  Dateien MUESSEN UNIX-Zeilenenden (LF) haben.
  pyarrow muss im Python des Flow-Servers installiert sein.
"""

import sys
import traceback
from pathlib import Path

script_path = Path(__file__).resolve().parent
target_path = script_path / "metadata_file"

if not target_path.is_dir():
    print(f"Unterordner nicht gefunden: {target_path}", file=sys.stderr)
    sys.exit(1)

if str(target_path) not in sys.path:
    sys.path.insert(0, str(target_path))

import metadata_file

try:
    from editshare_helpers.automation import AutomationScript, main as automation_main
    flow_helpers_available = True
except ImportError:
    AutomationScript = object
    automation_main = None
    flow_helpers_available = False


class MetadataFileScript(AutomationScript):
    def process_asset(self, asset_info):
        try:
            result = metadata_file.update_metadata_file(
                message_handler=self.add_message,
                extra_args=self.extra_args(),
            )
        except Exception as exc:
            self.add_message("error", traceback.format_exc())
            self.error_and_exit(f"{metadata_file.app_name} Update fehlgeschlagen: {exc}")
            return

        self.add_message(
            "info",
            "Datei aktualisiert: {parquet_path} ({rows_written}/{clip_count} Clips, "
            "{leaf_columns} Blattspalten, {row_groups} Blockgruppen, "
            "{file_size_MB} MB)".format(**result),
        )


def run_standalone(argv):
    return metadata_file.main(argv)


if __name__ == "__main__":
    args = sys.argv[1:]
    force_standalone = "--standalone" in args

    if force_standalone:
        args = [a for a in args if a != "--standalone"]
        sys.exit(run_standalone(args))

    if flow_helpers_available:
        automation_main(MetadataFileScript)
    else:
        print("editshare_helpers nicht verfuegbar - Standalone-Modus", file=sys.stderr)
        sys.exit(run_standalone(args))