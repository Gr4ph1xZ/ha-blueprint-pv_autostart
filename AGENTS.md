# Repository Guidelines

## Project Structure & Module Organization
`blueprints/automation/pv/pv_autostart.yaml` defines the Home Assistant blueprint inputs and metadata; keep related options grouped and preserve the two-space YAML indent Home Assistant expects. The runtime logic lives in `pyscript/pv_autostart.py`, which registers the pyscript service and orchestrates surplus tracking. Place future docs or configuration examples under `docs/` (create as needed) to keep the root uncluttered.

## Build, Test & Development Commands
Use `hass --script check_config --config /path/to/homeassistant` after placing the blueprint in your Home Assistant config to catch schema issues early. While iterating on the Python module, load it into a dev instance and call `pyscript.reload` from Developer Tools to test changes without restarting. Run `git status` before and after checks so you can spot stray edits and keep diffs reviewable.

## Coding Style & Naming Conventions
Follow Home Assistant YAML style: two-space indent, lowercase entity ids, and hyphenated keys (e.g., `sensor_grid_import_components`). Python code uses 4-space indents, type hints, and module-level constants in `UPPER_SNAKE_CASE`. Prefix log statements with `[pv_autostart]` plus the automation context, mirroring existing messages. Favor descriptive helper names such as `get_numeric_state` over abbreviations; add short docstrings when logic is non-obvious.

## Testing Guidelines
Automated tests are not yet provided, so rely on scenario testing inside a Home Assistant sandbox. Capture traces via Automation > Traces after each change and store any useful reproductions under `docs/traces/` to document edge cases. Simulate a full day using recorded PV data or the Energy dashboard to confirm buffer timings before opening a pull request.

## Commit & Pull Request Guidelines
History currently shows concise, lowercase summaries (`initial commit`); continue with present-tense, 60-character summaries that describe the change, e.g., `add forecast guard to pv autostart`. Group related YAML and Python edits together. Pull requests should list the scenario exercised, Home Assistant version, relevant trace screenshots, and link any issues or forum threads that motivated the work.
