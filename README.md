# PV Autostart Blueprint

## Overview
This repository contains a Home Assistant blueprint and companion Pyscript module that coordinate starting and stopping a single appliance based on photovoltaic (PV) surplus. The automation watches PV generation, grid import, and (optionally) device load sensors to decide when to run the appliance, while also honouring daily runtime and cycle targets.

## Layout
- `blueprints/automation/pv/pv_autostart.yaml` – blueprint definition invoked from Home Assistant. It wires UI inputs to the pyscript service.
- `pyscript/pv_autostart.py` – runtime logic registered as `pyscript.pv_autostart`, including surplus detection, hysteresis tracking, manual-start handling, and scheduled start/stop decisions.

## Getting Started
1. Copy the blueprint into your Home Assistant `blueprints/` folder and import it from Settings → Automations → Blueprints.
2. Place `pyscript/pv_autostart.py` under your Home Assistant `pyscript/` directory and reload Pyscript (Developer Tools → Services → `pyscript.reload`).
3. Create an automation from the blueprint and fill in the required inputs: the appliance switch, PV production sensor, grid import sensors, and buffer timings. Optional sensors (device power, forecast) unlock additional behaviours.

## Configuration Highlights
- **Buffer windows** (`buffer_on_minutes`, `buffer_off_minutes`) prevent rapid toggling by requiring surplus/deficit conditions to persist.
- **Fallback start** (`start_time_if_target_not_met`) and **scheduled stop** (`scheduled_turn_off_time`) act as bookends when daily targets are not yet satisfied. Forced turn-off can override unmet targets via `force_turn_off_if_target_unreached`.
- **Manual counting** (`exclude_blueprint_turn_on_from_counters`) determines whether manual device starts increment daily runtime/cycle counters. Blueprint-triggered starts always count.
- **Interruptible loads** allow early stop when device draw exceeds grid import during deficit periods.

## Logging & Observability
The pyscript uses structured log levels: `DEBUG` records decision details (surplus calculations, manual start detection, turn-on/off reasons), `INFO` captures lifecycle milestones (module load, instance registration, device state changes), `WARNING` flags recoverable anomalies, and `ERROR` reports failures requiring attention. Use Home Assistant's Logbook or `pyscript.pv_autostart_dump` service to inspect real-time counters.

## Contributing
When altering logic, ensure new behaviours remain deterministic across the 30-second evaluation tick and respect existing hysteresis counters. Update both the blueprint input descriptions and service docstrings when adding parameters, and keep manual/manual-count semantics aligned between YAML and Python. Consider capturing traces for edge cases (e.g., intermittent PV, manual overrides) under `docs/` to aid future contributors.
