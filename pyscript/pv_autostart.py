"""
PV Autostart
===============

This pyscript module provides a lightweight controller for automatically
starting and stopping a single appliance based on available photovoltaic
surplus power.  It takes a set of Home‑Assistant entities as inputs
and encapsulates the decision logic in a per‑automation class instance.

The implementation follows the architectural patterns of the existing
``pv_excess_control`` script: there is an instance registry keyed by
``automation_id``, helper functions for reading states and numbers from
entities, defensive validation, robust error handling, periodic
evaluation of conditions via ``@time_trigger`` and a daily reset of
counters at midnight.  The intent is to provide a clean separation
between Home‑Assistant YAML configuration (via a blueprint) and the
runtime logic executed by pyscript.

Key features of the controller include:

* Hysteresis around starting and stopping to avoid rapid toggling.  The
  ``buffer_on_minutes`` and ``buffer_off_minutes`` inputs determine how
  long an observed condition must persist before a decision is made.
* Automatic start when PV surplus has been positive for the configured
  buffer.  This can be overridden by a user‑defined fallback start
  time (``start_time_if_target_not_met``) if daily targets have not
  yet been met.
* Multiple stop conditions: no power draw, import from grid while the
  device is interruptible, or prolonged lack of PV surplus.  Each
  condition respects the configured buffers and honours minimum daily
  runtime and cycle targets.
* Daily run‑time and cycle counters with a reset at midnight.  Only
  runs initiated by this script contribute to the counters.
* Optional forecast integration: if a forecast of remaining solar
  generation (in kWh) is provided, the controller will delay or skip
  the forced start time when it predicts that enough energy will be
  available later in the day to meet the configured targets.  When
  forecasted energy is insufficient, the controller will honour the
  fallback start time.

All log messages include a prefix derived from the switch entity and
automation id to aid troubleshooting when multiple automations are
running concurrently.  Comments and logging are written in English.

"""

from typing import List, Optional, Dict, Any, Union, Tuple
import datetime

# Prefix used for all log messages originating from this module.  Using a
# constant prefix makes it easy to filter and search for messages related to
# this blueprint.  The prefix is prepended to the per‑instance log prefix
# defined on each PvAutostart instance.
MODULE_PREFIX = "[pv_autostart]"

log.info(f"{MODULE_PREFIX} module loaded: registering service and waiting for first call")



# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def _get_state(entity_id: str) -> Optional[str]:
    """
    Return the current state of an entity.  For climate domains a number of
    operational modes are normalised to ``on``/``off``.  Any error while
    retrieving the state is logged and None is returned.

    :param entity_id: Home‑Assistant entity id
    :return: The state as a string, or None on error
    """
    domain = entity_id.split('.')[0]
    try:
        entity_state = state.get(entity_id)
    except Exception as e:
        log.error(f"{MODULE_PREFIX} Could not get state from entity {entity_id}: {e}")
        return None
    if domain == 'climate':
        if isinstance(entity_state, str) and entity_state.lower() in ['heat', 'cool', 'boost', 'on']:
            return 'on'
        elif entity_state == 'off':
            return 'off'
        else:
            log.error(f"{MODULE_PREFIX} Entity state not supported for climate domain: {entity_state}")
            return None
    return entity_state


def _validate_number(num: Union[str, float, int], return_on_error: Optional[float] = None) -> Optional[float]:
    """
    Validate that a value can be converted into a float within a sensible
    range.  If conversion fails or the value is outside the range
    ``[-1_000_000, 1_000_000]`` the provided fallback is returned.

    :param num: The value to validate
    :param return_on_error: Value to return when validation fails
    :return: A float or the fallback
    """
    if num is None or num == 'unavailable':
        return return_on_error
    try:
        f = float(num)
    except Exception as e:
        log.error(f"{MODULE_PREFIX} {num=} is not a valid number: {e}")
        return return_on_error
    if -1000000 <= f <= 1000000:
        return f
    log.error(f"{MODULE_PREFIX} {f} is outside the valid range [-1000000, 1000000]")
    return return_on_error


def _get_num_state(entity_id: str, return_on_error: Optional[float] = None) -> Optional[float]:
    """
    Convenience wrapper around ``_get_state`` and ``_validate_number`` to
    retrieve the numeric state of an entity.

    :param entity_id: Home‑Assistant entity id
    :param return_on_error: Value to return on failure
    :return: A float or the fallback
    """
    return _validate_number(_get_state(entity_id), return_on_error)


def _turn_on(entity_id: str) -> bool:
    """
    Issue a service call to turn an entity on.  If the service does
    not exist or the call fails, a log entry is written and False is
    returned.

    :param entity_id: Home‑Assistant entity id
    :return: True on success, False otherwise
    """
    domain = entity_id.split('.')[0]
    if not service.has_service(domain, 'turn_on'):
        log.error(f"{MODULE_PREFIX} Cannot switch on {entity_id}: service '{domain}.turn_on' does not exist.")
        return False
    try:
        service.call(domain, 'turn_on', entity_id=entity_id)
    except Exception as e:
        log.error(f"{MODULE_PREFIX} Cannot switch on {entity_id}: {e}")
        return False
    return True


def _turn_off(entity_id: str) -> bool:
    """
    Issue a service call to turn an entity off.  If the service does
    not exist or the call fails, a log entry is written and False is
    returned.

    :param entity_id: Home‑Assistant entity id
    :return: True on success, False otherwise
    """
    domain = entity_id.split('.')[0]
    if not service.has_service(domain, 'turn_off'):
        log.error(f"{MODULE_PREFIX} Cannot switch off {entity_id}: service '{domain}.turn_off' does not exist.")
        return False
    try:
        service.call(domain, 'turn_off', entity_id=entity_id)
    except Exception as e:
        log.error(f"{MODULE_PREFIX} Cannot switch off {entity_id}: {e}")
        return False
    return True


def _replace_vowels(input_str: str) -> str:
    """
    Replace German umlaut vowels with their ASCII equivalents. This is
    primarily used to normalise automation identifiers.
    """
    vowel_replacement = {'ä': 'a', 'ö': 'o', 'ü': 'u', 'Ä': 'A', 'Ö': 'O', 'Ü': 'U'}
    # Avoid generator and list comprehensions for older pyscript AST
    res = ""
    for ch in input_str:
        repl = vowel_replacement.get(ch, ch)
        res = res + repl
    return res

def _normalise_id(text: str) -> str:
    """
    Turn arbitrary text into a clean automation id. If it already starts with
    'automation.', keep that but strip it before normalizing to avoid duplicates.
    """
    raw = (text or "").strip()
    if raw.startswith("automation."):
        raw = raw[len("automation."):]
    # normalize: umlauts, spaces, dots
    raw = _replace_vowels(raw).replace(" ", "_").replace(".", "_").lower()
    return f"automation.{raw}"



def _sum_positive_states(entities: List[str]) -> float:
    """
    Sum the positive numeric states of a list of sensor entities.  Any
    unavailable or invalid values are treated as zero.  Only positive
    numbers contribute to the sum; negative numbers are ignored.

    :param entities: List of sensor entity ids
    :return: Sum of positive numeric states
    """
    total = 0.0
    for ent in entities:
        val = _get_num_state(ent, return_on_error=0)
        if val is None:
            continue
        if val > 0:
            total += val
    return total


# -----------------------------------------------------------------------------
# Main class
# -----------------------------------------------------------------------------
class PvAutostart:
    """
    A class encapsulating the state machine for a single PV autostart
    automation.  Each instance corresponds to one Home‑Assistant
    automation (identified by ``automation_id``) and manages a single
    device (``switch_entity``).  Instances are registered in the class
    attribute ``instances`` so that they can be accessed globally for
    resetting and updating.
    """

    # Global registry of automation instances keyed by automation id
    instances: Dict[str, Dict[str, Any]] = {}
    # Interval in seconds at which the decision logic runs.  A shorter
    # interval yields a more responsive system but increases overhead.
    TICK_INTERVAL_SECONDS: int = 30

    def __init__(
        self,
        automation_id: str,
        switch_entity: str,
        sensor_device_power: Optional[str],
        off_after_minutes_without_draw: Optional[float],
        no_draw_threshold_watts: float,
        buffer_on_minutes: float,
        buffer_off_minutes: float,
        min_runtime_per_day_minutes: Optional[float],
        min_cycles_per_day: Optional[int],
        interruptible: bool,
        sensor_grid_import_components: List[str],
        start_time_if_target_not_met: datetime.time,
        sensor_forecast_energy_today: Optional[str],
        sensor_pv_power_now: str,
        exclude_blueprint_turn_on_from_counters: bool = False,
        turn_off_time: Optional[datetime.time] = None,
        force_turn_off_if_target_unreached: bool = False,
    ):
        self._hb = 0  # heartbeat counter

        # Basic identifiers
        self.automation_id: str = automation_id
        self.switch_entity: str = switch_entity
        # Prepend the module prefix to each instance specific prefix.  This makes
        # it easy to grep for messages related to this blueprint while still
        # retaining context about the switch and automation id.
        self.log_prefix: str = f"{MODULE_PREFIX} [{self.switch_entity} {self.automation_id}]"

        # Configuration parameters
        self.sensor_device_power: Optional[str] = sensor_device_power or None
        self.off_after_minutes_without_draw: Optional[float] = (
            float(off_after_minutes_without_draw)
            if off_after_minutes_without_draw not in [None, '']
            else None
        )
        self.no_draw_threshold_watts: float = float(no_draw_threshold_watts)
        self.buffer_on_minutes: float = max(0.0, float(buffer_on_minutes))
        self.buffer_off_minutes: float = max(0.0, float(buffer_off_minutes))
        self.min_runtime_per_day_minutes: Optional[float] = (
            float(min_runtime_per_day_minutes)
            if min_runtime_per_day_minutes not in [None, '']
            else None
        )
        self.min_cycles_per_day: Optional[int] = (
            int(min_cycles_per_day)
            if min_cycles_per_day not in [None, '']
            else None
        )
        self.interruptible: bool = bool(interruptible)
        self.sensor_grid_import_components: List[str] = sensor_grid_import_components or []
        self.start_time_if_target_not_met: datetime.time = start_time_if_target_not_met
        self.sensor_forecast_energy_today: Optional[str] = sensor_forecast_energy_today or None
        self.sensor_pv_power_now: str = sensor_pv_power_now

        # Extended configuration
        # When true, manual device starts will not increment the cycle or
        # run-time counters; blueprint-triggered starts always count.
        # Use this to prevent ad-hoc/manual toggles from skewing targets.
        # Default is False to preserve previous behaviour.
        self.exclude_manual_starts_from_counters: bool = bool(
            exclude_blueprint_turn_on_from_counters
        )
        # Optional time of day after which the device should be turned off.
        # If None, no scheduled turn off will occur.  If provided as a
        # datetime.time, the device will attempt to turn off at or after
        # this time, subject to the minimum runtime/cycle targets unless
        # force_turn_off_if_target_unreached is true.
        self.turn_off_time: Optional[datetime.time] = turn_off_time
        # If true, the device will be turned off at the scheduled turn off
        # time even if the configured daily runtime or cycle targets have
        # not yet been met.  Defaults to False to preserve prior logic.
        self.force_turn_off_if_target_unreached: bool = bool(
            force_turn_off_if_target_unreached
        )

        # Runtime state variables
        self.running_since: Optional[datetime.datetime] = None  # Timestamp when the current run began
        self.started_by_script: bool = False  # True if the current run was initiated by this blueprint
        self.current_run_counts: bool = False  # True when the current run contributes to runtime/cycle counters
        self.daily_run_time_sec: float = 0.0  # Accumulated run time in seconds for today (counted runs)
        self.cycles_today: int = 0  # Count of cycles credited to today's targets

        # Hysteresis counters (seconds)
        self.on_counter_sec: float = 0.0  # Surplus positive duration
        self.no_draw_counter_sec: float = 0.0  # Duration of no load
        self.import_off_counter_sec: float = 0.0  # Duration where device power > grid import
        self.pv_deficit_counter_sec: float = 0.0  # Duration where PV surplus <= 0 while device is running

        # Register periodic triggers only once per instance
        self._setup_triggers()
        log.info(f"{self.log_prefix} Registered instance; ticker every {self.TICK_INTERVAL_SECONDS}s")

    def _setup_triggers(self) -> None:
        """
        Create the periodic time trigger that evaluates this instance.  The
        trigger references ``self`` via closure and must therefore be defined
        inside this method.  Calling this method multiple times for the same
        instance will create multiple triggers, so it should only be
        invoked in the constructor.
        """
        interval = self.TICK_INTERVAL_SECONDS

        @time_trigger(f"period(now, {interval}s)")
        def _on_tick():
            # Ensure the instance still exists and is registered
            inst_entry = PvAutostart.instances.get(self.automation_id)
            if inst_entry is None or inst_entry.get('instance') is not self:
                return _on_tick
            try:
                self._tick()
            except Exception as e:
                log.error(f"{self.log_prefix} Exception in tick: {e}")
            return _on_tick

    # ----------------------------------------------------------------------
    # Public interface
    # ----------------------------------------------------------------------
    def update_params(
        self,
        switch_entity: str,
        sensor_device_power: Optional[str],
        off_after_minutes_without_draw: Optional[float],
        no_draw_threshold_watts: float,
        buffer_on_minutes: float,
        buffer_off_minutes: float,
        min_runtime_per_day_minutes: Optional[float],
        min_cycles_per_day: Optional[int],
        interruptible: bool,
        sensor_grid_import_components: List[str],
        start_time_if_target_not_met: datetime.time,
        sensor_forecast_energy_today: Optional[str],
        sensor_pv_power_now: str,
        exclude_blueprint_turn_on_from_counters: Optional[bool] = None,
        turn_off_time: Optional[datetime.time] = None,
        force_turn_off_if_target_unreached: Optional[bool] = None,
    ) -> None:
        """
        Update the configuration parameters of this instance.  This allows
        reloading of automation settings from the blueprint without
        recreating the instance.  Any running timers and counters remain
        unaffected, but decisions will use the updated values from the
        next tick onwards.
        """
        # Only log and assign if values actually change
        if self.switch_entity != switch_entity:
            log.info(f"{self.log_prefix} Updated switch entity: {self.switch_entity} -> {switch_entity}")
            self.switch_entity = switch_entity
        self.sensor_device_power = sensor_device_power or None
        self.off_after_minutes_without_draw = (
            float(off_after_minutes_without_draw)
            if off_after_minutes_without_draw not in [None, '']
            else None
        )
        self.no_draw_threshold_watts = float(no_draw_threshold_watts)
        self.buffer_on_minutes = max(0.0, float(buffer_on_minutes))
        self.buffer_off_minutes = max(0.0, float(buffer_off_minutes))
        self.min_runtime_per_day_minutes = (
            float(min_runtime_per_day_minutes)
            if min_runtime_per_day_minutes not in [None, '']
            else None
        )
        self.min_cycles_per_day = (
            int(min_cycles_per_day)
            if min_cycles_per_day not in [None, '']
            else None
        )
        self.interruptible = bool(interruptible)
        self.sensor_grid_import_components = sensor_grid_import_components or []
        self.start_time_if_target_not_met = start_time_if_target_not_met
        self.sensor_forecast_energy_today = sensor_forecast_energy_today or None
        self.sensor_pv_power_now = sensor_pv_power_now

        # Extended parameters
        if exclude_blueprint_turn_on_from_counters is not None:
            self.exclude_manual_starts_from_counters = bool(
                exclude_blueprint_turn_on_from_counters
            )
        # Only update the turn off time if explicitly provided; this prevents
        # overwriting an existing time with None when a parameter is omitted.
        if turn_off_time is not None:
            self.turn_off_time = turn_off_time
        if force_turn_off_if_target_unreached is not None:
            self.force_turn_off_if_target_unreached = bool(
                force_turn_off_if_target_unreached
            )

    # ----------------------------------------------------------------------
    # Core logic
    # ----------------------------------------------------------------------
    def _tick(self) -> None:
        """
        Periodic evaluation executed on every tick.  It reads the current
        states of relevant sensors and decides whether to turn the device
        on or off.  The method updates hysteresis counters, run‑time and
        cycle counters, and logs its reasoning.
        """
        now = datetime.datetime.now()
        state_now = _get_state(self.switch_entity)
        if state_now is None:
            log.error(f"{self.log_prefix} Unable to evaluate because state is None.")
            return

        # Compute PV surplus and grid import for this tick
        pv_power = _get_num_state(self.sensor_pv_power_now, return_on_error=0) or 0.0
        grid_import = _sum_positive_states(self.sensor_grid_import_components)
        pv_surplus = max(0.0, pv_power - grid_import)

        # Read device power if available
        device_power = None
        if self.sensor_device_power:
            device_power = _get_num_state(self.sensor_device_power, return_on_error=None)

        # Format some values for logging
        debug_vals = (
            f"pv_power={pv_power:.1f}W, grid_import={grid_import:.1f}W, "
            f"pv_surplus={pv_surplus:.1f}W"
        )

        # Device currently ON
        if state_now == 'on':
            # If we didn't start the device but it's running, record start time
            if self.running_since is None:
                self.running_since = now
                self.started_by_script = False
                if self.exclude_manual_starts_from_counters:
                    self.current_run_counts = False
                    log.debug(f"{self.log_prefix} manual start detected; counters excluded.")
                else:
                    self.current_run_counts = True
                    self.cycles_today += 1
                    log.debug(
                        f"{self.log_prefix} manual start detected; counting towards targets (cycle {self.cycles_today})."
                    )

            # Update counters
            # 1. No draw counter
            if device_power is not None and self.off_after_minutes_without_draw is not None:
                if device_power <= self.no_draw_threshold_watts:
                    self.no_draw_counter_sec += self.TICK_INTERVAL_SECONDS
                else:
                    self.no_draw_counter_sec = 0.0
            else:
                # If no sensor or off_after is not configured, reset counter
                self.no_draw_counter_sec = 0.0

            # 2. Import off counter
            if self.interruptible and device_power is not None and grid_import >= 0:
                # Turn off if device draws more than imported power
                # Only accumulate when both values are valid
                if device_power > grid_import:
                    self.import_off_counter_sec += self.TICK_INTERVAL_SECONDS
                else:
                    self.import_off_counter_sec = 0.0
            else:
                self.import_off_counter_sec = 0.0

            # 3. PV deficit counter (lack of surplus)
            if pv_surplus <= 0.0:
                self.pv_deficit_counter_sec += self.TICK_INTERVAL_SECONDS
            else:
                self.pv_deficit_counter_sec = 0.0

            # Evaluate off conditions in order of priority
            # a) No draw condition
            if self.should_turn_off_for_no_draw():
                # Only allow turning off for no draw after min cycles reached (handled in function)
                log.debug(f"{self.log_prefix} turn-off reason=no_draw; {debug_vals}")
                self._turn_device_off(now)
                return

            # b) Import condition
            if self.should_turn_off_for_import(device_power, grid_import):
                log.debug(
                    f"{self.log_prefix} turn-off reason=grid_import; {debug_vals}, "
                    f"device_power={device_power}, grid_import={grid_import}"
                )
                self._turn_device_off(now)
                return

            # c) PV deficit / general condition
            if self.should_turn_off_for_pv_shortage():
                log.debug(f"{self.log_prefix} turn-off reason=pv_deficit; {debug_vals}")
                self._turn_device_off(now)
                return

            # d) Scheduled turn off condition
            if self.should_turn_off_due_to_schedule(now):
                log.debug(f"{self.log_prefix} turn-off reason=scheduled_off; {debug_vals}")
                self._turn_device_off(now)
                return

            # Otherwise keep running
            log.debug(
                f"{self.log_prefix} Device ON; counters: no_draw={self.no_draw_counter_sec}s, "
                f"import_off={self.import_off_counter_sec}s, pv_deficit={self.pv_deficit_counter_sec}s; "
                f"{debug_vals}"
            )
            self._hb += 1
            if self._hb % 10 == 0:  # alle 10 Ticks (= ~5 min bei 30s-Intervall)
                log.debug(
                    f"{self.log_prefix} heartbeat: runtime={self.daily_run_time_sec / 60:.1f} min, cycles={self.cycles_today}"
                )

            return

        # Device currently OFF
        elif state_now == 'off':
            # Reset counters that only apply when device is running
            self.no_draw_counter_sec = 0.0
            self.import_off_counter_sec = 0.0
            self.pv_deficit_counter_sec = 0.0

            # Update PV surplus on counter
            if pv_surplus > 0:
                self.on_counter_sec += self.TICK_INTERVAL_SECONDS
            else:
                self.on_counter_sec = 0.0

            # Check whether we can turn on due to PV surplus
            if self.can_turn_on_by_surplus():
                log.debug(f"{self.log_prefix} turn-on reason=surplus; {debug_vals}")
                self._turn_device_on(now)
                return

            # If not turned on by surplus, check if we need to force run for targets
            if self.need_to_force_run_for_targets(now):
                log.debug(f"{self.log_prefix} turn-on reason=force_targets; {debug_vals}")
                self._turn_device_on(now)
                return

            # Otherwise remain off
            log.debug(f"{self.log_prefix} Device OFF; surplus_on_counter={self.on_counter_sec}s; {debug_vals}")
            return

        else:
            # Unexpected state; log and treat as off
            log.warning(f"{self.log_prefix} Unexpected device state: {state_now}; treating as off.")
            # Reset counters when state is unknown
            self.no_draw_counter_sec = 0.0
            self.import_off_counter_sec = 0.0
            self.pv_deficit_counter_sec = 0.0
            self.on_counter_sec = 0.0

    # ----------------------------------------------------------------------
    # Device control helpers
    # ----------------------------------------------------------------------
    def _turn_device_on(self, now: datetime.datetime) -> None:
        """
        Turn the controlled device on and update run‑time tracking.  If the
        service call is successful the script marks that the device was
        started by this script and increments the cycle counter.
        """
        if _turn_on(self.switch_entity):
            # Mark the time when the device was started so that run duration can be calculated on shut-down.
            self.running_since = now
            self.started_by_script = True
            self.current_run_counts = True
            self.cycles_today += 1
            # Reset the surplus counter so that subsequent on decisions require the configured buffer again.
            self.on_counter_sec = 0.0
            log.info(f"{self.log_prefix} Device switched ON (cycle {self.cycles_today}).")

    def _turn_device_off(self, now: datetime.datetime) -> None:
        """
        Turn the controlled device off and update run-time tracking.  Only
        runs marked to count (blueprint starts or manual starts when enabled)
        contribute to the daily run time and cycle counters.
        """
        if _turn_off(self.switch_entity):
            if self.current_run_counts and self.running_since is not None:
                run_duration_sec = (now - self.running_since).total_seconds()
                self.daily_run_time_sec += run_duration_sec
                log.debug(
                    f"{self.log_prefix} credited {run_duration_sec/60:.1f} minutes; "
                    f"total={self.daily_run_time_sec/60:.1f} min"
                )
            self.running_since = None
            self.started_by_script = False
            self.current_run_counts = False
            # Reset counters when turning off
            self.no_draw_counter_sec = 0.0
            self.import_off_counter_sec = 0.0
            self.pv_deficit_counter_sec = 0.0
            self.on_counter_sec = 0.0
            log.info(f"{self.log_prefix} Device switched OFF.")

    # ----------------------------------------------------------------------
    # Decision helpers
    # ----------------------------------------------------------------------
    def can_turn_on_by_surplus(self) -> bool:
        """
        Determine whether the device should be switched on based on PV
        surplus.  Returns True only if the surplus has been positive
        continuously for at least ``buffer_on_minutes``.
        """
        required_sec = self.buffer_on_minutes * 60.0
        return self.on_counter_sec >= required_sec and self.buffer_on_minutes > 0

    def should_turn_off_for_no_draw(self) -> bool:
        """
        Check if the device should be switched off because it is drawing no
        power for an extended period.  This condition is only evaluated if
        ``sensor_device_power`` and ``off_after_minutes_without_draw`` are
        configured.  When ``min_cycles_per_day`` is set, the device will not
        be turned off due to no draw until the cycle target is met.
        """
        if self.sensor_device_power is None or self.off_after_minutes_without_draw is None:
            return False
        # If a cycle target is defined and not yet met, do not turn off
        if self.min_cycles_per_day is not None and self.cycles_today < self.min_cycles_per_day:
            return False
        required_sec = self.off_after_minutes_without_draw * 60.0
        return self.no_draw_counter_sec >= required_sec

    def should_turn_off_for_import(self, device_power: Optional[float], grid_import: float) -> bool:
        """
        Check if the device should be switched off because it is drawing
        power from the grid while marked as interruptible.  The device power
        must be greater than the grid import for the configured buffer
        duration.  Only evaluated when ``interruptible`` is True and both
        values are available.

        :param device_power: Current device power in watts (may be None)
        :param grid_import: Total grid import in watts (>=0)
        :return: True if the device should be turned off
        """
        if not self.interruptible:
            return False
        if device_power is None:
            return False
        # When buffer is zero we should never use this condition
        if self.buffer_off_minutes <= 0:
            return False
        required_sec = self.buffer_off_minutes * 60.0
        return self.import_off_counter_sec >= required_sec

    def should_turn_off_for_pv_shortage(self) -> bool:
        """
        Determine whether to turn off the device due to prolonged lack of
        PV surplus.  This condition is only considered if turning off will
        not prevent meeting the configured daily targets.
        """
        if self.buffer_off_minutes <= 0:
            return False
        # Do not turn off if runtime target or cycle target still needs to be met
        if self.min_runtime_per_day_minutes is not None:
            if (self.daily_run_time_sec / 60.0) < self.min_runtime_per_day_minutes:
                return False
        if self.min_cycles_per_day is not None:
            if self.cycles_today < self.min_cycles_per_day:
                return False
        required_sec = self.buffer_off_minutes * 60.0
        return self.pv_deficit_counter_sec >= required_sec

    def should_turn_off_due_to_schedule(self, now: datetime.datetime) -> bool:
        """
        Determine whether the device should be switched off because the
        configured turn_off_time has been reached.  When a turn‑off time
        is defined, the device will be turned off at or after that time if
        either (1) the daily runtime and cycle targets have been met or
        (2) the force_turn_off_if_target_unreached flag is True.  If no
        turn_off_time is defined, this method always returns False.

        :param now: Current datetime
        :return: True if the device should be turned off according to schedule
        """
        if self.turn_off_time is None:
            return False
        # Construct today's datetime at the scheduled off time.  If the off
        # time is earlier than now's time and we crossed midnight, we still
        # combine with today's date; the off condition will become true on
        # the next midnight tick.  This keeps behaviour consistent across
        # days.
        try:
            off_dt = datetime.datetime.combine(now.date(), self.turn_off_time)
        except Exception:
            # If time construction fails, do not schedule off
            return False
        if now < off_dt:
            return False
        # At or after the scheduled turn off time.  If targets are unmet and
        # the force flag is not set, we defer the off.
        if not self.force_turn_off_if_target_unreached:
            # Check runtime target
            if self.min_runtime_per_day_minutes is not None:
                if (self.daily_run_time_sec / 60.0) < self.min_runtime_per_day_minutes:
                    return False
            # Check cycle target
            if self.min_cycles_per_day is not None:
                if self.cycles_today < self.min_cycles_per_day:
                    return False
        return True

    def need_to_force_run_for_targets(self, now: datetime.datetime) -> bool:
        """
        Evaluate whether to force start the device in order to achieve the
        configured daily run‑time or cycle targets.  This method consults
        the optional forecast sensor: if the forecasted remaining PV energy
        appears sufficient to meet the targets, the forced start is
        postponed.  Otherwise the start is scheduled at or after
        ``start_time_if_target_not_met``.

        :param now: The current datetime
        :return: True if the device should be started immediately
        """
        # Determine if any targets are configured and unmet
        runtime_target_unmet = False
        cycles_target_unmet = False
        remaining_runtime_min = 0.0
        # Run‑time target
        if self.min_runtime_per_day_minutes is not None:
            current_runtime_min = self.daily_run_time_sec / 60.0
            if current_runtime_min < self.min_runtime_per_day_minutes:
                runtime_target_unmet = True
                remaining_runtime_min = max(0.0, self.min_runtime_per_day_minutes - current_runtime_min)
        # Cycle target
        if self.min_cycles_per_day is not None:
            if self.cycles_today < self.min_cycles_per_day:
                cycles_target_unmet = True
        if not runtime_target_unmet and not cycles_target_unmet:
            return False  # All targets already met

        # If a forecast sensor is provided, evaluate remaining energy
        if self.sensor_forecast_energy_today:
            forecast_kwh = _get_num_state(self.sensor_forecast_energy_today, return_on_error=None)
            if forecast_kwh is not None:
                # Estimate remaining energy required based on device power and remaining runtime
                energy_needed_kwh = 0.0
                if runtime_target_unmet and self.sensor_device_power:
                    current_power = _get_num_state(self.sensor_device_power, return_on_error=None)
                    if current_power is not None and current_power > 0:
                        # Convert power [W] * time [h]
                        energy_needed_kwh = (current_power / 1000.0) * (remaining_runtime_min / 60.0)
                # If forecasted energy is greater than or equal to required energy, postpone forced start
                if forecast_kwh >= energy_needed_kwh:
                    # There is enough forecast energy – only enforce start time if forecast energy becomes insufficient
                    log.debug(f"{self.log_prefix} Forecast {forecast_kwh:.2f} kWh >= needed {energy_needed_kwh:.2f} kWh; "
                              f"postponing forced start.")
                    # However, if cycles target is unmet but no runtime target exists, there is no energy requirement;
                    # therefore we only postpone if forecast is strictly positive, otherwise we proceed to time check below.
                    if energy_needed_kwh > 0 or forecast_kwh > 0:
                        return False
        # Evaluate start time
        # Construct today's datetime for start_time
        try:
            start_time = self.start_time_if_target_not_met
            start_datetime = datetime.datetime.combine(now.date(), start_time)
        except Exception:
            # If parsing fails, default to midnight
            start_datetime = datetime.datetime.combine(now.date(), datetime.time(0, 0))
        if now >= start_datetime:
            return True
        return False


# -----------------------------------------------------------------------------
# Daily reset of counters for all autostart instances
# -----------------------------------------------------------------------------
@time_trigger("cron(0 0 * * *)")
def reset_pv_autostart_counters():
    """
    Reset daily counters for all registered PvAutostart instances at midnight.
    The run‑time counter, cycle counter and all hysteresis timers are
    cleared.  This function runs once per day and ensures that each
    automation starts with a clean slate.
    """
    log.info(f"{MODULE_PREFIX} Resetting PvAutostart counters for all instances at midnight.")
    for entry in list(PvAutostart.instances.values()):
        inst: PvAutostart = entry.get('instance')
        if inst:
            inst.daily_run_time_sec = 0.0
            inst.cycles_today = 0
            inst.running_since = None
            inst.started_by_script = False
            inst.current_run_counts = False
            inst.on_counter_sec = 0.0
            inst.no_draw_counter_sec = 0.0
            inst.import_off_counter_sec = 0.0
            inst.pv_deficit_counter_sec = 0.0
            log.info(f"{inst.log_prefix} Counters reset.")


# -----------------------------------------------------------------------------
# Service entry point
# -----------------------------------------------------------------------------
@service
def pv_autostart(
    automation_id: Optional[str] = None,
    switch_entity: Optional[str] = None,
    sensor_device_power: Optional[str] = None,
    off_after_minutes_without_draw: Optional[float] = None,
    no_draw_threshold_watts: Optional[float] = None,
    buffer_on_minutes: Optional[float] = None,
    buffer_off_minutes: Optional[float] = None,
    min_runtime_per_day_minutes: Optional[float] = None,
    min_cycles_per_day: Optional[int] = None,
    interruptible: Optional[bool] = None,
    sensor_grid_import_components: Optional[List[str]] = None,
    start_time_if_target_not_met: Optional[str] = None,
    sensor_forecast_energy_today: Optional[str] = None,
    sensor_pv_power_now: Optional[str] = None,
    exclude_blueprint_turn_on_from_counters: Optional[bool] = None,
    scheduled_turn_off_time: Optional[str] = None,
    force_turn_off_if_target_unreached: Optional[bool] = None,
) -> None:
    """
    Register or update a PV autostart automation.  When invoked from the
    blueprint this service will either create a new controller instance
    associated with ``automation_id`` or update an existing one with
    changed parameters.  The parameters correspond directly to the
    blueprint inputs.

    :param automation_id: Unique automation id (will be normalised)
    :param switch_entity: Device entity to control (e.g. ``switch.pool_pump``)
    :param sensor_device_power: Sensor for current device power in watts
    :param off_after_minutes_without_draw: Minutes to wait before switching off when no draw
    :param no_draw_threshold_watts: Threshold in watts below which the device is considered idle
    :param buffer_on_minutes: Surplus buffer time before switching on
    :param buffer_off_minutes: Deficit buffer time before switching off
    :param min_runtime_per_day_minutes: Minimum run time per day in minutes
    :param min_cycles_per_day: Minimum number of cycles per day
    :param interruptible: Whether the device may be interrupted mid cycle
    :param sensor_grid_import_components: List of sensors measuring grid import power
    :param start_time_if_target_not_met: Fallback autostart time as string (HH:MM)
    :param sensor_forecast_energy_today: Sensor for remaining forecast PV energy in kWh
    :param sensor_pv_power_now: Sensor for current PV production in watts
    :param exclude_blueprint_turn_on_from_counters: When true, manual starts will be ignored by the daily counters; blueprint starts always count. Default is False.
    :param scheduled_turn_off_time: Optional time of day (``HH:MM``) after which the device should be switched off,
        mirroring the fallback start behaviour.  Leave blank to disable the scheduled turn off.
    :param force_turn_off_if_target_unreached: When true, the device will be turned off at the scheduled turn off
        time even if the configured minimum runtime or cycle targets have not yet been met.  Default is False.
    """
    log.info(
        f"{MODULE_PREFIX} pv_autostart service called with: "
        f"automation_id={automation_id}, switch={switch_entity}, pv={sensor_pv_power_now}, "
        f"import_sensors={sensor_grid_import_components}, buffers(on/off)={buffer_on_minutes}/{buffer_off_minutes} min, "
        f"exclude_from_counters={exclude_blueprint_turn_on_from_counters}, scheduled_turn_off_time={scheduled_turn_off_time}, "
        f"force_turn_off_if_target_unreached={force_turn_off_if_target_unreached}"
    )

    # Basic validation and normalisation of inputs
    if switch_entity is None:
        log.error(f"{MODULE_PREFIX} pv_autostart service call missing required parameter 'switch_entity'")
        return
    if no_draw_threshold_watts is None:
        log.error(f"{MODULE_PREFIX} pv_autostart service call missing required parameter 'no_draw_threshold_watts'")
        return
    if buffer_on_minutes is None or buffer_off_minutes is None:
        log.error(f"{MODULE_PREFIX} pv_autostart service call missing required buffer_on_minutes or buffer_off_minutes")
        return
    if interruptible is None:
        interruptible = False

    # Normalise extended booleans
    exclude_flag = bool(exclude_blueprint_turn_on_from_counters) if exclude_blueprint_turn_on_from_counters is not None else False
    force_turn_off_flag = bool(force_turn_off_if_target_unreached) if force_turn_off_if_target_unreached is not None else False
    # Parse scheduled turn-off time (mirrors fallback start behaviour).  Accepts ``HH:MM`` or ``HH:MM:SS``.
    turn_off_time: Optional[datetime.time] = None
    if scheduled_turn_off_time is not None:
        s = str(scheduled_turn_off_time).strip()
        if s:
            try:
                parts = s.split(':')
                if len(parts) == 2:
                    hour, minute = map(int, parts)
                    turn_off_time = datetime.time(hour=hour, minute=minute)
                elif len(parts) == 3:
                    hour, minute, second = map(int, parts)
                    turn_off_time = datetime.time(hour=hour, minute=minute, second=second)
                else:
                    raise ValueError
            except Exception:
                log.error(
                    f"{MODULE_PREFIX} Invalid scheduled_turn_off_time: {scheduled_turn_off_time}; disabling scheduled turn off"
                )
                turn_off_time = None
    # Normalise automation id
    if automation_id is None:
        # Derive id from switch_entity if not provided
        automation_id = _normalise_id(switch_entity)
    else:
        automation_id = _normalise_id(str(automation_id))
    # Parse start time string to time object
    if start_time_if_target_not_met:
        try:
            # Accept strings like "HH:MM" or "HH:MM:SS"
            parts = start_time_if_target_not_met.split(':')
            if len(parts) == 2:
                hour, minute = map(int, parts)
                start_time = datetime.time(hour=hour, minute=minute)
            elif len(parts) == 3:
                hour, minute, second = map(int, parts)
                start_time = datetime.time(hour=hour, minute=minute, second=second)
            else:
                raise ValueError
        except Exception:
            log.error(f"{MODULE_PREFIX} Invalid start_time_if_target_not_met: {start_time_if_target_not_met}, falling back to 00:00")
            start_time = datetime.time(0, 0)
    else:
        # Default to midnight if not supplied
        start_time = datetime.time(0, 0)
    # Ensure grid import components is a list
    grid_import_list: List[str] = sensor_grid_import_components or []

    # Create or update the instance
    if automation_id not in PvAutostart.instances:
        # Ensure sensor_pv_power_now exists
        if not sensor_pv_power_now:
            log.error(
                f"{MODULE_PREFIX} pv_autostart service call missing required parameter 'sensor_pv_power_now'"
            )
            return
        inst = PvAutostart(
            automation_id=automation_id,
            switch_entity=switch_entity,
            sensor_device_power=sensor_device_power,
            off_after_minutes_without_draw=off_after_minutes_without_draw,
            no_draw_threshold_watts=no_draw_threshold_watts,
            buffer_on_minutes=buffer_on_minutes,
            buffer_off_minutes=buffer_off_minutes,
            min_runtime_per_day_minutes=min_runtime_per_day_minutes,
            min_cycles_per_day=min_cycles_per_day,
            interruptible=interruptible,
            sensor_grid_import_components=grid_import_list,
            start_time_if_target_not_met=start_time,
            sensor_forecast_energy_today=sensor_forecast_energy_today,
            sensor_pv_power_now=sensor_pv_power_now,
            exclude_blueprint_turn_on_from_counters=exclude_flag,
            turn_off_time=turn_off_time,
            force_turn_off_if_target_unreached=force_turn_off_flag,
        )
        PvAutostart.instances[automation_id] = {'instance': inst}
        log.info(f"{MODULE_PREFIX} [{switch_entity} {automation_id}] Created new PvAutostart instance.")
    else:
        inst: PvAutostart = PvAutostart.instances[automation_id]['instance']
        inst.update_params(
            switch_entity=switch_entity,
            sensor_device_power=sensor_device_power,
            off_after_minutes_without_draw=off_after_minutes_without_draw,
            no_draw_threshold_watts=no_draw_threshold_watts,
            buffer_on_minutes=buffer_on_minutes,
            buffer_off_minutes=buffer_off_minutes,
            min_runtime_per_day_minutes=min_runtime_per_day_minutes,
            min_cycles_per_day=min_cycles_per_day,
            interruptible=interruptible,
            sensor_grid_import_components=grid_import_list,
            start_time_if_target_not_met=start_time,
            sensor_forecast_energy_today=sensor_forecast_energy_today,
            sensor_pv_power_now=sensor_pv_power_now,
            exclude_blueprint_turn_on_from_counters=exclude_flag,
            turn_off_time=turn_off_time,
            force_turn_off_if_target_unreached=force_turn_off_flag,
        )
        log.info(f"{MODULE_PREFIX} [{switch_entity} {automation_id}] Updated existing PvAutostart instance.")

@service
def pv_autostart_dump(automation_id: str):
    entry = PvAutostart.instances.get(automation_id)
    if not entry:
        log.warning(f"{MODULE_PREFIX} [{automation_id}] dump: no instance found")
        return
    inst = entry["instance"]
    pv = _get_num_state(inst.sensor_pv_power_now, 0) or 0.0
    imp = 0.0
    for e in inst.sensor_grid_import_components:
        v = _get_num_state(e, 0) or 0.0
        if v > 0:
            imp += v
    dev = _get_num_state(inst.sensor_device_power, None) if inst.sensor_device_power else None
    log.info(
        f"{inst.log_prefix} DUMP → pv={pv:.0f}W, import={imp:.0f}W, dev={dev}, "
        f"surplus={max(0.0, pv - imp):.0f}W, "
        f"counters: on={inst.on_counter_sec}s no_draw={inst.no_draw_counter_sec}s "
        f"imp_off={inst.import_off_counter_sec}s pv_def={inst.pv_deficit_counter_sec}s; "
        f"runtime={inst.daily_run_time_sec / 60:.1f}min cycles={inst.cycles_today}"
    )
