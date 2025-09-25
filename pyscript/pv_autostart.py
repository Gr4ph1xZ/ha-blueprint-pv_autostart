# INFO -------------------------------------------------------------------------
# PV Autostart (Pyscript)
# Controls a single appliance based on PV surplus with hysteresis buffers,
# daily targets (runtime & cycles), and optional forecast override.
# - Architecture inspired by pv_excess_control.py (helpers, registry, ticker,
#   midnight reset), but logic is purpose-built for single-device auto-start.
# - Comments in English for clarity.
# ------------------------------------------------------------------------------

from typing import Optional, List, Union, Dict
import datetime

# ----------------------------- Helpers (generic) -------------------------------

def _replace_vowels(input_str: str) -> str:
    """Replace lowercase umlauts to avoid odd entity ids in logs."""
    mapping = {'ä': 'a', 'ö': 'o', 'ü': 'u'}
    return ''.join(mapping.get(c, c) for c in input_str)


def _get_state(entity_id: str) -> Optional[str]:
    """
    Get the state of an entity in Home Assistant.
    Handles climate domain special-casing to normalized 'on'/'off' behavior.
    """
    domain = entity_id.split('.')[0]
    try:
        entity_state = state.get(entity_id)
    except Exception as e:
        log.error(f'[_get_state] Could not get state for {entity_id}: {e}')
        return None

    if entity_state is None:
        return None

    if domain == 'climate':
        s = str(entity_state).lower()
        if s in ['heat', 'cool', 'boost', 'on']:
            return 'on'
        if s == 'off':
            return 'off'
        log.error(f'[_get_state] Unsupported climate state {entity_state} for {entity_id}')
        return None

    return str(entity_state)


def _validate_number(num: Union[float, int, str, None], return_on_error: Optional[float] = None) -> Optional[float]:
    """
    Validate that a value is numeric and in a plausible range [-1e6, 1e6].
    Returns a float or return_on_error on failure.
    """
    if num is None or num == 'unavailable':
        return return_on_error
    try:
        f = float(num)
        if -1_000_000 <= f <= 1_000_000:
            return f
        raise ValueError(f'Out of range: {f}')
    except Exception as e:
        log.error(f'[_validate_number] Invalid number {num=}: {e}')
        return return_on_error


def _get_num_state(entity_id: Optional[str], return_on_error: Optional[float] = None) -> Optional[float]:
    """Convenience: get and validate numeric state."""
    if not entity_id:
        return return_on_error
    return _validate_number(_get_state(entity_id), return_on_error)


def _turn_on(entity_id: str) -> bool:
    """Switch an entity on (generic domain.turn_on)."""
    domain = entity_id.split('.')[0]
    if not service.has_service(domain, 'turn_on'):
        log.error(f'[_turn_on] Service {domain}.turn_on not available for {entity_id}')
        return False
    try:
        service.call(domain, 'turn_on', entity_id=entity_id)
        return True
    except Exception as e:
        log.error(f'[_turn_on] Failed to turn on {entity_id}: {e}')
        return False


def _turn_off(entity_id: str) -> bool:
    """Switch an entity off (generic domain.turn_off)."""
    domain = entity_id.split('.')[0]
    if not service.has_service(domain, 'turn_off'):
        log.error(f'[_turn_off] Service {domain}.turn_off not available for {entity_id}')
        return False
    try:
        service.call(domain, 'turn_off', entity_id=entity_id)
        return True
    except Exception as e:
        log.error(f'[_turn_off] Failed to turn off {entity_id}: {e}')
        return False


# ------------------------------- Core Service ---------------------------------

@time_trigger("cron(0 0 * * *)")
def pv_autostart_midnight_reset():
    """
    Daily reset at 00:00 for all instances:
    - cycles_today
    - daily_run_time_sec
    - flags/timers
    """
    log.info("[PV Autostart] Midnight reset of daily counters for all instances.")
    for data in PvAutostart.instances.copy().values():
        inst = data['instance']
        inst.reset_daily_counters()


@service
def pv_autostart(
    automation_id: str,
    switch_entity: str,
    sensor_device_power: Optional[str] = None,
    off_after_minutes_without_draw: Optional[float] = None,
    no_draw_threshold_watts: Optional[float] = None,
    buffer_on_minutes: Optional[float] = None,
    buffer_off_minutes: Optional[float] = None,
    min_runtime_per_day_minutes: Optional[float] = None,
    min_cycles_per_day: Optional[int] = None,
    interruptible: bool = False,
    sensor_grid_import_components: Optional[List[str]] = None,
    start_time_if_target_not_met: Optional[str] = None,  # 'HH:MM' or 'HH:MM:SS'
    sensor_forecast_energy_today: Optional[str] = None,  # kWh remaining today
    sensor_pv_power_now: Optional[str] = None,
):
    """
    Register or update a PV Autostart controller instance bound to an Automation entity.
    This is intended to be called once on startup and on config changes. The controller
    runs its own periodic ticker inside Pyscript.
    """
    # Normalize automation id like the reference blueprint does
    automation_id = automation_id[11:] if automation_id.startswith('automation.') else automation_id
    automation_id = _replace_vowels(f"automation.{automation_id.strip().replace(' ', '_').lower()}")

    if sensor_grid_import_components is None:
        sensor_grid_import_components = []

    PvAutostart(
        automation_id=automation_id,
        switch_entity=switch_entity,
        sensor_device_power=sensor_device_power,
        off_after_minutes_without_draw=off_after_minutes_without_draw,
        no_draw_threshold_watts=no_draw_threshold_watts,
        buffer_on_minutes=buffer_on_minutes,
        buffer_off_minutes=buffer_off_minutes,
        min_runtime_per_day_minutes=min_runtime_per_day_minutes,
        min_cycles_per_day=min_cycles_per_day,
        interruptible=bool(interruptible),
        sensor_grid_import_components=sensor_grid_import_components,
        start_time_if_target_not_met=start_time_if_target_not_met,
        sensor_forecast_energy_today=sensor_forecast_energy_today,
        sensor_pv_power_now=sensor_pv_power_now,
    )


# ------------------------------- Implementation -------------------------------

class PvAutostart:
    """
    Single-device PV autostart controller.
    - Instance registry keyed by automation_id
    - Periodic ticker (every 10s) per instance
    - Hysteresis via continuous buffer windows
    - Targets: runtime minutes and cycles/day (cycles only count for auto-starts)
    - Forecast controller may override the fixed start time
    """

    instances: Dict[str, Dict[str, Union['PvAutostart', int]]] = {}

    # Global defaults
    TICK_SECONDS = 10
    MIN_SWITCH_COOLDOWN_SEC = 30  # additional relay protection besides hysteresis
    DEFAULT_DEVICE_WATTS = 500.0  # used for forecast-based estimation if no sensor

    def __init__(
        self,
        automation_id: str,
        switch_entity: str,
        sensor_device_power: Optional[str],
        off_after_minutes_without_draw: Optional[float],
        no_draw_threshold_watts: Optional[float],
        buffer_on_minutes: Optional[float],
        buffer_off_minutes: Optional[float],
        min_runtime_per_day_minutes: Optional[float],
        min_cycles_per_day: Optional[int],
        interruptible: bool,
        sensor_grid_import_components: List[str],
        start_time_if_target_not_met: Optional[str],
        sensor_forecast_energy_today: Optional[str],
        sensor_pv_power_now: Optional[str],
    ):
        # Attach or update existing instance
        if automation_id in PvAutostart.instances:
            inst: 'PvAutostart' = PvAutostart.instances[automation_id]['instance']
        else:
            inst = self

        # Configuration
        inst.automation_id = automation_id
        inst.switch_entity = switch_entity
        inst.sensor_device_power = sensor_device_power
        inst.off_after_minutes_without_draw = float(off_after_minutes_without_draw) if off_after_minutes_without_draw is not None else None
        inst.no_draw_threshold_watts = float(no_draw_threshold_watts) if no_draw_threshold_watts is not None else None
        inst.buffer_on_minutes = float(buffer_on_minutes) if buffer_on_minutes is not None else 0.0
        inst.buffer_off_minutes = float(buffer_off_minutes) if buffer_off_minutes is not None else 0.0
        inst.min_runtime_per_day_minutes = float(min_runtime_per_day_minutes) if min_runtime_per_day_minutes is not None else None
        inst.min_cycles_per_day = int(min_cycles_per_day) if min_cycles_per_day is not None else None
        inst.interruptible = bool(interruptible)
        inst.sensor_grid_import_components = list(sensor_grid_import_components or [])
        inst.start_time_if_target_not_met = start_time_if_target_not_met
        inst.sensor_forecast_energy_today = sensor_forecast_energy_today
        inst.sensor_pv_power_now = sensor_pv_power_now

        # State
        inst.log_prefix = f'[PV Autostart {inst.switch_entity} {inst.automation_id}]'
        inst.daily_run_time_sec = 0.0
        inst.cycles_today = 0
        inst.on_since: Optional[datetime.datetime] = None
        inst.last_switch_time: Optional[datetime.datetime] = None
        inst.last_known_switch_state: Optional[str] = None
        inst.last_switched_by_blueprint = False  # becomes True on auto-start

        # Hysteresis timers (continuous windows)
        inst._on_candidate_since: Optional[datetime.datetime] = None
        inst._off_candidate_since: Optional[datetime.datetime] = None  # generic off window (import/deficit)
        inst._no_draw_since: Optional[datetime.datetime] = None        # "no draw" window

        # Start ticker on first registration
        if automation_id not in PvAutostart.instances:
            inst._start_ticker()
            PvAutostart.instances[automation_id] = {'instance': inst, 'priority': 0}
            log.info(f'{inst.log_prefix} Controller started.')
        else:
            log.info(f'{inst.log_prefix} Configuration updated.')

    # --------------------------- Public daily reset ---------------------------

    def reset_daily_counters(self):
        """Reset targets and daily timers at midnight."""
        self.cycles_today = 0
        self.daily_run_time_sec = 0.0
        self._on_candidate_since = None
        self._off_candidate_since = None
        self._no_draw_since = None
        log.info(f'{self.log_prefix} Daily counters reset.')

    # ------------------------------ Periodic loop -----------------------------

    def _start_ticker(self):
        """Create the periodic time trigger for this instance."""
        @time_trigger('period(now, {}s)'.format(self.TICK_SECONDS))
        def _on_time():
            # If automation is deleted or disabled, disable loop behavior gracefully
            if not self._automation_active():
                return _on_time

            try:
                self._tick()
            except Exception as e:
                log.error(f'{self.log_prefix} Tick error: {e}')
            return _on_time

    # --------------------------------- Tick -----------------------------------

    def _tick(self):
        """One control step."""
        now = datetime.datetime.now()

        # Track runtime if currently on
        current_state = _get_state(self.switch_entity)
        if current_state == 'on':
            if self.on_since is None:
                self.on_since = now  # handle HA restart while device was on
            else:
                self.daily_run_time_sec += (now - self.on_since).total_seconds()
                self.on_since = now  # advance marker
        else:
            self.on_since = None

        # Pull sensor values
        pv_now_w = _get_num_state(self.sensor_pv_power_now, return_on_error=0.0) or 0.0
        grid_import_w = self._sum_grid_import_components()
        surplus_w = max(0.0, pv_now_w - grid_import_w)  # approximation path by design

        # Optional device power readings
        device_power_w = _get_num_state(self.sensor_device_power, return_on_error=None) if self.sensor_device_power else None

        # Logging path clarity
        log.debug(f'{self.log_prefix} pv_now={pv_now_w:.0f}W | grid_import_total={grid_import_w:.0f}W | surplus={surplus_w:.0f}W | device_pwr={device_power_w if device_power_w is not None else "n/a"}W')

        # Decide on actions
        if current_state == 'on':
            # Evaluate off conditions in priority order
            if self.should_turn_off_for_no_draw(device_power_w, now):
                self._maybe_turn_off('No-draw window satisfied')
                return
            if self.should_turn_off_for_import(device_power_w, grid_import_w, now):
                self._maybe_turn_off('Import check window satisfied')
                return
            if self._off_hysteresis_met(surplus_w, now) and self._goals_not_violated_if_off():
                self._maybe_turn_off('Generic off hysteresis satisfied')
                return
        else:
            # Off state: either force-run for targets or surplus-based
            if self.need_to_force_run_for_targets(now):
                self._maybe_turn_on('Forced start to meet daily targets (incl. forecast override logic)')
                return
            if self.can_turn_on_by_surplus(surplus_w, now):
                self._maybe_turn_on('PV surplus on-window satisfied')
                return

    # ------------------------------ Conditions --------------------------------

    def can_turn_on_by_surplus(self, surplus_w: float, now: datetime.datetime) -> bool:
        """
        True if a stable surplus existed for buffer_on_minutes continuously.
        """
        if self.buffer_on_minutes <= 0:
            return surplus_w > 0

        if surplus_w > 0:
            if self._on_candidate_since is None:
                self._on_candidate_since = now
            elapsed = (now - self._on_candidate_since).total_seconds() / 60.0
            log.debug(f'{self.log_prefix} On-buffer elapsed={elapsed:.1f}min / required={self.buffer_on_minutes:.1f}min')
            return elapsed >= self.buffer_on_minutes
        else:
            self._on_candidate_since = None
            return False

    def should_turn_off_for_no_draw(self, device_power_w: Optional[float], now: datetime.datetime) -> bool:
        """
        If device power is available and below or equal to threshold continuously for
        off_after_minutes_without_draw minutes. Only permitted when min_cycles condition
        is satisfied or not configured.
        """
        if self.sensor_device_power is None:
            return False
        if self.off_after_minutes_without_draw is None or self.no_draw_threshold_watts is None:
            return False
        if self.min_cycles_per_day is not None and self.cycles_today < self.min_cycles_per_day:
            # not allowed to use "no draw" shutoff yet
            return False

        if device_power_w is None:
            return False

        if device_power_w <= float(self.no_draw_threshold_watts):
            if self._no_draw_since is None:
                self._no_draw_since = now
            elapsed = (now - self._no_draw_since).total_seconds() / 60.0
            log.debug(f'{self.log_prefix} No-draw elapsed={elapsed:.1f}min / required={self.off_after_minutes_without_draw:.1f}min (threshold={self.no_draw_threshold_watts}W)')
            return elapsed >= float(self.off_after_minutes_without_draw)
        else:
            self._no_draw_since = None
            return False

    def should_turn_off_for_import(self, device_power_w: Optional[float], grid_import_w: float, now: datetime.datetime) -> bool:
        """
        If device is interruptible and appears to cause sustained import.
        Condition: grid_import_total > 0 and device power > grid_import_total
        continuously for buffer_off_minutes.
        """
        if not self.interruptible:
            return False
        if self.buffer_off_minutes <= 0:
            return False
        if device_power_w is None:
            return False

        cond = grid_import_w > 0 and device_power_w > grid_import_w
        if cond:
            if self._off_candidate_since is None:
                self._off_candidate_since = now
            elapsed = (now - self._off_candidate_since).total_seconds() / 60.0
            log.debug(f'{self.log_prefix} Import off-buffer elapsed={elapsed:.1f}min / required={self.buffer_off_minutes:.1f}min (dev={device_power_w:.0f}W > import={grid_import_w:.0f}W)')
            return elapsed >= self.buffer_off_minutes
        else:
            self._off_candidate_since = None
            return False

    def _off_hysteresis_met(self, surplus_w: float, now: datetime.datetime) -> bool:
        """
        Generic off hysteresis: treat "no surplus" (surplus == 0) as an off window.
        """
        if self.buffer_off_minutes <= 0:
            return surplus_w <= 0
        if surplus_w <= 0:
            if self._off_candidate_since is None:
                self._off_candidate_since = now
            elapsed = (now - self._off_candidate_since).total_seconds() / 60.0
            log.debug(f'{self.log_prefix} Off-buffer elapsed={elapsed:.1f}min / required={self.buffer_off_minutes:.1f}min (surplus={surplus_w:.0f}W)')
            return elapsed >= self.buffer_off_minutes
        else:
            self._off_candidate_since = None
            return False

    def _goals_not_violated_if_off(self) -> bool:
        """
        Ensure turning off will not immediately violate daily targets. This is a soft
        check: if already met, OK; if not met and device is currently on because of
        forced run, we should not cut it off via generic hysteresis.
        """
        need_runtime = self._remaining_runtime_minutes() > 0
        need_cycles = self._remaining_cycles() > 0
        allow = not (need_runtime or need_cycles)
        if not allow:
            log.debug(f'{self.log_prefix} Preventing off: targets not met (need_runtime={need_runtime}, need_cycles={need_cycles})')
        return allow

    def need_to_force_run_for_targets(self, now: datetime.datetime) -> bool:
        """
        Decide whether to enforce a start based on start_time_if_target_not_met and
        optional forecast value (kWh remaining today). If forecast suggests enough
        PV energy later, delay the forced start; otherwise start now.
        """
        # If no targets, never force-run
        if self.min_runtime_per_day_minutes is None and self.min_cycles_per_day is None:
            return False

        # Parse start time
        if not self.start_time_if_target_not_met:
            return False
        try:
            parts = [int(p) for p in self.start_time_if_target_not_met.split(':')]
            h, m = parts[0], parts[1] if len(parts) > 1 else 0
            s = parts[2] if len(parts) > 2 else 0
            start_today = now.replace(hour=h, minute=m, second=s, microsecond=0)
        except Exception as e:
            log.error(f'{self.log_prefix} Invalid start_time_if_target_not_met="{self.start_time_if_target_not_met}": {e}')
            return False

        # Only consider once we reached or passed the configured time
        if now < start_today:
            return False

        # Are targets already met?
        if self._remaining_runtime_minutes() <= 0 and self._remaining_cycles() <= 0:
            return False

        # Forecast override: only enforce start if otherwise we would likely miss targets
        remaining_forecast_kwh = _get_num_state(self.sensor_forecast_energy_today, return_on_error=None) if self.sensor_forecast_energy_today else None
        if remaining_forecast_kwh is None:
            # No forecast -> enforce start
            return True

        # Estimate energy required to meet *runtime* target (cycles don't consume deterministic energy).
        remaining_minutes = max(0.0, self._remaining_runtime_minutes())
        nominal_watts = self._nominal_device_watts()
        required_wh = (remaining_minutes * nominal_watts) / 60.0

        available_wh = max(0.0, float(remaining_forecast_kwh) * 1000.0)
        # Add a small safety margin so we don't miss by a hair
        margin = 0.10 * required_wh
        decision = available_wh < (required_wh + margin)

        log.info(f'{self.log_prefix} Forecast override check: remaining_forecast={available_wh:.0f}Wh vs required_for_target={required_wh:.0f}Wh -> {"enforce start" if decision else "delay start"}')
        return decision

    # ------------------------------ Actuators ---------------------------------

    def _maybe_turn_on(self, reason: str):
        """Turn on if cooldown allows. Count cycles for auto-starts only."""
        if not self._cooldown_ok():
            log.debug(f'{self.log_prefix} Not turning on due to cooldown.')
            return
        if _turn_on(self.switch_entity):
            self.last_switch_time = datetime.datetime.now()
            self.last_switched_by_blueprint = True
            self.last_known_switch_state = 'on'
            # Count a cycle when we actively started the device
            self.cycles_today += 1
            self.on_since = self.last_switch_time
            log.info(f'{self.log_prefix} Turned ON. Reason: {reason}. cycles_today={self.cycles_today}')

    def _maybe_turn_off(self, reason: str):
        """Turn off if cooldown allows. Only stop forced runs once goals are met or rule explicitly allows it."""
        if not self._cooldown_ok():
            log.debug(f'{self.log_prefix} Not turning off due to cooldown.')
            return
        if _turn_off(self.switch_entity):
            now = datetime.datetime.now()
            # finalize runtime accrual if it was on
            if self.on_since:
                self.daily_run_time_sec += (now - self.on_since).total_seconds()
                self.on_since = None
            self.last_switch_time = now
            self.last_known_switch_state = 'off'
            self.last_switched_by_blueprint = False
            self._on_candidate_since = None
            self._off_candidate_since = None
            self._no_draw_since = None
            log.info(f'{self.log_prefix} Turned OFF. Reason: {reason}. run_time_today_min={(self.daily_run_time_sec/60):.1f}')

    def _cooldown_ok(self) -> bool:
        """Honor a minimal switch interval to protect relays."""
        if self.last_switch_time is None:
            return True
        delta = (datetime.datetime.now() - self.last_switch_time).total_seconds()
        return delta >= self.MIN_SWITCH_COOLDOWN_SEC

    # ----------------------------- Util functions -----------------------------

    def _nominal_device_watts(self) -> float:
        """Return a plausible device power for forecast estimations."""
        p = _get_num_state(self.sensor_device_power, return_on_error=None) if self.sensor_device_power else None
        if p is None or p <= 0:
            return self.DEFAULT_DEVICE_WATTS
        return float(p)

    def _sum_grid_import_components(self) -> float:
        """
        Sum positive import sensors; treat missing/unavailable values as 0.
        Any negative value is clipped to 0 because inputs are defined as positive import.
        """
        total = 0.0
        for eid in self.sensor_grid_import_components:
            v = _get_num_state(eid, return_on_error=0.0) or 0.0
            total += max(0.0, v)
        return total

    def _automation_active(self) -> bool:
        """
        Check if the Automation entity still exists and is enabled. If the automation
        was deleted, clean up this instance.
        """
        a_state = _get_state(self.automation_id)
        if a_state == 'off':
            log.debug(f'{self.log_prefix} Automation is disabled.')
            return False
        if a_state is None:
            log.info(f'{self.log_prefix} Automation entity removed. Deregistering instance.')
            try:
                del PvAutostart.instances[self.automation_id]
            except Exception:
                pass
            return False
        return True

    def _remaining_runtime_minutes(self) -> float:
        if self.min_runtime_per_day_minutes is None:
            return 0.0
        return max(0.0, float(self.min_runtime_per_day_minutes) - (self.daily_run_time_sec / 60.0))

    def _remaining_cycles(self) -> int:
        if self.min_cycles_per_day is None:
            return 0
        return max(0, int(self.min_cycles_per_day) - int(self.cycles_today))
