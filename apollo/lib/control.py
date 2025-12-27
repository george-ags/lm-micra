import math
import logging
import pickle
import time
import copy
import threading
from collections import deque
from timeit import default_timer as timer
from typing import Optional, Callable

from gpiozero import Button, DigitalOutputDevice

import lib.pyacaia as pyacaia
from lib.pyacaia import AcaiaScale

default_target = 36.0
default_overshoot = 1.0
memory_save_file = "memory.save"

class TargetMemory:
    def __init__(self, name: str, color="#ff1303"):
        self.name: str = name
        self.target: float = default_target
        self.overshoot: float = default_overshoot
        self.color: str = color

    def target_minus_overshoot(self) -> float:
        return self.target - self.overshoot

    def update_overshoot(self, weight: float):
        new_overshoot = self.overshoot + (weight - self.target)
        if new_overshoot > 10 or new_overshoot < -10:
            logging.error("New overshoot out of safe range, ignoring")
        else:
            self.overshoot = new_overshoot
            logging.debug("set new overshoot to %.2f" % self.overshoot)


class ControlManager:
    TARE_GPIO = 4
    MEM_GPIO = 21
    SCALE_CONNECT_GPIO = 5
    TGT_INC_GPIO = 12
    TGT_DEC_GPIO = 16
    PADDLE_GPIO = 20
    RELAY_GPIO = 26

    def __init__(self, max_flow_points=500):
        self.flow_rate_data = deque([])
        self.flow_rate_max_points = max_flow_points
        self.relay_off_time = timer()
        self.shot_timer_start: Optional[float] = None
        self.image_needs_save = False
        self.running = True  # Flag to keep watchdog alive
        self.load_memory()

        self.relay = DigitalOutputDevice(ControlManager.RELAY_GPIO)

        # TARGET BUTTONS
        self.tgt_inc_button = Button(ControlManager.TGT_INC_GPIO, hold_time=0.5, hold_repeat=True, pull_up=True, bounce_time=0.05)
        self.tgt_inc_button.when_released = lambda: self.__change_target(0.1)
        self.tgt_inc_button.when_held = lambda: self.__change_target_held(1)

        self.tgt_dec_button = Button(ControlManager.TGT_DEC_GPIO, hold_time=0.5, hold_repeat=True, pull_up=True, bounce_time=0.05)
        self.tgt_dec_button.when_released = lambda: self.__change_target(-0.1)
        self.tgt_dec_button.when_held = lambda: self.__change_target_held(-1)

        # PADDLE SWITCH (CRITICAL CHANGES HERE)
        # 1. bounce_time=None (Raw signal, we handle noise in logic)
        self.paddle_switch = Button(ControlManager.PADDLE_GPIO, pull_up=True, bounce_time=None)
        
        # 2. Only use Interrupt for STARTING (Pressed)
        self.paddle_switch.when_pressed = self.__start_shot
        
        # 3. DO NOT use when_released. We use the Watchdog instead.
        # self.paddle_switch.when_released = self.disable_relay 

        self.tare_button = Button(ControlManager.TARE_GPIO, pull_up=True)

        self.memory_button = Button(ControlManager.MEM_GPIO, pull_up=True)
        self.memory_button.when_pressed = self.__rotate_memory

        self.scale_connect_button = Button(ControlManager.SCALE_CONNECT_GPIO, pull_up=True)
        self.tgt_button_was_held = False

        # START WATCHDOG
        self.wd_thread = threading.Thread(target=self._watchdog_loop)
        self.wd_thread.daemon = True
        self.wd_thread.start()

    def _watchdog_loop(self):
        """
        Safety Loop: Checks 20 times/sec if the paddle is physically open.
        This guarantees the relay turns off even if the 'Event' is missed.
        """
        logging.info("Paddle Watchdog Started")
        while self.running:
            # If Relay is ON, but Switch is NOT Pressed (Open/3.3V) -> STOP
            if self.relay_on() and not self.paddle_switch.is_pressed:
                # Double check to filter micro-noise (10ms wait)
                time.sleep(0.01)
                if not self.paddle_switch.is_pressed:
                    logging.info("Watchdog detected paddle OPEN - Stopping shot")
                    self.disable_relay()
            
            time.sleep(0.05) # Check every 50ms

    def save_memory(self):
        # We don't call this directly in the loop anymore, but keeping it for manual calls if needed
        self._save_worker(self.memories)

    def _save_worker(self, data_to_save):
        try:
            with open(memory_save_file, 'wb') as savefile:
                pickle.dump(data_to_save, savefile)
                logging.info("Saved shot data to memory")
        except Exception as e:
            logging.error("Error persisting memory: %s" % e)

    def load_memory(self):
        try:
            with open(memory_save_file, 'rb') as savefile:
                self.memories = pickle.load(savefile)
        except Exception as e:
            logging.warn("Not able to load memory from save, resetting memory to defaults. Error was: %s" % e)
            self.memories = deque([TargetMemory("A"), TargetMemory("B", "#25a602"), TargetMemory("C", "#376efa")])

    def add_tare_handler(self, callback: Callable):
        self.tare_button.when_pressed = callback

    def should_scale_connect(self) -> bool:
        return self.scale_connect_button.value

    def relay_on(self) -> bool:
        return self.relay.value

    def add_flow_rate_data(self, data_point: float):
        if self.relay_on() or self.relay_off_time + 3.0 > timer():
            self.flow_rate_data.append(data_point)
            if len(self.flow_rate_data) > self.flow_rate_max_points:
                self.flow_rate_data.popleft()

    def disable_relay(self):
        # Only act if relay is actually on (prevents log spam)
        if self.relay_on():
            logging.info("disable relay")
            self.relay_off_time = timer()
            self.relay.off()

            # Thread-safe save
            memories_snapshot = copy.deepcopy(self.memories)
            save_thread = threading.Thread(target=self._save_worker, args=(memories_snapshot,))
            save_thread.start()

    def current_memory(self):
        return self.memories[0]

    def shot_time_elapsed(self):
        if self.shot_timer_start is None:
            return 0.0
        elif self.relay_on():
            return timer() - self.shot_timer_start
        else:
            return self.relay_off_time - self.shot_timer_start

    def __change_target(self, amount):
        if not self.tgt_button_was_held:
            self.memories[0].target += amount
        else:
            self.tgt_button_was_held = False

    def __change_target_held(self, amount):
        self.tgt_button_was_held = True
        if amount > 0:
            self.memories[0].target = math.floor(self.memories[0].target) + math.floor(amount)
        if amount < 0:
            self.memories[0].target = math.ceil(self.memories[0].target) + math.ceil(amount)

    def __rotate_memory(self):
        self.memories.rotate(-1)

    def __start_shot(self):
        # SAFETY CHECK: If the relay is already on, ignore this press
        if self.relay_on():
            return

        logging.info("Start shot")
        self.flow_rate_data = deque([])

        if self.tare_button.when_pressed is not None:
            self.tare_button.when_pressed()
            logging.info("Sent tare to scale")
        
        self.shot_timer_start = timer()
        self.relay.on()


def try_connect_scale(scale: AcaiaScale, mgr: ControlManager) -> bool:
    try:
        if not scale.connected and mgr.should_scale_connect():
            scale.device = None
            devices = pyacaia.find_acaia_devices(timeout=1)
            if devices:
                scale.mac = devices[0]
                logging.debug("calling connect on mac %s" % scale.mac)
                scale.connect()
                return True
            else:
                logging.debug("no devices found")
                return False
        elif scale.connected and not mgr.should_scale_connect():
            logging.debug("scale connected but should not be")
            scale.disconnect()
            return False
        if scale.connected:
            logging.debug("Connected to scale %s" % scale.mac)
            return True
    except Exception as ex:
        logging.error("Failed to connect to found device:%s" % str(ex))
        return False
    