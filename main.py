#!/usr/bin/env python3
"""
HANAYA Bot v5.7 - Interactive CLI Manager
"""

import sys
import time
import signal
from typing import Optional, List, Dict

from config import ConfigManager
from loader import BotManager
from monitor import HealthMonitor
from display import DisplayManager
from utils import Colors, clear_screen, pause


class HANAYABotCLI:
    """Main CLI Application"""

    def __init__(self):
        self.config_manager = ConfigManager()
        self.bot_manager = BotManager()
        self.monitor = HealthMonitor(self.bot_manager)
        self.display = DisplayManager()
        self.running = False

    def setup(self):
        """Setup aplikasi"""
        # Load environment
        if not self.config_manager.load_env():
            print(Colors.warning("python-dotenv tidak terinstall, menggunakan env sistem"))

        # Check bot file
        if not self.config_manager.check_bot_file():
            print(Colors.error("File bot.py tidak ditemukan!"))
            sys.exit(1)

        # Load bot configurations
        self.bot_manager.load_config(self.config_manager)

        # Start monitoring
        self.monitor.start()

    def cleanup(self):
        """Cleanup sebelum exit"""
        self.monitor.stop()
        print(Colors.info("Shutting down..."))

    def run(self):
        """Run main loop"""
        self.running = True
        self.setup()

        # Show banner
        self.display.show_banner()

        # Main loop
        while self.running:
            choice = self.display.show_main_menu()

            if choice == 0:
                continue
            elif choice == 1:  # Start All Bots
                self._start_all_bots()
            elif choice == 2:  # Stop All Bots
                self._stop_all_bots()
            elif choice == 3:  # Restart All Bots
                self._restart_all_bots()
            elif choice == 4:  # View Dashboard
                self._show_dashboard()
            elif choice == 5:  # Manage Individual Bot
                self._manage_individual_bot()
            elif choice == 6:  # View Logs
                self._view_logs()
            elif choice == 7:  # Check Health
                self._check_health()
            elif choice == 8:  # Exit
                self._exit_app()

    # ─── Menu Handlers ────────────────────────────────────────────────────────

    def _start_all_bots(self):
        """Start semua bot"""
        clear_screen()
        self.display.show_menu_header("🚀 Starting All Bots")

        started = self.bot_manager.start_all()
        total = len(self.bot_manager.bots)

        if started == total:
            print(Colors.success(f"✅ {started}/{total} bots started successfully"))
        elif started > 0:
            print(Colors.warning(f"⚠️  {started}/{total} bots started, {total - started} failed"))
        else:
            print(Colors.error("❌ No bots started"))

        pause()

    def _stop_all_bots(self):
        """Stop semua bot"""
        clear_screen()
        self.display.show_menu_header("🛑 Stopping All Bots")

        stopped = self.bot_manager.stop_all()
        total = len(self.bot_manager.bots)

        if stopped == total:
            print(Colors.success(f"✅ {stopped}/{total} bots stopped successfully"))
        elif stopped > 0:
            print(Colors.warning(f"⚠️  {stopped}/{total} bots stopped, {total - stopped} failed"))
        else:
            print(Colors.error("❌ No bots stopped"))

        pause()

    def _restart_all_bots(self):
        """Restart semua bot"""
        clear_screen()
        self.display.show_menu_header("🔄 Restarting All Bots")

        restarted = self.bot_manager.restart_all()
        total = len(self.bot_manager.bots)

        if restarted == total:
            print(Colors.success(f"✅ {restarted}/{total} bots restarted successfully"))
        elif restarted > 0:
            print(Colors.warning(f"⚠️  {restarted}/{total} bots restarted, {total - restarted} failed"))
        else:
            print(Colors.error("❌ No bots restarted"))

        pause()

    def _show_dashboard(self):
        """Tampilkan dashboard"""
        clear_screen()
        global_status = self.bot_manager.get_global_status()
        self.display.show_dashboard(self.bot_manager, global_status)
        pause()

    def _manage_individual_bot(self):
        """Manage bot individual"""
        clear_screen()
        running_bots = [bot.get_info() for bot in self.bot_manager.get_running_bots()]
        stopped_bots = [bot.get_info() for bot in self.bot_manager.get_stopped_bots()]
        all_bots = [bot.get_info() for bot in self.bot_manager.get_all_bots()]

        while True:
            clear_screen()
            print(f"{Colors.BOLD}🤖 Bot Management{Colors.RESET}")
            print(f"   • {Colors.CYAN}Running: {len(running_bots)}{Colors.RESET}")
            print(f"   • {Colors.YELLOW}Stopped: {len(stopped_bots)}{Colors.RESET}")
            print(f"   • {Colors.DIM}Total: {len(all_bots)}{Colors.RESET}\n")

            choice = self.display.show_bot_menu()

            if choice == 0:
                continue
            elif choice == 1:  # Start Bot
                bot = self._select_bot("Pilih bot untuk di-start")
                if bot:
                    bot_obj = self.bot_manager.get_bot(bot["bot_num"])
                    if bot_obj:
                        if bot_obj.start():
                            print(Colors.success(f"✅ Bot {bot['name']} started"))
                        else:
                            print(Colors.error(f"❌ Gagal start bot {bot['name']}"))
                    pause()
            elif choice == 2:  # Stop Bot
                bot = self._select_bot("Pilih bot untuk di-stop")
                if bot:
                    bot_obj = self.bot_manager.get_bot(bot["bot_num"])
                    if bot_obj:
                        if bot_obj.stop():
                            print(Colors.success(f"✅ Bot {bot['name']} stopped"))
                        else:
                            print(Colors.error(f"❌ Gagal stop bot {bot['name']}"))
                    pause()
            elif choice == 3:  # Restart Bot
                bot = self._select_bot("Pilih bot untuk di-restart")
                if bot:
                    bot_obj = self.bot_manager.get_bot(bot["bot_num"])
                    if bot_obj:
                        if bot_obj.restart():
                            print(Colors.success(f"✅ Bot {bot['name']} restarted"))
                        else:
                            print(Colors.error(f"❌ Gagal restart bot {bot['name']}"))
                    pause()
            elif choice == 4:  # View Bot Detail
                bot = self._select_bot("Pilih bot untuk dilihat detailnya")
                if bot:
                    self.display.show_bot_detail(bot)
                    pause()
            elif choice == 5:  # View Log
                bot = self._select_bot("Pilih bot untuk melihat log")
                if bot:
                    self._view_bot_log(bot)
            elif choice == 6:  # Clear Log
                bot = self._select_bot("Pilih bot untuk clear log")
                if bot:
                    self._clear_bot_log(bot)
            elif choice == 7:  # Back to Main Menu
                break

    def _view_logs(self):
        """View logs"""
        clear_screen()
        all_bots = [bot.get_info() for bot in self.bot_manager.get_all_bots()]

        if not all_bots:
            print(Colors.warning("Tidak ada bot yang tersedia"))
            pause()
            return

        bot = self._select_bot("Pilih bot untuk melihat log")
        if not bot:
            return

        while True:
            clear_screen()
            self.display.show_menu_header(f"📄 Log Viewer: {bot['name']}")

            choice = self.display.show_log_menu()

            if choice == 0:
                continue
            elif choice == 1:  # View Main Log
                self.display.show_log_viewer(bot["name"], "main")
                pause()
            elif choice == 2:  # View Network Log
                self.display.show_log_viewer(bot["name"], "network")
                pause()
            elif choice == 3:  # View Debug Log
                self.display.show_log_viewer(bot["name"], "debug")
                pause()
            elif choice == 4:  # View Reload Log
                self.display.show_log_viewer(bot["name"], "reload")
                pause()
            elif choice == 5:  # Clear Log
                self._clear_bot_log(bot)
            elif choice == 6:  # Back to Main Menu
                break

    def _check_health(self):
        """Check health semua bot"""
        clear_screen()
        self.display.show_menu_header("🏥 Health Check")

        results = self.bot_manager.check_all_health()
        total = len(results)
        healthy = sum(1 for _, is_healthy, _ in results if is_healthy)

        print(f"Health check results:")
        print(f"   • Total bots: {total}")
        print(f"   • Healthy: {Colors.GREEN}{healthy}{Colors.RESET}")
        print(f"   • Unhealthy: {Colors.RED}{total - healthy}{Colors.RESET}\n")

        for bot_num, is_healthy, msg in results:
            status = Colors.GREEN + "🟢" + Colors.RESET if is_healthy else Colors.RED + "🔴" + Colors.RESET
            print(f"{status} Bot {bot_num}: {msg}")

        pause()

    def _exit_app(self):
        """Exit aplikasi"""
        clear_screen()
        self.display.show_menu_header("👋 Goodbye!")

        if self.bot_manager.get_running_bots():
            if self.display.confirm_action("stop all running bots"):
                print(Colors.info("Stopping all bots..."))
                self.bot_manager.stop_all()
                print(Colors.success("All bots stopped"))

        print(Colors.info("Thank you for using HANAYA Bot v5.7!"))
        self.cleanup()
        self.running = False

    # ─── Helper Methods ───────────────────────────────────────────────────────

    def _select_bot(self, prompt: str) -> Optional[Dict]:
        """Select bot from list"""
        all_bots = [bot.get_info() for bot in self.bot_manager.get_all_bots()]
        return self.display.select_bot(all_bots, prompt)

    def _view_bot_log(self, bot_info: Dict):
        """View log for specific bot"""
        clear_screen()
        self.display.show_menu_header(f"📄 Log Viewer: {bot_info['name']}")

        print(f"1. View Main Log")
        print(f"2. View Network Log")
        print(f"3. View Debug Log")
        print(f"4. View Reload Log")
        print(f"5. Back")

        try:
            choice = int(input(f"\n{Colors.CYAN}Pilih log (1-5): {Colors.RESET}").strip())
            if choice == 1:
                self.display.show_log_viewer(bot_info["name"], "main")
            elif choice == 2:
                self.display.show_log_viewer(bot_info["name"], "network")
            elif choice == 3:
                self.display.show_log_viewer(bot_info["name"], "debug")
            elif choice == 4:
                self.display.show_log_viewer(bot_info["name"], "reload")
            elif choice == 5:
                return
            else:
                print(Colors.warning("Pilihan tidak valid"))
        except ValueError:
            print(Colors.warning("Masukkan angka yang valid"))

        pause()

    def _clear_bot_log(self, bot_info: Dict):
        """Clear log for specific bot"""
        clear_screen()
        self.display.show_menu_header(f"🗑️  Clear Log: {bot_info['name']}")

        print(f"1. Clear Main Log")
        print(f"2. Clear Network Log")
        print(f"3. Clear Debug Log")
        print(f"4. Clear Reload Log")
        print(f"5. Clear All Logs")
        print(f"6. Back")

        try:
            choice = int(input(f"\n{Colors.CYAN}Pilih log untuk di-clear (1-6): {Colors.RESET}").strip())
            if choice == 1:
                if self.display.confirm_action("clear main log", bot_info["name"]):
                    if self.bot_manager.get_bot(bot_info["bot_num"]).clear_log("main"):
                        print(Colors.success(f"✅ Main log {bot_info['name']} cleared"))
                    else:
                        print(Colors.error(f"❌ Gagal clear main log {bot_info['name']}"))
            elif choice == 2:
                if self.display.confirm_action("clear network log", bot_info["name"]):
                    if self.bot_manager.get_bot(bot_info["bot_num"]).clear_log("network"):
                        print(Colors.success(f"✅ Network log {bot_info['name']} cleared"))
                    else:
                        print(Colors.error(f"❌ Gagal clear network log {bot_info['name']}"))
            elif choice == 3:
                if self.display.confirm_action("clear debug log", bot_info["name"]):
                    if self.bot_manager.get_bot(bot_info["bot_num"]).clear_log("debug"):
                        print(Colors.success(f"✅ Debug log {bot_info['name']} cleared"))
                    else:
                        print(Colors.error(f"❌ Gagal clear debug log {bot_info['name']}"))
            elif choice == 4:
                if self.display.confirm_action("clear reload log", bot_info["name"]):
                    if self.bot_manager.get_bot(bot_info["bot_num"]).clear_log("reload"):
                        print(Colors.success(f"✅ Reload log {bot_info['name']} cleared"))
                    else:
                        print(Colors.error(f"❌ Gagal clear reload log {bot_info['name']}"))
            elif choice == 5:
                if self.display.confirm_action("clear all logs", bot_info["name"]):
                    cleared = 0
                    for log_type in ["main", "network", "debug", "reload"]:
                        if self.bot_manager.get_bot(bot_info["bot_num"]).clear_log(log_type):
                            cleared += 1
                    print(Colors.success(f"✅ {cleared}/4 logs {bot_info['name']} cleared"))
            elif choice == 6:
                return
            else:
                print(Colors.warning("Pilihan tidak valid"))
        except ValueError:
            print(Colors.warning("Masukkan angka yang valid"))

        pause()


def signal_handler(signum, frame):
    """Handle Ctrl+C"""
    print(f"\n\n{Colors.warning('Received interrupt signal')}")
    sys.exit(0)


if __name__ == "__main__":
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run application
    app = HANAYABotCLI()
    try:
        app.run()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.warning('Exiting...')}")
    except Exception as e:
        print(f"\n{Colors.error(f'Fatal error: {e}')}")
        sys.exit(1)