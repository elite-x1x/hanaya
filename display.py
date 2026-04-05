#!/usr/bin/env python3
"""
HANAYA Bot - Display & UI Components
"""

from typing import List, Dict, Optional
from utils import Colors, format_uptime, format_timestamp


class DisplayManager:
    """Manager untuk semua tampilan UI"""

    def __init__(self):
        pass

    # ─── Banner & Header ──────────────────────────────────────────────────────

    def show_banner(self):
        """Tampilkan banner aplikasi"""
        print("\n")
        print(Colors.BOLD + "╔" + "═" * 68 + "╗" + Colors.RESET)
        print(Colors.BOLD + "║" + Colors.MAGENTA + "  🌸 HANAYA BOT v5.7 - Interactive CLI Manager" + Colors.RESET + Colors.BOLD + " ║" + Colors.RESET)
        print(Colors.BOLD + "╚" + "═" * 68 + "╝" + Colors.RESET)
        print()

    def show_menu_header(self, title: str):
        """Tampilkan header menu"""
        print("\n" + "=" * 70)
        print(Colors.BOLD + f" {title}" + Colors.RESET)
        print("=" * 70 + "\n")

    # ─── Dashboard ────────────────────────────────────────────────────────────

    def show_dashboard(self, bot_manager, global_status: Dict):
        """Tampilkan dashboard utama"""
        self.show_menu_header("🌸 HANAYA BOT v5.7 - Dashboard")

        # Global status
        print(Colors.BOLD + "📊 GLOBAL STATUS:" + Colors.RESET)
        print(f"   ├─ Total Bots     : {global_status['total']}")
        print(f"   ├─ Running        : {global_status['running']}")
        print(f"   ├─ Stopped        : {global_status['stopped']}")
        print(f"   ├─ Healthy        : {global_status['healthy']}")
        print(f"   └─ Uptime         : {global_status['uptime']}\n")

        # Bot status
        print(Colors.BOLD + "🤖 BOT STATUS:" + Colors.RESET)
        for bot_info in bot_manager.get_dashboard():
            status = bot_info["status"]
            if status == "Running":
                status_icon = Colors.GREEN + "🟢" + Colors.RESET
            elif "Dead" in status:
                status_icon = Colors.RED + "🔴" + Colors.RESET
            else:
                status_icon = Colors.YELLOW + "🟡" + Colors.RESET

            print(f"{status_icon} Bot {bot_info['bot_num']}: {bot_info['name']}")
            print(f"   ├─ Status      : {status}")
            print(f"   ├─ Uptime      : {bot_info['uptime']}")
            print(f"   ├─ Port        : {bot_info['port']}")
            print(f"   ├─ PID         : {bot_info['pid']}")
            print(f"   ├─ Health      : {bot_info['health']}")
            print(f"   ├─ Restarts    : {bot_info['restarts']}")
            print(f"   └─ Last Check  : {bot_info['last_health_check']}")
            print()

        # Quick access URLs
        print(Colors.BOLD + "🌐 QUICK ACCESS:" + Colors.RESET)
        for bot_info in bot_manager.get_dashboard():
            print(f"   • Bot {bot_info['bot_num']}:")
            print(f"      - Dashboard: http://localhost:{bot_info['port']}/dashboard")
            print(f"      - Health:   http://localhost:{bot_info['port']}/health")
        print()

    # ─── Bot List ─────────────────────────────────────────────────────────────

    def show_bot_list(self, bots: List[Dict], title: str = "Available Bots"):
        """Tampilkan daftar bot"""
        self.show_menu_header(title)

        if not bots:
            print(Colors.warning("Tidak ada bot yang ditemukan"))
            return

        print(f"{'No':<3} {'Bot #':<5} {'Name':<15} {'Status':<10} {'Port':<6} {'Health':<8} {'Uptime':<10}")
        print("-" * 70)

        for i, bot in enumerate(bots, 1):
            status = bot["status"]
            if status == "Running":
                status_icon = Colors.GREEN + "🟢" + Colors.RESET
            elif "Dead" in status:
                status_icon = Colors.RED + "🔴" + Colors.RESET
            else:
                status_icon = Colors.YELLOW + "🟡" + Colors.RESET

            print(f"{i:<3} {bot['bot_num']:<5} {bot['name']:<15} {status_icon} {status:<8} {bot['port']:<6} {bot['health']:<8} {bot['uptime']:<10}")

    # ─── Bot Detail ───────────────────────────────────────────────────────────

    def show_bot_detail(self, bot_info: Dict):
        """Tampilkan detail bot"""
        self.show_menu_header(f"🤖 Bot {bot_info['bot_num']}: {bot_info['name']}")

        print(Colors.BOLD + "🔧 CONFIGURATION:" + Colors.RESET)
        print(f"   ├─ Bot Number     : {bot_info['bot_num']}")
        print(f"   ├─ Name           : {bot_info['name']}")
        print(f"   ├─ Token          : {bot_info['token_preview']}")
        print(f"   ├─ Target Chat    : {bot_info['target_chat']}")
        print(f"   ├─ Admin Chat     : {bot_info['admin_chat']}")
        print(f"   ├─ Superadmins    : {bot_info['superadmins']}")
        print(f"   ├─ Moderators     : {bot_info['moderators']}")
        print(f"   └─ Flask Port     : {bot_info['port']}\n")

        print(Colors.BOLD + "📊 STATUS:" + Colors.RESET)
        print(f"   ├─ Status         : {bot_info['status']}")
        print(f"   ├─ Uptime         : {bot_info['uptime']}")
        print(f"   ├─ PID            : {bot_info['pid']}")
        print(f"   ├─ Restarts       : {bot_info['restarts']}")
        print(f"   ├─ Health         : {bot_info['health']}")
        print(f"   ├─ Health Detail  : {bot_info['health_detail']}")
        print(f"   └─ Last Check     : {bot_info['last_health_check']}\n")

        print(Colors.BOLD + "📁 LOG FILES:" + Colors.RESET)
        for log_type in ["main", "network", "debug", "reload"]:
            print(f"   └─ {log_type.capitalize():<8}: logs/{bot_info['name']}_{log_type}.log")

        print("\n" + Colors.BOLD + "🌐 ACCESS URLS:" + Colors.RESET)
        print(f"   • Dashboard: http://localhost:{bot_info['port']}/dashboard")
        print(f"   • Health:   http://localhost:{bot_info['port']}/health")

    # ─── Log Viewer ───────────────────────────────────────────────────────────

    def show_log_viewer(self, bot_name: str, log_type: str = "main", lines: int = 20):
        """Tampilkan log viewer"""
        from utils import tail_log, get_log_size

        self.show_menu_header(f"📄 Log Viewer: {bot_name} - {log_type}")

        print(f"Log file: logs/{bot_name}_{log_type}.log")
        print(f"Size: {get_log_size(bot_name, log_type)}")
        print(f"Showing last {lines} lines:\n")
        print("-" * 80)

        log_lines = tail_log(bot_name, log_type, lines)
        for line in log_lines:
            print(line)

        print("-" * 80)
        print(f"Total lines shown: {len(log_lines)}")

    # ─── Confirmation & Input ─────────────────────────────────────────────────

    def confirm_action(self, action: str, target: str = "") -> bool:
        """Konfirmasi aksi"""
        if target:
            msg = f"Apakah Anda yakin ingin {action} {target}?"
        else:
            msg = f"Apakah Anda yakin ingin {action}?"

        return input(f"{Colors.YELLOW}{msg} [y/N]: {Colors.RESET}").strip().lower() in ("y", "yes", "ya")

    def select_bot(self, bots: List[Dict], prompt_text: str = "Pilih bot") -> Optional[Dict]:
        """Pilih bot dari daftar"""
        if not bots:
            print(Colors.warning("Tidak ada bot yang tersedia"))
            return None

        print(f"\n{Colors.CYAN}{prompt_text}:{Colors.RESET}")
        print(f"{'No':<3} {'Bot #':<5} {'Name':<15} {'Status':<10} {'Port':<6} {'Health':<8}")
        print("-" * 60)

        for i, bot in enumerate(bots, 1):
            status = bot["status"]
            if status == "Running":
                status_icon = Colors.GREEN + "🟢" + Colors.RESET
            elif "Dead" in status:
                status_icon = Colors.RED + "🔴" + Colors.RESET
            else:
                status_icon = Colors.YELLOW + "🟡" + Colors.RESET

            print(f"{i:<3} {bot['bot_num']:<5} {bot['name']:<15} {status_icon} {status:<8} {bot['port']:<6} {bot['health']:<8}")

        try:
            choice = int(input(f"\n{Colors.CYAN}Pilih nomor (0 untuk batal): {Colors.RESET}").strip())
            if choice == 0:
                return None
            if 1 <= choice <= len(bots):
                return bots[choice - 1]
            else:
                print(Colors.warning("Pilihan tidak valid"))
                return None
        except ValueError:
            print(Colors.warning("Masukkan angka yang valid"))
            return None

    # ─── Menu Navigation ──────────────────────────────────────────────────────

    def show_main_menu(self) -> int:
        """Tampilkan menu utama"""
        self.show_menu_header("🌸 HANAYA BOT v5.7 - Main Menu")

        print(f"{Colors.CYAN}1.{Colors.RESET} Start All Bots")
        print(f"{Colors.CYAN}2.{Colors.RESET} Stop All Bots")
        print(f"{Colors.CYAN}3.{Colors.RESET} Restart All Bots")
        print(f"{Colors.CYAN}4.{Colors.RESET} View Dashboard")
        print(f"{Colors.CYAN}5.{Colors.RESET} Manage Individual Bot")
        print(f"{Colors.CYAN}6.{Colors.RESET} View Logs")
        print(f"{Colors.CYAN}7.{Colors.RESET} Check Health")
        print(f"{Colors.CYAN}8.{Colors.RESET} Exit")
        print()

        try:
            choice = int(input(f"{Colors.CYAN}Pilih menu (1-8): {Colors.RESET}").strip())
            if 1 <= choice <= 8:
                return choice
            else:
                print(Colors.warning("Pilihan tidak valid"))
                return 0
        except ValueError:
            print(Colors.warning("Masukkan angka yang valid"))
            return 0

    def show_bot_menu(self) -> int:
        """Tampilkan menu bot individual"""
        self.show_menu_header("🤖 Bot Management Menu")

        print(f"{Colors.CYAN}1.{Colors.RESET} Start Bot")
        print(f"{Colors.CYAN}2.{Colors.RESET} Stop Bot")
        print(f"{Colors.CYAN}3.{Colors.RESET} Restart Bot")
        print(f"{Colors.CYAN}4.{Colors.RESET} View Bot Detail")
        print(f"{Colors.CYAN}5.{Colors.RESET} View Log")
        print(f"{Colors.CYAN}6.{Colors.RESET} Clear Log")
        print(f"{Colors.CYAN}7.{Colors.RESET} Back to Main Menu")
        print()

        try:
            choice = int(input(f"{Colors.CYAN}Pilih menu (1-7): {Colors.RESET}").strip())
            if 1 <= choice <= 7:
                return choice
            else:
                print(Colors.warning("Pilihan tidak valid"))
                return 0
        except ValueError:
            print(Colors.warning("Masukkan angka yang valid"))
            return 0

    def show_log_menu(self) -> int:
        """Tampilkan menu log"""
        self.show_menu_header("📄 Log Management Menu")

        print(f"{Colors.CYAN}1.{Colors.RESET} View Main Log")
        print(f"{Colors.CYAN}2.{Colors.RESET} View Network Log")
        print(f"{Colors.CYAN}3.{Colors.RESET} View Debug Log")
        print(f"{Colors.CYAN}4.{Colors.RESET} View Reload Log")
        print(f"{Colors.CYAN}5.{Colors.RESET} Clear Log")
        print(f"{Colors.CYAN}6.{Colors.RESET} Back to Main Menu")
        print()

        try:
            choice = int(input(f"{Colors.CYAN}Pilih menu (1-6): {Colors.RESET}").strip())
            if 1 <= choice <= 6:
                return choice
            else:
                print(Colors.warning("Pilihan tidak valid"))
                return 0
        except ValueError:
            print(Colors.warning("Masukkan angka yang valid"))
            return 0