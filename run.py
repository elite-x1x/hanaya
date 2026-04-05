#!/usr/bin/env python3
"""
HANAYA Bot v5.7 - Multi Bot Loader
Script untuk menjalankan multiple bots Telegram dengan loader otomatis
"""

import subprocess
import os
import sys
import time
import signal
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import threading
import requests

class Colors:
    """ANSI color codes untuk terminal output"""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    
    @staticmethod
    def success(text): return f"{Colors.GREEN}✅ {text}{Colors.RESET}"
    @staticmethod
    def error(text): return f"{Colors.RED}❌ {text}{Colors.RESET}"
    @staticmethod
    def warning(text): return f"{Colors.YELLOW}⚠️ {text}{Colors.RESET}"
    @staticmethod
    def info(text): return f"{Colors.CYAN}ℹ️ {text}{Colors.RESET}"
    @staticmethod
    def status(text): return f"{Colors.BLUE}📊 {text}{Colors.RESET}"

class BotConfig:
    """Konfigurasi untuk setiap bot"""
    
    def __init__(self, bot_num: int):
        self.bot_num = bot_num
        self.name = os.getenv(f"BOT_NAME_{bot_num}", f"bot_{bot_num}")
        self.token = os.getenv(f"BOT_TOKEN_{bot_num}")
        self.target_chat = os.getenv(f"TARGET_CHAT_ID_{bot_num}")
        self.admin_chat = os.getenv(f"ADMIN_CHAT_ID_{bot_num}")
        self.flask_port = int(os.getenv(f"FLASK_PORT_{bot_num}", 5000 + bot_num))
        self.superadmins = os.getenv(f"SUPERADMINS_{bot_num}", "")
        self.moderators = os.getenv(f"MODERATORS_{bot_num}", "")
        self.allowed_chats = os.getenv(f"ALLOWED_SOURCE_CHATS_{bot_num}", "")
    
    def is_valid(self) -> bool:
        """Cek apakah konfigurasi valid"""
        return bool(self.name and self.token and self.target_chat)
    
    def get_error_message(self) -> str:
        """Dapatkan pesan error validasi"""
        if not self.name:
            return f"BOT_NAME_{self.bot_num} tidak ditemukan"
        if not self.token:
            return f"BOT_TOKEN_{self.bot_num} tidak ditemukan"
        if not self.target_chat:
            return f"TARGET_CHAT_ID_{self.bot_num} tidak ditemukan"
        return "Konfigurasi tidak valid"

class BotProcess:
    """Manajemen proses bot individual"""
    
    def __init__(self, config: BotConfig):
        self.config = config
        self.process: Optional[subprocess.Popen] = None
        self.start_time: Optional[datetime] = None
        self.is_running = False
        self.restart_count = 0
        self.last_health_check: Optional[datetime] = None
        self.health_status = "unknown"
        self.last_error = None
    
    def get_env(self) -> dict:
        """Dapatkan environment variables untuk bot"""
        env = os.environ.copy()
        env.update({
            'BOT_NAME': self.config.name,
            'BOT_TOKEN': self.config.token,
            'TARGET_CHAT_ID': self.config.target_chat,
            'FLASK_PORT': str(self.config.flask_port),
            'ADMIN_CHAT_ID': self.config.admin_chat or '0',
            'SUPERADMINS': self.config.superadmins,
            'MODERATORS': self.config.moderators,
            'ALLOWED_SOURCE_CHATS': self.config.allowed_chats,
            'PYTHONUNBUFFERED': '1',
        })
        # ✅ GLOBAL settings diambil dari environment (tidak di-override)
        # Semua bot akan menggunakan ALLOWED_MEDIA_TYPES, DAILY_LIMIT, dll dari .env
        return env
    
    def start(self) -> bool:
        """Mulai proses bot"""
        try:
            print(Colors.info(f"Starting {self.config.name} on port {self.config.flask_port}..."))
            
            # ✅ Redirect stdout dan stderr ke DEVNULL
            # Semua logging ditangani oleh bot.py sendiri ke file
            self.process = subprocess.Popen(
                [sys.executable, 'bot.py'],
                env=self.get_env(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None  # Unix only
            )
            
            self.start_time = datetime.now()
            self.is_running = True
            self.restart_count += 1
            self.last_error = None
            
            print(Colors.success(f"{self.config.name} started (PID: {self.process.pid})"))
            print(Colors.info(f"Logs:"))
            print(f"   ├─ Main   : logs/{self.config.name}_main.log")
            print(f"   ├─ Network: logs/{self.config.name}_network.log")
            print(f"   ├─ Debug  : logs/{self.config.name}_debug.log")
            print(f"   └─ Reload : logs/{self.config.name}_reload.log\n")
            
            return True
            
        except FileNotFoundError:
            self.last_error = "File bot.py tidak ditemukan"
            print(Colors.error(self.last_error))
            return False
        except Exception as e:
            self.last_error = str(e)
            print(Colors.error(f"Gagal start {self.config.name}: {self.last_error}"))
            return False
    
    def stop(self, timeout: int = 10) -> bool:
        """Hentikan proses bot dengan graceful shutdown"""
        if not self.process or not self.is_running:
            return False
        
        try:
            print(Colors.warning(f"Stopping {self.config.name} (PID: {self.process.pid})..."))
            
            # Coba terminate dulu (graceful)
            try:
                if hasattr(os, 'killpg'):
                    # Unix: kill process group
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                else:
                    # Windows
                    self.process.terminate()
                
                # Tunggu hingga timeout
                self.process.wait(timeout=timeout)
                print(Colors.success(f"{self.config.name} gracefully stopped"))
                
            except subprocess.TimeoutExpired:
                print(Colors.warning(f"Force killing {self.config.name}..."))
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                else:
                    self.process.kill()
                self.process.wait()
                print(Colors.success(f"{self.config.name} force stopped"))
            
            self.is_running = False
            return True
            
        except Exception as e:
            self.last_error = str(e)
            print(Colors.error(f"Error stopping {self.config.name}: {self.last_error}"))
            return False
    
    def check_health(self) -> Tuple[bool, str]:
        """Check kesehatan bot via HTTP"""
        try:
            url = f"http://localhost:{self.config.flask_port}/health"
            response = requests.get(url, timeout=5)
            self.last_health_check = datetime.now()
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    status = data.get('status', 'unknown')
                    queue_size = data.get('queue_size', 0)
                    daily_count = data.get('daily_count', 0)
                    daily_limit = data.get('daily_limit', 0)
                    
                    self.health_status = status
                    
                    # Return detailed info
                    info = f"{status} (Q:{queue_size} D:{daily_count}/{daily_limit})"
                    return True, info
                except Exception:
                    self.health_status = 'ok'
                    return True, 'ok'
            else:
                self.health_status = 'error'
                return False, f"HTTP {response.status_code}"
                
        except requests.exceptions.ConnectionError:
            self.health_status = 'warming_up'
            return False, "Warming up"
        except requests.exceptions.Timeout:
            self.health_status = 'timeout'
            return False, "Timeout"
        except Exception as e:
            self.health_status = 'error'
            return False, str(type(e).__name__)
    
    def get_uptime(self) -> str:
        """Dapatkan uptime bot"""
        if not self.start_time:
            return "N/A"
        
        delta = datetime.now() - self.start_time
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        seconds = delta.seconds % 60
        
        return f"{hours}h {minutes}m {seconds}s"
    
    def get_status(self) -> str:
        """Dapatkan status bot"""
        if not self.process:
            return "Not started"
        
        if self.process.poll() is not None:
            return f"Dead (exit: {self.process.returncode})"
        
        if self.is_running:
            return "Running"
        
        return "Unknown"

class BotLoader:
    """Main loader untuk manage multiple bots"""
    
    def __init__(self, bot_numbers: Optional[List[int]] = None):
        self.bot_numbers = bot_numbers
        self.bots: List[BotProcess] = []
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.startup_complete = False
    
    def load_env(self):
        """Load environment variables dari .env"""
        try:
            from dotenv import load_dotenv
            if load_dotenv('.env'):
                print(Colors.success("File .env berhasil dimuat"))
            else:
                print(Colors.warning("File .env tidak ditemukan, menggunakan env sistem"))
        except ImportError:
            print(Colors.warning("python-dotenv tidak terinstall"))
    
    def check_bot_file(self) -> bool:
        """Check apakah file bot.py ada"""
        if not Path("bot.py").exists():
            print(Colors.error("File bot.py tidak ditemukan!"))
            return False
        return True
    
    def discover_available_bots(self) -> List[int]:
        """Temukan semua bot yang dikonfigurasi"""
        available = []
        for i in range(1, 100):  # Check hingga 100 bots
            config = BotConfig(i)
            if config.is_valid():
                available.append(i)
            else:
                # Jika bot i tidak valid, stop checking
                if i > 1:
                    break
        return available
    
    def setup_bots(self) -> bool:
        """Setup konfigurasi bots yang dipilih"""
        # Jika bot_numbers tidak ditentukan, gunakan semua yang tersedia
        if self.bot_numbers is None:
            self.bot_numbers = self.discover_available_bots()
        
        if not self.bot_numbers:
            print(Colors.error("Tidak ada bot yang dikonfigurasi!"))
            return False
        
        print("\n" + "="*70)
        print(Colors.info(f"Setting up {len(self.bot_numbers)} bot(s)"))
        print("="*70 + "\n")
        
        # ✅ Display GLOBAL settings
        print(Colors.BOLD + "🌍 GLOBAL CONFIGURATION:" + Colors.RESET)
        print(f"   ├─ Media Types      : {os.getenv('ALLOWED_MEDIA_TYPES', 'video,photo,document')}")
        print(f"   ├─ Daily Limit      : {os.getenv('DAILY_LIMIT', '3500')} media/hari")
        print(f"   ├─ Delay Between    : {os.getenv('DELAY_BETWEEN_SEND', '1.5')}s")
        print(f"   ├─ Group Size       : {os.getenv('GROUP_SIZE', '5')} media/grup")
        print(f"   ├─ Max Queue Size   : {os.getenv('MAX_QUEUE_SIZE', '2000')}")
        print(f"   ├─ Batch Pause      : Setiap {os.getenv('BATCH_PAUSE_EVERY', '500')} media")
        print(f"   ├─ Batch Pause Time : {os.getenv('BATCH_PAUSE_MIN', '30')}-{os.getenv('BATCH_PAUSE_MAX', '120')}s")
        print(f"   └─ Log Level        : {os.getenv('LOG_LEVEL', 'INFO')}\n")
        
        valid_bots = 0
        
        for bot_num in self.bot_numbers:
            config = BotConfig(bot_num)
            
            if not config.is_valid():
                print(Colors.warning(f"Bot {bot_num}: {config.get_error_message()}"))
                continue
            
            bot_process = BotProcess(config)
            self.bots.append(bot_process)
            valid_bots += 1
            
            print(Colors.success(f"Bot {bot_num}: {config.name}"))
            print(f"   ├─ Token        : {config.token[:20]}...")
            print(f"   ├─ Target Chat  : {config.target_chat}")
            print(f"   ├─ Flask Port   : {config.flask_port}")
            print(f"   ├─ Superadmins  : {config.superadmins if config.superadmins else 'None'}")
            print(f"   └─ Moderators   : {config.moderators if config.moderators else 'None'}\n")
        
        print("="*70)
        print(f"✅ Total: {valid_bots}/{len(self.bot_numbers)} bots configured\n")
        
        return valid_bots > 0
    
    def start_all_bots(self) -> bool:
        """Mulai semua bots yang dipilih"""
        print("="*70)
        print(Colors.info(f"Starting {len(self.bots)} bot(s)"))
        print("="*70 + "\n")
        
        if not self.bots:
            print(Colors.error("Tidak ada bot untuk dijalankan"))
            return False
        
        started = 0
        for bot in self.bots:
            if bot.start():
                started += 1
            time.sleep(2)  # Delay antar bot startup
        
        self.running = True
        
        print("="*70)
        print(f"✅ {started}/{len(self.bots)} bots started")
        print("="*70 + "\n")
        
        return started > 0
    
    def display_dashboard(self):
        """Tampilkan dashboard info bots"""
        print("\n" + "="*70)
        print(Colors.BOLD + "🌸 HANAYA BOT v5.7 - Status Dashboard" + Colors.RESET)
        print("="*70 + "\n")
        
        for i, bot in enumerate(self.bots, 1):
            status = bot.get_status()
            uptime = bot.get_uptime()
            
            # Status icon
            if "Running" in status:
                status_icon = Colors.GREEN + "🟢" + Colors.RESET
            elif "Dead" in status:
                status_icon = Colors.RED + "🔴" + Colors.RESET
            else:
                status_icon = Colors.YELLOW + "🟡" + Colors.RESET
            
            print(f"{status_icon} Bot {bot.config.bot_num}: {bot.config.name}")
            print(f"   ├─ Status      : {status}")
            print(f"   ├─ Uptime      : {uptime}")
            print(f"   ├─ Port        : {bot.config.flask_port}")
            print(f"   ├─ PID         : {bot.process.pid if bot.process else 'N/A'}")
            print(f"   ├─ Health      : {bot.health_status}")
            print(f"   ├─ Restarts    : {bot.restart_count}")
            print(f"   └─ Log dir     : logs/")
            print()
        
        print("="*70)
        print("\n📊 Dashboard URLs:")
        for bot in self.bots:
            print(f"   • Bot {bot.config.bot_num}: http://localhost:{bot.config.flask_port}/dashboard")
        
        print("\n🏥 Health Check APIs:")
        for bot in self.bots:
            print(f"   • Bot {bot.config.bot_num}: http://localhost:{bot.config.flask_port}/health")
        
        print("\n📁 Log Files:")
        for bot in self.bots:
            print(f"   • Bot {bot.config.bot_num} ({bot.config.name}):")
            print(f"      - Main   : logs/{bot.config.name}_main.log")
            print(f"      - Network: logs/{bot.config.name}_network.log")
            print(f"      - Debug  : logs/{bot.config.name}_debug.log")
            print(f"      - Reload : logs/{bot.config.name}_reload.log")
        
        print("\n💡 Commands:")
        print("   • Press Ctrl+C to stop all bots")
        print("   • Check logs for detailed information")
        print("   • Use /status command in Telegram for bot status\n")
    
    def monitor_bots(self):
        """Monitor kesehatan bots secara berkala"""
        print(Colors.info("Health monitoring started\n"))
        
        # Startup delay untuk bots fully initialize
        startup_delay = 30
        print(Colors.warning(f"Waiting {startup_delay}s for bots to fully startup...\n"))
        
        for remaining in range(startup_delay, 0, -1):
            if remaining % 10 == 0 or remaining <= 5:
                print(f"   ⏳ Starting health check in {remaining}s...")
            time.sleep(1)
        
        print("   ✅ Health checks started\n")
        self.startup_complete = True
        
        check_count = 0
        last_status_print = {}
        
        while self.running:
            try:
                check_count += 1
                current_time = datetime.now()
                
                # Check status setiap bot
                for bot in self.bots:
                    if bot.is_running:
                        # Cek apakah process masih berjalan
                        if bot.process.poll() is not None:
                            print(Colors.error(f"Bot {bot.config.bot_num} ({bot.config.name}) crashed (exit: {bot.process.returncode})"))
                            bot.is_running = False
                            last_status_print[bot.config.bot_num] = None
                        else:
                            # Check health endpoint
                            is_healthy, msg = bot.check_health()
                            
                            # Update status (hanya print perubahan status)
                            current_status = bot.health_status
                            last_status = last_status_print.get(bot.config.bot_num)
                            
                            # Print jika status berubah atau setiap 120 detik
                            if current_status != last_status or (check_count % 4 == 0):
                                if current_status == 'up':
                                    print(Colors.success(f"Bot {bot.config.bot_num}: {msg}"))
                                else:
                                    print(Colors.warning(f"Bot {bot.config.bot_num}: {msg}"))
                                last_status_print[bot.config.bot_num] = current_status
                
                # Print status summary setiap 120 detik (4 x 30 detik)
                if check_count % 4 == 0:
                    healthy = sum(1 for bot in self.bots if bot.health_status == 'up')
                    total = len(self.bots)
                    print(Colors.status(f"Health check #{check_count} - {healthy}/{total} bots healthy"))
                
                time.sleep(30)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(Colors.error(f"Monitoring error: {str(e)}"))
                time.sleep(30)
    
    def start_monitoring(self):
        """Start monitoring thread"""
        self.monitor_thread = threading.Thread(
            target=self.monitor_bots,
            daemon=True,
            name="BotMonitor"
        )
        self.monitor_thread.start()
    
    def stop_all_bots(self):
        """Hentikan semua bots dengan graceful shutdown"""
        if not self.running:
            return
        
        print("\n\n" + "="*70)
        print(Colors.warning("Shutting down all bots gracefully"))
        print("="*70 + "\n")
        
        self.running = False
        
        # Stop bots dalam urutan reverse (LIFO)
        for bot in reversed(self.bots):
            bot.stop(timeout=10)
            time.sleep(1)
        
        print("="*70)
        print(Colors.success("All bots stopped"))
        print("="*70 + "\n")
    
    def run(self):
        """Main entry point"""
        try:
            # Display banner
            print("\n")
            print(Colors.BOLD + "╔" + "═"*68 + "╗" + Colors.RESET)
            print(Colors.BOLD + "║" + Colors.MAGENTA + "  🌸 HANAYA BOT v5.7 - Multi Bot Loader" + Colors.RESET + Colors.BOLD + " ║" + Colors.RESET)
            print(Colors.BOLD + "╚" + "═"*68 + "╝" + Colors.RESET)
            print()
            
            # Check bot file
            if not self.check_bot_file():
                sys.exit(1)
            
            # Load environment
            self.load_env()
            
            # Setup bots
            if not self.setup_bots():
                sys.exit(1)
            
            # Start bots
            if not self.start_all_bots():
                sys.exit(1)
            
            # Display dashboard
            self.display_dashboard()
            
            # Start monitoring
            self.start_monitoring()
            
            # Wait for interrupt
            print(Colors.info("Press Ctrl+C to stop all bots\n"))
            while True:
                time.sleep(1)
        
        except KeyboardInterrupt:
            self.stop_all_bots()
        except Exception as e:
            print(Colors.error(f"Fatal error: {str(e)}"))
            self.stop_all_bots()
            sys.exit(1)

def print_usage():
    """Print usage information"""
    print(Colors.info("Usage:"))
    print("  python run.py              # Run ALL configured bots (default)")
    print("  python run.py 1            # Run bot 1 only")
    print("  python run.py 1 2          # Run bot 1 and 2")
    print("  python run.py 1 2 3        # Run bot 1, 2, and 3")
    print("  python run.py 2 4          # Run bot 2 and 4")
    print("  python run.py 1 3 4        # Run bot 1, 3, and 4\n")

def main():
    """Main function"""
    def signal_handler(signum, frame):
        print("\n\n" + Colors.warning("Received interrupt signal"))
        loader.stop_all_bots()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse arguments
    bot_numbers = None
    
    if len(sys.argv) > 1:
        if sys.argv in ['--help', '-h']:
            print_usage()
            sys.exit(0)
        
        bot_numbers = []
        try:
            for arg in sys.argv[1:]:
                num = int(arg)
                if num < 1:
                    print(Colors.error(f"Bot number must be >= 1, got {num}"))
                    sys.exit(1)
                bot_numbers.append(num)
        except ValueError as e:
            print(Colors.error(f"Invalid bot number: {e}\n"))
            print_usage()
            sys.exit(1)
        
        if not bot_numbers:
            print(Colors.error("No valid bot numbers provided!\n"))
            print_usage()
            sys.exit(1)
        
        # Remove duplicates dan sort
        bot_numbers = sorted(set(bot_numbers))
        print(Colors.info(f"Starting {len(bot_numbers)} bot(s): {', '.join(map(str, bot_numbers))}\n"))
    else:
        print(Colors.info("No bot numbers specified, running ALL configured bots\n"))
    
    # Create dan run BotLoader
    loader = BotLoader(bot_numbers=bot_numbers)
    loader.run()

if __name__ == "__main__":
    main()