#!/usr/bin/env python3
"""
Service Management Script
Start/stop/status for the background service
"""

import os
import sys
import subprocess
import time
import psutil
import signal

SERVICE_SCRIPT = "background_service.py"
SERVICE_LOG = "background_service.log"

def is_service_running():
	"""Check if background service is running"""
	for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
		try:
			if proc.info['cmdline'] and SERVICE_SCRIPT in ' '.join(proc.info['cmdline']):
				return proc.info['pid']
		except (psutil.NoSuchProcess, psutil.AccessDenied):
			pass
	return None

def start_service():
	"""Start the background service"""
	if is_service_running():
		print("✅ Background service is already running")
		return True
	
	print("🚀 Starting background service...")
	try:
		# Start service in background
		subprocess.Popen([sys.executable, SERVICE_SCRIPT], 
						stdout=open(SERVICE_LOG, 'a'), 
						stderr=subprocess.STDOUT)
		
		# Wait a bit for service to start
		time.sleep(2)
		
		if is_service_running():
			print("✅ Background service started successfully")
			print(f"📋 Logs are being written to: {SERVICE_LOG}")
			return True
		else:
			print("❌ Failed to start background service")
			return False
			
	except Exception as e:
		print(f"❌ Error starting background service: {e}")
		return False

def stop_service():
	"""Stop the background service"""
	pid = is_service_running()
	if not pid:
		print("✅ Background service is not running")
		return True
	
	print("🛑 Stopping background service...")
	try:
		os.kill(pid, signal.SIGTERM)
		time.sleep(1)
		
		if not is_service_running():
			print("✅ Background service stopped successfully")
			return True
		else:
			print("❌ Failed to stop background service")
			return False
			
	except Exception as e:
		print(f"❌ Error stopping background service: {e}")
		return False

def service_status():
	"""Check service status"""
	pid = is_service_running()
	if pid:
		print("🟢 Background service is RUNNING")
		print(f"   PID: {pid}")
		
		# Show last few lines of log
		try:
			with open(SERVICE_LOG, 'r') as f:
				lines = f.readlines()
				if lines:
					print("   Recent log entries:")
					for line in lines[-3:]:
						print(f"   {line.strip()}")
		except FileNotFoundError:
			print("   No log file found")
	else:
		print("🔴 Background service is STOPPED")

def main():
	if len(sys.argv) != 2:
		print("Usage: python manage_service.py <start|stop|status|restart>")
		sys.exit(1)
	
	command = sys.argv[1].lower()
	
	if command == "start":
		start_service()
	elif command == "stop":
		stop_service()
	elif command == "status":
		service_status()
	elif command == "restart":
		stop_service()
		time.sleep(1)
		start_service()
	else:
		print("Invalid command. Use: start, stop, status, or restart")

if __name__ == "__main__":
	main()
