import subprocess
import time
import sys
import os
from flaskwebgui import FlaskUI
import threading

def start_flask_server():
	"""Start the Flask server in a separate process"""
	try:
		# Start the Flask app with a specific port
		subprocess.run([sys.executable, "app.py"])
	except Exception as e:
		print(f"Error starting Flask server: {e}")

def main():
	# Start Flask server in a background thread
	server_thread = threading.Thread(target=start_flask_server, daemon=True)
	server_thread.start()
	
	# Give the server time to start
	time.sleep(2)
	
	# Configure and start the desktop UI
	# The desktop app will connect to the already running Flask server
	FlaskUI(
		server="flask",
		server_kwargs={
			"port": 5000,
			"host": "127.0.0.1"
		},
		width=1200,
		height=800,
		fullscreen=False,
		app_mode=True
	).run()

if __name__ == "__main__":
	main()
