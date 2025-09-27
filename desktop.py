import os
import sys
from flaskwebgui import FlaskUI
from app import app  # Import your Flask app

def main():
	# Add the current directory to Python path
	current_dir = os.path.dirname(os.path.abspath(__file__))
	if current_dir not in sys.path:
		sys.path.insert(0, current_dir)
	
	# Configuration for the desktop app
	config = {
		'app': app,
		'server': "flask",
		'port': 5000,  # Same port as web interface
		'width': 1400,
		'height': 900,
		'fullscreen': False,
		'app_mode': True,  # Run in app mode (no address bar)
	}
	
	print("🚀 Starting desktop application...")
	print("📊 Application will open in a desktop window")
	print("🌐 Web interface remains available at http://127.0.0.1:5000")
	print("💡 Automatic processing is handled by background_service.py")
	print("⏳ Please wait while the application loads...")
	
	# Create and run the desktop UI
	ui = FlaskUI(**config)
	ui.run()

if __name__ == "__main__":
	main()
