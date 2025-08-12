#!/usr/bin/env python3
import http.server
import socketserver
import subprocess
import json
import os
from pathlib import Path

class VisualizationHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/run-analyzer':
            # Run the analyzer script
            try:
                subprocess.run(['python', 'analyze_codebase.py'], check=True)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'status': 'success'}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'status': 'error', 'message': str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == '__main__':
    PORT = 8000
    
    # Change to the script directory
    os.chdir(Path(__file__).parent)
    
    # Run the analyzer first
    print("Running initial analysis...")
    subprocess.run(['python', 'analyze_codebase.py'])
    
    # Start the server
    with socketserver.TCPServer(("", PORT), VisualizationHandler) as httpd:
        print(f"Server running at http://localhost:{PORT}/")
        print(f"Open http://localhost:{PORT}/package_visualization.html in your browser")
        print("Press Ctrl+C to stop")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")