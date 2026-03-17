"""
Run the PHS Portal for mobile testing on local network
This allows you to test on actual phones/tablets on the same WiFi
"""
import socket
from app import app

def get_local_ip():
    """Get the local IP address of this computer"""
    try:
        # Create a socket to determine the local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        return "Unable to determine IP"

if __name__ == "__main__":
    local_ip = get_local_ip()
    port = 5000
    
    print("\n" + "="*60)
    print("🚀 PHS PORTAL - MOBILE TESTING MODE")
    print("="*60)
    print(f"\n📱 To test on your phone:")
    print(f"   1. Connect your phone to the SAME WiFi network")
    print(f"   2. Open your phone's browser")
    print(f"   3. Go to: http://{local_ip}:{port}")
    print(f"\n💻 On this computer: http://localhost:{port}")
    print(f"\n🔧 Your computer's IP: {local_ip}")
    print("\n" + "="*60)
    print("Press Ctrl+C to stop the server")
    print("="*60 + "\n")
    
    # Run Flask with network access enabled
    app.run(host='0.0.0.0', port=port, debug=True)
