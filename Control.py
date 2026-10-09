from flask import Blueprint, request, session, render_template_string, json, jsonify, redirect, render_template, send_file
import subprocess
import os
import signal
import logging
import re
import threading
import time, datetime
import tempfile
import shutil
from pathlib import Path

streaming_bp = Blueprint('streaming', __name__)

channel_assignments = {}  # Track which channel uses which device: {channel_id: device_name}
active_stream_configs = {}  # Track config for each active stream: {device_name: {config}}
assignment_lock = threading.Lock() # Lock for channel assignments

# HLS state tracking
hls_states = {}  # {channel_id: {input_type, input_key, port, playing: bool}}
hls_state_lock = threading.Lock()

# === DEDICATED STREAMING LOGGER SETUP ===
streaming_logger = logging.getLogger('streaming_routes')
streaming_logger.setLevel(logging.DEBUG)

# Remove any existing handlers to avoid duplicates
streaming_logger.handlers = []

# Create file handler for streaming logs
streaming_handler = logging.FileHandler('/home/rf/Desktop/modular/streaming/streaming.log')
streaming_handler.setLevel(logging.DEBUG)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s')
streaming_handler.setFormatter(formatter)

# Add handler to logger
streaming_logger.addHandler(streaming_handler)

# Prevent propagation to root logger (so logs don't go to FGpy.log)
streaming_logger.propagate = False

# Debug logging function using dedicated logger
def debug_log(message):
    """Enhanced debug logging with timestamps and thread info using dedicated streaming logger"""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_name = threading.current_thread().name
    streaming_logger.info(f"[{timestamp}] {message}")
# === END LOGGER SETUP ===

def normalize_device_name(name: str) -> str:
    """
    Normalize device names from ffmpeg/device strings so keys are consistent.
    Removes surrounding quotes and trims whitespace.
    """
    if not isinstance(name, str):
        return name
    # remove surrounding quotes and non-printable whitespace, collapse internal whitespace
    n = name.strip().strip('"').strip("'")
    # collapse repeated spaces
    n = re.sub(r'\s+', ' ', n)
    return n

# Add CORS headers right here - after Blueprint definition, before other routes
@streaming_bp.after_request
def after_request(response):
    """Add CORS headers to all responses"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# Dictionary to store subprocesses and states
subprocesses = {}
device_states = {}
device_list = []

# HLS conversion processes and paths
hls_processes = {}
hls_output_dirs = {}

# Thread lock for safe dictionary operations
process_lock = threading.Lock()
hls_lock = threading.Lock()  # HLS operations

# Preview stream configurations
PREVIEW_STREAMS = {
    'preview1': {
        'srt': 'srt://27.131.14.34:1935?streamid=play/stream/preview1',
        'rtmp': 'rtmp://27.131.14.34:1935/live/preview1'
    },
    'preview2': {
        'srt': 'srt://27.131.14.34:1935?streamid=play/stream/preview2', 
        'rtmp': 'rtmp://27.131.14.34:1935/live/preview2'
    },
    'preview3': {
        'srt': 'srt://27.131.14.34:1935?streamid=play/stream/preview3',
        'rtmp': 'rtmp://27.131.14.34:1935/live/preview3'
    },
    'preview4': {
        'srt': 'srt://27.131.14.34:1935?streamid=play/stream/preview4',
        'rtmp': 'rtmp://27.131.14.34:1935/live/preview4'
    }
}

#FFMPEG_PATH = '/home/rf/bin/ffmpeg'
FFMPEG_PATH = '/opt/ffmpeg/bin/ffmpeg'

PRELOADED_FORMATS = [
    "ntsc: 720x486 at 30000/1001 fps (interlaced, lower field first)",
    "nt23: 720x486 at 24000/1001 fps",
    "pal: 720x576 at 25000/1000 fps (interlaced, upper field first)",
    "23ps: 1920x1080 at 24000/1001 fps",
    "24ps: 1920x1080 at 24000/1000 fps",
    "Hp25: 1920x1080 at 25000/1000 fps",
    "Hp29: 1920x1080 at 30000/1001 fps",
    "Hp30: 1920x1080 at 30000/1000 fps",
    "Hp50: 1920x1080 at 50000/1000 fps",
    "Hp59: 1920x1080 at 60000/1001 fps",
    "Hp60: 1920x1080 at 60000/1000 fps",
    "Hi50: 1920x1080 at 25000/1000 fps (interlaced, upper field first)",
    "Hi59: 1920x1080 at 30000/1001 fps (interlaced, upper field first)",
    "Hi60: 1920x1080 at 30000/1000 fps (interlaced, upper field first)",
    "hp50: 1280x720 at 50000/1000 fps",
    "hp59: 1280x720 at 60000/1001 fps",
    "hp60: 1280x720 at 60000/1000 fps"
]

@streaming_bp.route('/logs_data')
def logs_data():
    try:
        # Change to read from the dedicated streaming log file
        log_file_path = '/home/rf/Desktop/modular/streaming/streaming.log'
        with open(log_file_path, 'r') as file:
            logs = file.readlines()
    except Exception as e:
        logs = [f"Error reading log file: {str(e)}"]
    return {'logs': logs}
    
def get_audio_sample_rate(input_url):
    """
    Detect the audio sample rate of a stream using ffprobe
    Returns sample rate as integer or None if detection fails
    """
    try:
        # Build ffprobe command
        #ffprobe_path = '/home/rf/bin/ffprobe'  # Assuming ffprobe is in same directory as ffmpeg
        ffprobe_path = '/opt/ffmpeg/bin/ffprobe'
        
        cmd = [
            ffprobe_path,
            '-v', 'error',
            '-select_streams', 'a:0',
            '-show_entries', 'stream=sample_rate',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_url
        ]
        
        debug_log(f"Running ffprobe to detect sample rate: {' '.join(cmd)}")
        
        # Run ffprobe with timeout
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10  # 10 second timeout
        )
        
        if result.returncode == 0 and result.stdout.strip():
            sample_rate = int(result.stdout.strip())
            debug_log(f"Detected sample rate: {sample_rate}Hz")
            return sample_rate
        else:
            debug_log(f"Failed to detect sample rate: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        debug_log("ffprobe timeout while detecting sample rate")
        return None
    except Exception as e:
        debug_log(f"Error detecting sample rate: {str(e)}")
        return None

def get_decklink_devices():
    """Get list of DeckLink devices with their current states (inventory-only, safe pairing)."""
    try:
        streaming_logger.info("Executing ffmpeg to list DeckLink devices (inventory only)")
        output = subprocess.check_output(f"{FFMPEG_PATH} -sinks decklink", shell=True, stderr=subprocess.STDOUT)
        devices = []
        found_devices = set()

        # Parse ffmpeg output and normalize names
        for line in output.decode('utf-8').split('\n'):
            if '[' in line and ']' in line:
                pattern = r'\[([^\]]+)\]'
                matches = re.findall(pattern, line)
                for match in matches:
                    device_name = normalize_device_name(match)
                    # filter relevant names
                    if (device_name.startswith('DeckLink') or 'SDI' in device_name or 'Mini Monitor' in device_name):
                        if device_name not in found_devices:
                            found_devices.add(device_name)
                            devices.append(device_name)

        # With process_lock, update inventory only:
        with process_lock:
            # Add new devices to device_states if absent; do NOT remove existing entries here.
            for dev in devices:
                if dev not in device_states:
                    device_states[dev] = 'inactive'
            # Also, if there are devices in device_states not present in devices,
            # keep them (do not remove), because they may be temporarily invisible.
            # Now, pair with running subprocesses (read-only pairing)
            for dev in devices:
                if dev in subprocesses:
                    proc = subprocesses[dev]
                    if proc.poll() is None:
                        device_states[dev] = 'active'
                    else:
                        # process in subprocesses has died; cleanup that key here:
                        streaming_logger.warning(f"Device {dev} had dead process during inventory pairing; cleaning subprocess entry")
                        del subprocesses[dev]
                        device_states[dev] = 'inactive'
                else:
                    # If no tracked subprocess, leave existing state as-is (do not forcibly set inactive),
                    # but set to inactive if it's unknown
                    if dev not in device_states:
                        device_states[dev] = 'inactive'

        streaming_logger.info(f"Found devices: {devices}, Current states snapshot: {dict(device_states)}")
        return devices

    except subprocess.CalledProcessError as e:
        streaming_logger.error(f"Error listing DeckLink devices: {e}")
        # Fallback: return known device_states keys as a list (normalized)
        with process_lock:
            fallback_devices = list(device_states.keys()) or ["DeckLink SDI (1)", "DeckLink SDI (2)", "DeckLink Mini Monitor 4K"]
        return fallback_devices
            
def get_device_formats(device_name):
    """Get supported formats for a DeckLink device"""
    try:
        streaming_logger.info(f"Executing ffmpeg to list formats for {device_name}")
        output = subprocess.check_output(f'{FFMPEG_PATH} -f decklink -list_formats 1 -i "{device_name}"', shell=True, stderr=subprocess.STDOUT)
        formats = []
        format_section = False
        
        for line in output.decode('utf-8').split('\n'):
            if 'format_code' in line and 'description' in line:
                format_section = True
                continue
            elif '[in' in line:
                break
            elif format_section and line.strip():
                formats.append(line.strip())
        
        streaming_logger.info(f"Found formats: {formats}")
        return formats
    except subprocess.CalledProcessError as e:
        streaming_logger.error(f"Error listing formats for {device_name}: {e}")
        streaming_logger.info(f"Using preloaded formats due to error: {PRELOADED_FORMATS}")
        return PRELOADED_FORMATS

@streaming_bp.route('/get_formats/<device_name>', methods=['GET'])
def get_formats(device_name):
    formats = get_device_formats(device_name)
    return jsonify(formats)

@streaming_bp.route('/get_device_names', methods=['GET'])
def get_device_names():
    """Return device names (normalized) without mutating global state aggressively."""
    try:
        result = subprocess.run(
            f"{FFMPEG_PATH} -sinks decklink",
            shell=True,
            capture_output=True,
            text=True,
            timeout=5
        )

        devices = []
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if '[' in line and ']' in line:
                    pattern = r'\[([^\]]+)\]'
                    matches = re.findall(pattern, line)
                    for match in matches:
                        device_name = normalize_device_name(match)
                        if (device_name.startswith('DeckLink') or 'SDI' in device_name or 'Mini Monitor' in device_name):
                            if device_name not in devices:
                                devices.append(device_name)

        # If no devices discovered, fall back to known inventory
        if not devices:
            with process_lock:
                devices = list(device_states.keys())
                if not devices:
                    devices = ["DeckLink SDI (1) (FB)", "DeckLink SDI (2) (FB)", "DeckLink Mini Monitor 4K (FB)"]

        streaming_logger.info(f"Returning {len(devices)} device names")
        return jsonify(devices)

    except Exception as e:
        streaming_logger.error(f"Error in get_device_names: {e}")
        with process_lock:
            devices = list(device_states.keys())
            if not devices:
                devices = ["DeckLink SDI (1) (E)", "DeckLink SDI (2) (E)", "DeckLink Mini Monitor 4K (E)"]
            return jsonify(devices)

def start_command(device, input_url, resolution_frame_rate, channel_id=None, 
                  input_type=None, input_key=None, port_number=None, format=None):
    debug_log(f"start_command called for device: {device}, channel: {channel_id}")

    # Normalize device key immediately
    device = normalize_device_name(device)

    resolution, frame_rate = extract_resolution_and_frame_rate(resolution_frame_rate)
    if not resolution or not frame_rate:
        debug_log(f"Invalid resolution/frame rate: {resolution_frame_rate}")
        return "Invalid resolution or frame rate.", None, None

    # Use lock to check if device is already in use
    with process_lock:
        if device in subprocesses:
            proc_status = subprocesses[device].poll()
            if proc_status is None:
                debug_log(f"Device {device} is actively running, rejecting start")
                return f"Device {device} is already in use.", None, None
            else:
                debug_log(f"Device {device} had a dead process, cleaning up")
                del subprocesses[device]
                device_states[device] = 'inactive'
                # Also clean up channel assignment
                with assignment_lock:
                    for ch, dev in list(channel_assignments.items()):
                        if dev == device:
                            del channel_assignments[ch]

    # Detect audio sample rate with retry logic
    debug_log(f"Detecting audio sample rate for {input_url}")
    sample_rate = None
    max_retries = 2
    
    for attempt in range(max_retries):
        sample_rate = get_audio_sample_rate(input_url)
        if sample_rate is not None:
            break
        if attempt < max_retries - 1:
            debug_log(f"Sample rate detection failed, retrying... ({attempt + 1}/{max_retries})")
            time.sleep(1)
    
    # Build FFmpeg command with appropriate audio handling
    if "srt://" in input_url.lower():
        # SRT: no -re flag, add buffer options
        base_command = f'{FFMPEG_PATH} -i "{input_url}"'
        is_srt = True
    else:
        # RTMP: keep -re flag
        base_command = f'{FFMPEG_PATH} -re -i "{input_url}"'
        is_srt = False
    
    # Handle different audio scenarios
    if sample_rate is not None and sample_rate != 48000:
        debug_log(f"Audio sample rate {sample_rate}Hz detected, adding resampling to 48kHz")
        # Use filter_complex for audio resampling
        if is_srt:
            # For SRT, add latency parameters
            command = (f'{base_command} -thread_queue_size 1024 -max_delay 500000 '
                      f'-filter_complex "[0:a]aresample=48000:async=1:min_comp=0.01[a]" '
                      f'-pix_fmt uyvy422 -s {resolution} -r {frame_rate} '
                      f'-map 0:v -map "[a]" -f decklink "{device}"')
        else:
            command = (f'{base_command} -thread_queue_size 1024 -max_delay 500000 '
                      f'-filter_complex "[0:a]aresample=48000:async=1[a]" '
                      f'-pix_fmt uyvy422 -s {resolution} -r {frame_rate} '
                      f'-map 0:v -map "[a]" -f decklink "{device}"')
    elif sample_rate == 48000:
        debug_log(f"Audio already at 48kHz, no resampling needed")
        command = (f'{base_command} -thread_queue_size 1024 -max_delay 500000 '
                  f'-pix_fmt uyvy422 -s {resolution} -r {frame_rate} '
                  f'-ar 48000 -ac 2 -f decklink "{device}"')
    else:
        # Sample rate detection failed, use safe default with resampling
        debug_log("Sample rate detection failed, using safe mode with audio resampling")
        command = (f'{base_command} -thread_queue_size 1024 -max_delay 500000 '
                  f'-filter_complex "[0:a]aresample=48000:async=1[a]" '
                  f'-pix_fmt uyvy422 -s {resolution} -r {frame_rate} '
                  f'-map 0:v -map "[a]" -f decklink "{device}"')
    
    debug_log(f"FFmpeg command: {command}")
    
    try:
        # Start FFmpeg with stderr capture for statistics
        proc = subprocess.Popen(
            command, 
            shell=True, 
            stderr=subprocess.PIPE,  # Capture stderr for statistics
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
            preexec_fn=os.setsid
        )
        
        debug_log(f"FFmpeg process started with PID: {proc.pid}")

        # Store process with additional monitoring data
        with process_lock:
            subprocesses[device] = proc
            device_states[device] = 'active'
            
            # Track channel assignment if channel_id provided
            if channel_id:
                with assignment_lock:
                    channel_assignments[channel_id] = device
            
            # Store ALL parameters in config
            active_stream_configs[device] = {
                'channel_id': channel_id,
                'input_url': input_url,
                'resolution_frame_rate': resolution_frame_rate,
                'resolution': resolution,
                'frame_rate': frame_rate,
                'start_time': datetime.datetime.now().isoformat(),
                'last_packet_time': datetime.datetime.now().isoformat(),
                'packets_received': 0,
                'health_status': 'HEALTHY',
                'monitoring_thread': None,
                # Store all parameters for syncing
                'input_type': input_type,
                'input_key': input_key,
                'port_number': port_number,
                'format': format,
                # Add audio info
                'detected_sample_rate': sample_rate,
                'resampling_applied': sample_rate != 48000
            }
            
            debug_log(f"Added device {device} to subprocesses, current count: {len(subprocesses)}")
        
        # Start monitoring thread for this process
        monitor_thread = threading.Thread(
            target=monitor_ffmpeg_health,
            args=(device, proc, channel_id),
            daemon=True
        )
        monitor_thread.start()
        
        # Store thread reference
        active_stream_configs[device]['monitoring_thread'] = monitor_thread

        debug_log(f"Successfully started streaming to {device} with audio handling")
        return f"Started streaming to {device} (audio: {sample_rate}Hz→48kHz)\n", command, proc.pid

    except Exception as e:
        debug_log(f"Error starting FFmpeg: {str(e)}")
        with process_lock:
            if device in subprocesses:
                del subprocesses[device]
            device_states[device] = 'inactive'
            # Clean up assignment on error
            with assignment_lock:
                if channel_id in channel_assignments:
                    del channel_assignments[channel_id]
                if device in active_stream_configs:
                    del active_stream_configs[device]
        return str(e), command, None

def monitor_ffmpeg_health(device, proc, channel_id=None):
    """
    Real-time monitoring of FFmpeg process health
    Tracks packet reception, buffer health, and stream stability
    """
    debug_log(f"Starting health monitor for {device}")
    
    # Inside monitor_ffmpeg_health function, add this near the top:
    audio_info = {}
    with process_lock:
        if device in active_stream_configs:
            audio_info = {
                'detected_rate': active_stream_configs[device].get('detected_sample_rate'),
                'resampling': active_stream_configs[device].get('resampling_applied')
            }

    debug_log(f"Health monitor for {device} started. Audio: {audio_info}")
    
    last_frame_time = time.time()
    frames_received = 0
    consecutive_errors = 0
    buffer_warnings = 0
    stream_frozen = False
    
    # Health check intervals
    CHECK_INTERVAL = 2  # Check every 2 seconds
    FROZEN_THRESHOLD = 10  # 10 seconds without frames = frozen
    MAX_BUFFER_WARNINGS = 5  # Max buffer warnings before action
    
    while proc.poll() is None:  # While process is running
        try:
            # Read FFmpeg output line by line
            line = proc.stderr.readline()
            if line:
                line = line.strip()
                
                # Parse for frame statistics
                if "frame=" in line:
                    last_frame_time = time.time()
                    frames_received += 1
                    consecutive_errors = 0
                    
                    # Extract frame count
                    frame_match = re.search(r'frame=\s*(\d+)', line)
                    if frame_match:
                        current_frame = int(frame_match.group(1))
                        
                        # Update statistics in config
                        with process_lock:
                            if device in active_stream_configs:
                                active_stream_configs[device]['last_packet_time'] = datetime.datetime.now().isoformat()
                                active_stream_configs[device]['packets_received'] = frames_received
                                active_stream_configs[device]['current_frame'] = current_frame
                
                # Check for buffer warnings (DeckLink specific)
                if "not enough buffered" in line.lower() or "buffer" in line.lower():
                    buffer_warnings += 1
                    debug_log(f"Buffer warning #{buffer_warnings} on {device}: {line}")
                    
                    if buffer_warnings >= MAX_BUFFER_WARNINGS:
                        debug_log(f"Critical buffer warnings on {device}, considering restart")
                        # Could adjust parameters or restart here
                
                # Check for SRT/RTMP connection issues
                if "error" in line.lower() or "failed" in line.lower():
                    consecutive_errors += 1
                    debug_log(f"Error detected on {device}: {line}")
                    
                    if consecutive_errors > 3:
                        debug_log(f"Multiple consecutive errors on {device}, stream may be failing")
                
                # Check for stream end/disconnect
                if "connection closed" in line.lower() or "end of file" in line.lower():
                    debug_log(f"Stream disconnected on {device}")
                    stream_frozen = True
                
                # Log significant events
                if any(term in line.lower() for term in ['speed=', 'bitrate=', 'time=']):
                    debug_log(f"FFmpeg stats for {device}: {line}")
            
            # Check for frozen stream
            current_time = time.time()
            time_since_last_frame = current_time - last_frame_time
            
            if time_since_last_frame > FROZEN_THRESHOLD and not stream_frozen:
                debug_log(f"Stream frozen on {device}: No frames for {time_since_last_frame:.1f} seconds")
                stream_frozen = True
                
                # Update health status
                with process_lock:
                    if device in active_stream_configs:
                        active_stream_configs[device]['health_status'] = 'FROZEN'
                        active_stream_configs[device]['last_health_check'] = datetime.datetime.now().isoformat()
                
                # Auto-recovery: restart the stream
                if channel_id:
                    debug_log(f"Attempting auto-recovery for {device} (channel {channel_id})")
                    try:
                        # Get stream config for restart
                        with process_lock:
                            if device in active_stream_configs:
                                config = active_stream_configs[device].copy()
                        
                        # Stop current stream
                        stop_command(device, channel_id)
                        
                        # Wait a moment
                        time.sleep(2)
                        
                        # Restart with same config
                        if config:
                            debug_log(f"Restarting {device} with saved config")
                            # Extract parameters from config
                            input_url = config.get('input_url', '')
                            resolution_frame_rate = config.get('resolution_frame_rate', '')
                            
                            if input_url and resolution_frame_rate:
                                start_command(
                                    device, input_url, resolution_frame_rate, channel_id,
                                    input_type=config.get('input_type'),
                                    input_key=config.get('input_key'),
                                    port_number=config.get('port_number'),
                                    format=config.get('format')
                                )
                                debug_log(f"Auto-restart completed for {device}")
                    except Exception as e:
                        debug_log(f"Auto-recovery failed for {device}: {str(e)}")
            
            # Update health status periodically
            if frames_received > 0 and not stream_frozen:
                with process_lock:
                    if device in active_stream_configs:
                        active_stream_configs[device]['health_status'] = 'HEALTHY'
                        active_stream_configs[device]['frames_received'] = frames_received
                        active_stream_configs[device]['buffer_warnings'] = buffer_warnings
            
            # Sleep to prevent CPU burn
            time.sleep(0.1)
            
        except Exception as e:
            debug_log(f"Health monitor error for {device}: {str(e)}")
            time.sleep(CHECK_INTERVAL)
    
    # Process ended
    debug_log(f"Health monitor stopping for {device}, process ended")
    
    # Cleanup
    with process_lock:
        if device in active_stream_configs:
            active_stream_configs[device]['health_status'] = 'STOPPED'

def stop_command(device, channel_id=None):
    debug_log(f"stop_command called for device: {device}, channel: {channel_id}")

    # Normalize device key
    device = normalize_device_name(device)

    killed_any = False  # Track whether we killed something

    with process_lock:
        # --- 1) Kill the tracked FFmpeg process (the one we started) ---
        proc = subprocesses.get(device)
        if proc:
            try:
                debug_log(f"Killing main process group for {device}, PGID={os.getpgid(proc.pid)}")
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                killed_any = True
            except Exception as e:
                debug_log(f"Error killing primary process for {device}: {e}")
            finally:
                if device in subprocesses:
                    del subprocesses[device]
                device_states[device] = "inactive"
                
                # Clean up monitoring data
                if device in active_stream_configs:
                    if 'monitoring_thread' in active_stream_configs[device]:
                        # Thread will exit when process ends
                        pass
                    del active_stream_configs[device]
                
                # Clean up channel assignment and config
                with assignment_lock:
                    # Find and remove channel assignment
                    channels_to_remove = []
                    for ch, dev in channel_assignments.items():
                        if dev == device:
                            channels_to_remove.append(ch)
                    for ch in channels_to_remove:
                        del channel_assignments[ch]
                    
                    # Remove config
                    if device in active_stream_configs:
                        del active_stream_configs[device]

    # --- 2) Extra safety: kill any stray FFmpeg processes still using this DeckLink device ---
    try:
        output = subprocess.check_output(["ps", "aux"], text=True)
        for line in output.splitlines():
            if "ffmpeg" in line and device in line:
                parts = line.split()
                pid = int(parts[1])
                try:
                    debug_log(f"Killing stray FFmpeg PID={pid} using {device}")
                    os.kill(pid, signal.SIGKILL)
                    killed_any = True
                except Exception as e:
                    debug_log(f"Failed to kill stray PID={pid}: {e}")
    except Exception as e:
        debug_log(f"Error searching for stray FFmpeg processes: {e}")

    if killed_any:
        debug_log(f"Stopped all FFmpeg activity on {device}")
        return f"Stopped streaming on {device}\n"
    else:
        debug_log(f"No active FFmpeg processes found on {device}")
        return f"No running stream found on {device}\n"
        
@streaming_bp.route('/stream_health/<device_name>', methods=['GET'])
def stream_health(device_name):
    """Get health status of a stream"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Not authorized'}), 401
    
    with process_lock:
        if device_name not in active_stream_configs:
            return jsonify({'error': 'Stream not found'}), 404
        
        config = active_stream_configs.get(device_name, {})
        
        # Calculate uptime
        uptime = None
        if 'start_time' in config:
            try:
                start_time = datetime.datetime.fromisoformat(config['start_time'])
                uptime = str(datetime.datetime.now() - start_time)
            except:
                uptime = "Unknown"
        
        # Get process status
        proc = subprocesses.get(device_name)
        process_alive = proc is not None and proc.poll() is None
        
        health_data = {
            'device': device_name,
            'process_alive': process_alive,
            'health_status': config.get('health_status', 'UNKNOWN'),
            'frames_received': config.get('frames_received', 0),
            'last_packet_time': config.get('last_packet_time'),
            'buffer_warnings': config.get('buffer_warnings', 0),
            'uptime': uptime,
            'channel_id': config.get('channel_id'),
            'input_type': config.get('input_type'),
            'input_key': config.get('input_key'),
            # Add audio info
            'audio_sample_rate': config.get('detected_sample_rate'),
            'audio_resampling': config.get('resampling_applied', False)
        }
        
        return jsonify(health_data)

@streaming_bp.route('/all_streams_health', methods=['GET'])
def all_streams_health():
    """Get health status of all active streams"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Not authorized'}), 401
    
    health_status = {}
    
    with process_lock:
        for device, config in active_stream_configs.items():
            proc = subprocesses.get(device)
            process_alive = proc is not None and proc.poll() is None
            
            # Calculate uptime
            uptime = None
            if 'start_time' in config:
                try:
                    start_time = datetime.datetime.fromisoformat(config['start_time'])
                    uptime = str(datetime.datetime.now() - start_time)
                except:
                    uptime = "Unknown"
            
            health_status[device] = {
                'process_alive': process_alive,
                'health_status': config.get('health_status', 'UNKNOWN'),
                'frames_received': config.get('frames_received', 0),
                'uptime': uptime,
                'channel_id': config.get('channel_id')
            }
    
    return jsonify(health_status)

@streaming_bp.route('/device_state/<device_name>', methods=['GET'])
def device_state(device_name):
    """Get state of a specific device"""
    state = device_states.get(device_name, 'inactive')
    return jsonify(state)

@streaming_bp.route('/active_channel_configs', methods=['GET'])
def get_active_channel_configs():
    """Get configurations for all active streams with channel assignments"""
    if not session.get('logged_in'):
        streaming_logger.warning("Unauthorized access to /active_channel_configs")
        return jsonify({'error': 'Not authorized'}), 401
    
    try:
        # Use separate with statements to avoid potential deadlocks
        with process_lock:
            active_configs = {}
            current_subprocesses = dict(subprocesses)  # Create a copy
            current_device_states = dict(device_states)  # Create a copy
            
            # Build response with current active streams
            for device, proc in current_subprocesses.items():
                if proc.poll() is None:  # Process is running
                    config = active_stream_configs.get(device, {})
                    
                    # Ensure config is JSON-serializable - convert any non-serializable values
                    serializable_config = {}
                    if config:
                        for key, value in config.items():
                            # Skip functions/methods
                            if callable(value):
                                continue
                            # Convert datetime to string
                            if isinstance(value, datetime.datetime):
                                serializable_config[key] = value.isoformat()
                            # Convert other non-serializable types
                            elif hasattr(value, '__dict__'):
                                serializable_config[key] = str(value)
                            else:
                                try:
                                    # Test if it's JSON serializable
                                    json.dumps({key: value})
                                    serializable_config[key] = value
                                except (TypeError, ValueError):
                                    serializable_config[key] = str(value)
                    
                    active_configs[device] = {
                        'device': device,
                        'state': 'active',
                        'pid': proc.pid if hasattr(proc, 'pid') else None,
                        'config': serializable_config,
                        'channel_id': serializable_config.get('channel_id')
                    }
        
        # Get channel assignments separately
        with assignment_lock:
            channel_mapping = dict(channel_assignments)
            
        # Get HLS states separately
        with hls_state_lock:
            # Ensure HLS states are JSON-serializable
            hls_states_copy = {}
            for channel_id, state in hls_states.items():
                serializable_state = {}
                for key, value in state.items():
                    if callable(value):
                        continue
                    elif isinstance(value, datetime.datetime):
                        serializable_state[key] = value.isoformat()
                    else:
                        try:
                            json.dumps({key: value})
                            serializable_state[key] = value
                        except (TypeError, ValueError):
                            serializable_state[key] = str(value)
                hls_states_copy[channel_id] = serializable_state
        
        return jsonify({
            'active_streams': active_configs,
            'channel_assignments': channel_mapping,
            'device_states': current_device_states,
            'hls_states': hls_states_copy
        })
        
    except Exception as e:
        streaming_logger.error(f"Error in /active_channel_configs: {str(e)}", exc_info=True)
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500

@streaming_bp.route('/all_device_states', methods=['GET'])
def all_device_states():
    """Return states of all devices for the frontend"""
    with process_lock:
        # Ensure subprocesses are reflected properly
        for device, proc in list(subprocesses.items()):
            if proc.poll() is None:
                device_states[device] = 'active'
            else:
                # cleanup dead process
                del subprocesses[device]
                device_states[device] = 'inactive'

        # Ensure we have known devices present in response - do not remove existing states
        all_devices = ["DeckLink SDI (1)", "DeckLink SDI (2)", "DeckLink Mini Monitor 4K"]
        for device in all_devices:
            if device not in device_states:
                device_states[device] = 'inactive'

        response = {
            'device_states': dict(device_states),
            '_debug': {
                'subprocess_count': len(subprocesses),
                'subprocess_devices': list(subprocesses.keys()),
                'actual_running': [d for d, p in subprocesses.items() if p.poll() is None],
                'timestamp': datetime.datetime.now().isoformat()
            }
        }
        return jsonify(response)

def monitor_processes():
    """Background thread to monitor FFmpeg processes"""
    debug_log("Background monitor thread started")
    
    while True:
        try:
            devices_to_cleanup = []
            
            # Use lock to safely check processes
            with process_lock:
                for device, proc in list(subprocesses.items()):
                    poll_result = proc.poll()
                    if poll_result is not None:  # Process finished
                        debug_log(f"Monitor: Process for {device} finished with returncode {poll_result}")
                        devices_to_cleanup.append(device)
            
            # Clean up finished processes
            for device in devices_to_cleanup:
                with process_lock:
                    if device in subprocesses:
                        debug_log(f"Monitor: Removing {device} from subprocesses")
                        del subprocesses[device]
                        device_states[device] = 'inactive'
            
            if devices_to_cleanup:
                debug_log(f"Monitor: Cleaned up {len(devices_to_cleanup)} processes")
                        
        except Exception as e:
            streaming_logger.error(f"Error in process monitor: {e}")
        
        time.sleep(5)  # Check every 5 seconds

# Start the monitor thread when the module loads
monitor_thread = threading.Thread(target=monitor_processes, daemon=True)
monitor_thread.start()

@streaming_bp.route('/cleanup_processes', methods=['POST'])
def cleanup_processes():
    """Manual cleanup endpoint"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Not authorized'}), 401
    
    try:
        cleaned = []
        with process_lock:
            for device, proc in list(subprocesses.items()):
                if proc.poll() is not None:
                    del subprocesses[device]
                    device_states[device] = 'inactive'
                    cleaned.append(device)
        
        return jsonify({'cleaned': cleaned, 'message': f'Cleaned up {len(cleaned)} processes'})
    except Exception as e:
        streaming_logger.error(f"Error cleaning processes: {e}")
        return jsonify({'error': str(e)}), 500

def extract_resolution_and_frame_rate(format_string):
    """Extract resolution and frame rate from format string"""
    match = re.search(r'(\d+x\d+) at (\d+/\d+)', format_string)
    if match:
        resolution = match.group(1)
        frame_rate = match.group(2)
        return resolution, frame_rate
    return None, None

@streaming_bp.route('/streaming', methods=['GET', 'POST'])
def decklink_control():
    """Main streaming control page"""
    if not session.get('logged_in'):
        return redirect('/streaming_login')

    message = ''
    command = ''
    
    if request.method == 'POST' and 'action' in request.form:
        action = request.form['action']
        device = request.form['device']
        input_type = request.form.get('input_type')
        input_key = request.form.get('input_key')
        port_number = request.form.get('port_number')
        resolution_frame_rate = request.form.get('resolution_frame_rate')
        channel_id = request.form.get('channel_id')
        
        if action == 'start':
            if input_type and input_key and port_number and resolution_frame_rate:
                if input_type == "RTMP":
                    input_url = f"rtmp://27.131.14.34:{port_number}/live/{input_key}"
                else:
                    input_url = f"srt://27.131.14.34:{port_number}?streamid=play/stream/{input_key}"
                
                try:
                    # Pass additional parameters to start_command
                    message, command, pid = start_command(
                        device, 
                        input_url, 
                        resolution_frame_rate, 
                        channel_id,
                        # Pass additional parameters
                        input_type=input_type,
                        input_key=input_key,
                        port_number=port_number,
                        format=resolution_frame_rate
                    )
                except Exception as e:
                    streaming_logger.error(f"Exception in start_command: {str(e)}", exc_info=True)
                    message = f"Error starting stream: {str(e)}"
            else:
                message = "Please provide all the required details to start streaming."
        elif action == 'stop':
            try:
                message = stop_command(device, channel_id)
            except Exception as e:
                streaming_logger.error(f"Exception in stop_command: {str(e)}", exc_info=True)
                message = f"Error stopping stream: {str(e)}"
        
        # Refresh devices after action ONLY
        decklink_devices = get_decklink_devices()
    else:
        # For GET requests, get current device list WITHOUT resetting states
        with process_lock:
            decklink_devices = list(device_states.keys())

    return render_template('streaming.html', 
                         devices=decklink_devices,
                         message=message, 
                         command=command,
                         device_states=device_states,
                         formats=PRELOADED_FORMATS)

#Login_logout-refresh-reboot

@streaming_bp.route('/streaming_login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == 'admin' and password == 'password':
            session['logged_in'] = True
            return redirect('/streaming')
        else:
            return "Invalid credentials"
    return render_template_string('''
    <html>
    <body>
        <h2>Login</h2>
        <form method="post">
            <label for="username">Username:</label>
            <input type="text" id="username" name="username"><br><br>
            <label for="password">Password:</label>
            <input type="password" id="password" name="password"><br><br>
            <input type="submit" value="Login">
        </form>
    </body>
    </html>
    ''')

@streaming_bp.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect('/streaming_login')

def delayed_restart():
    """Wait a moment then restart to allow the HTTP response to be sent"""
    time.sleep(3)  # Give enough time for the HTML page to load in browser
    try:
        subprocess.run(
            ['/usr/bin/sudo', '/bin/systemctl', 'restart', 'flaskapp-modular.service'],
            capture_output=True,
            timeout=10
        )
    except:
        pass  # Ignore errors since we're shutting down anyway

@streaming_bp.route('/restart', methods=['POST'])
def restart_flaskapp():
    if not session.get('logged_in'):
        return redirect('/streaming_login')
    try:
        # Start the restart in a background thread with delay
        thread = threading.Thread(target=delayed_restart)
        thread.daemon = True
        thread.start()
        
        # Return HTML that will redirect client-side
        return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Restarting Flask App</title>
            <script>
                // Show restarting message for 5 seconds, then redirect to login
                setTimeout(function() {
                    window.location.href = '/streaming_login';
                }, 5000);
            </script>
        </head>
        <body style="font-family: Arial; text-align: center; padding: 50px;">
            <h2>🔄 Restarting Flask App...</h2>
            <p>Please wait while the application restarts.</p>
            <p>You will be automatically redirected to the login page in 5 seconds.</p>
            <p>If not redirected, <a href="/streaming_login">click here to go to login</a>.</p>
        </body>
        </html>
        '''
        
    except Exception as e:
        streaming_logger.error(f"Exception: {str(e)}")
        return f"Exception: {str(e)}"

@streaming_bp.route('/reboot', methods=['POST'])
def reboot():
    if not session.get('logged_in'):
        return redirect('/streaming_login')
    try:
        result = subprocess.run(['/usr/bin/sudo', '/usr/sbin/reboot'], capture_output=True, text=True)
        if result.returncode == 0:
            streaming_logger.info("Reboot command executed successfully.")
            return "Rebooting server..."
        else:
            streaming_logger.error(f"Error: {result.stderr}")
            return f"Error: {result.stderr}"
    except Exception as e:
        streaming_logger.error(f"Exception: {str(e)}")
        return str(e)








#HLS Preview parts:

@streaming_bp.route('/preview_streams', methods=['GET'])
def get_preview_streams():
    """Return available preview streams"""
    return jsonify(PREVIEW_STREAMS)

@streaming_bp.route('/start_preview', methods=['POST'])
def start_preview():
    """Start HLS conversion for preview - OPTIMIZED VERSION"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Not authorized'}), 401
    
    data = request.json
    channel_id = data.get('channel_id')
    input_type = data.get('input_type')
    input_key = data.get('input_key')
    port = data.get('port', '1935')
    
    if not all([channel_id, input_type, input_key]):
        return jsonify({'error': 'Missing parameters'}), 400
    
    # Save HLS state
    with hls_state_lock:
        hls_states[channel_id] = {
            'input_type': input_type,
            'input_key': input_key,
            'port': port,
            'playing': True,
            'start_time': datetime.datetime.now().isoformat()
        }
    
    # Stop existing preview if any - WITH ERROR HANDLING
    try:
        # Don't call the broken stop_preview, clean up manually
        with hls_lock:
            if channel_id in hls_processes:
                proc = hls_processes[channel_id]
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except:
                    proc.kill()
                    proc.wait()
                del hls_processes[channel_id]
            
            if channel_id in hls_output_dirs:
                shutil.rmtree(hls_output_dirs[channel_id], ignore_errors=True)
                del hls_output_dirs[channel_id]
    except Exception as e:
        streaming_logger.warning(f"Error cleaning up previous preview for {channel_id}: {e}")
    
    # Generate input URL
    if input_type == "RTMP":
        input_url = f"rtmp://27.131.14.34:{port}/live/{input_key}"
    else:
        input_url = f"srt://27.131.14.34:{port}?streamid=play/stream/{input_key}"
    
    # Create temp directory for HLS files
    temp_dir = tempfile.mkdtemp(prefix=f"preview_{channel_id}_")

    with hls_lock:
        hls_output_dirs[channel_id] = temp_dir
    
    hls_playlist = Path(temp_dir) / "stream.m3u8"
    

# OPTIMIZED FFmpeg command for low-res, smooth preview
    command = [
    FFMPEG_PATH,

    # Input
    "-i", input_url,

    # ↓ Scale down for low bandwidth preview
    "-vf", "scale=426:240",         # 240p for very smooth playback

    # ↓ H.264 settings optimized for preview responsiveness
    "-c:v", "libx264",
    "-preset", "veryfast",          # faster encoding than ultrafast but better quality
    "-tune", "zerolatency",

    # ↓ Quality/bitrate controls
    "-g", "50",                     # GOP size (affects keyframe frequency)
    "-sc_threshold", "0",
    "-crf", "28",                   # quality level (lower = higher quality)
    "-maxrate", "500k",             # lower bitrate for smoother playback
    "-bufsize", "1000k",

    "-r", "20",                     # slightly lower fps for smoother output on all devices

    # ↓ Audio
    "-c:a", "aac",
    "-b:a", "48k",

    # ↓ HLS settings for smooth preview (not ultra-low latency)
    "-hls_time", "2",               # segment length in seconds
    "-hls_list_size", "6",
    "-hls_flags", "delete_segments+append_list",
    "-hls_segment_filename", f"{temp_dir}/segment_%03d.ts",

    "-f", "hls",
    str(hls_playlist)
    ]

    
    try:
        streaming_logger.info(f"Starting OPTIMIZED HLS conversion: {' '.join(command)}")
        
        # Start process with stderr capture to see FFmpeg errors
        proc = subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        
        with hls_lock:
            hls_processes[channel_id] = proc
        
        # Wait for HLS to initialize with better checking
        max_attempts = 15  # Increased attempts
        for attempt in range(max_attempts):
            time.sleep(1)
            
            # Check if process died early
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                streaming_logger.error(f"HLS process died early. STDOUT: {stdout}, STDERR: {stderr}")
                # Cleanup WITH LOCK
                with hls_lock:
                    if channel_id in hls_output_dirs:
                        shutil.rmtree(hls_output_dirs[channel_id], ignore_errors=True)
                        del hls_output_dirs[channel_id]
                return jsonify({'error': f'HLS conversion failed: {stderr}'}), 500
            
            if hls_playlist.exists():
                # Check if playlist has content and segments exist
                with open(hls_playlist, 'r') as f:
                    content = f.read()
                    if '#EXTINF' in content:
                        # Verify at least one segment file exists
                        segment_files = list(Path(temp_dir).glob('segment*.ts'))
                        if segment_files:
                            streaming_logger.info(f"HLS preview started for channel {channel_id} - Found {len(segment_files)} segments")
                            
                            # Debug: List created files
                            created_files = list(Path(temp_dir).glob('*'))
                            streaming_logger.info(f"HLS files created: {[f.name for f in created_files]}")
                            
                            return jsonify({
                                'success': True,
                                'hls_url': f'/hls/{channel_id}/stream.m3u8',
                                'message': 'HLS preview started'
                            })
        
        # If we get here, HLS never started properly
        streaming_logger.error(f"HLS preview timeout for channel {channel_id}")
        
        # Get any error output from FFmpeg
        try:
            stdout, stderr = proc.communicate(timeout=5)
            streaming_logger.error(f"FFmpeg output - STDOUT: {stdout}, STDERR: {stderr}")
        except:
            proc.kill()
            stdout, stderr = proc.communicate()
        
        # Cleanup timeout WITH LOCK
        with hls_lock:
            if channel_id in hls_output_dirs:
                shutil.rmtree(hls_output_dirs[channel_id], ignore_errors=True)
                del hls_output_dirs[channel_id]
            if channel_id in hls_processes:
                del hls_processes[channel_id]
                
        return jsonify({'error': f'HLS conversion timeout: {stderr}'}), 500
        
    except Exception as e:
        streaming_logger.error(f"Error starting HLS preview: {e}")
        # Cleanup on error WITH LOCK
        with hls_lock:
            if channel_id in hls_output_dirs:
                shutil.rmtree(hls_output_dirs[channel_id], ignore_errors=True)
                del hls_output_dirs[channel_id]
        return jsonify({'error': str(e)}), 500

@streaming_bp.route('/hls_status/<channel_id>')
def hls_status(channel_id):
    """Check HLS conversion status"""
    with hls_lock:  # Add lock for entire function
        if channel_id not in hls_output_dirs:
            return jsonify({'status': 'not_running'})
        
        hls_dir = hls_output_dirs[channel_id]
        playlist_path = Path(hls_dir) / "stream.m3u8"
        
        status = {
            'status': 'running',
            'playlist_exists': playlist_path.exists(),
            'process_running': channel_id in hls_processes and hls_processes[channel_id].poll() is None,
            'hls_dir': hls_dir,
            'files_in_dir': []
        }
        
        if Path(hls_dir).exists():
            status['files_in_dir'] = [f.name for f in Path(hls_dir).glob('*')]
        
        if playlist_path.exists():
            try:
                with open(playlist_path, 'r') as f:
                    content = f.read()
                    status['playlist_content'] = content
                    status['has_segments'] = '#EXTINF' in content
            except Exception as e:
                status['playlist_error'] = str(e)
        
        # Check process status
        if channel_id in hls_processes:
            proc = hls_processes[channel_id]
            status['process_returncode'] = proc.poll()
        
        return jsonify(status)

@streaming_bp.route('/stop_preview/<channel_id>', methods=['POST'])
def stop_preview(channel_id):
    """Stop HLS conversion and cleanup"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Not authorized'}), 401
    
    # Update HLS state
    with hls_state_lock:
        if channel_id in hls_states:
            hls_states[channel_id]['playing'] = False
    
    try:
        proc = None
        # Stop FFmpeg process with robust termination
        with hls_lock:  # Add lock for reading
            if channel_id in hls_processes:
                proc = hls_processes[channel_id]
        
        if proc:  # Only if we found a process
            # Hierarchical termination
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            
            with hls_lock:  # Add lock for writing
                del hls_processes[channel_id]
        
        # Cleanup HLS files
        with hls_lock:  # Add lock
            if channel_id in hls_output_dirs:
                shutil.rmtree(hls_output_dirs[channel_id], ignore_errors=True)
                del hls_output_dirs[channel_id]
        
        streaming_logger.info(f"Stopped HLS preview for channel {channel_id}")
        return jsonify({'success': True, 'message': 'Preview stopped'})
        
    except Exception as e:
        streaming_logger.error(f"Error stopping preview: {e}")
        return jsonify({'error': str(e)}), 500

@streaming_bp.route('/hls_states', methods=['GET'])
def get_hls_states():
    """Get current HLS preview states"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Not authorized'}), 401
    
    with hls_state_lock:
        return jsonify(hls_states)

@streaming_bp.route('/hls/<channel_id>/<filename>')
def serve_hls(channel_id, filename):
    """Serve HLS files with proper caching and error handling"""
    with hls_lock:  # Keep lock for entire operation
        if channel_id not in hls_output_dirs:
            streaming_logger.warning(f"HLS directory not found for channel {channel_id}")
            return "Not found", 404
        
        hls_dir = hls_output_dirs[channel_id]
    file_path = Path(hls_dir) / filename
    
    if not file_path.exists():
        streaming_logger.warning(f"HLS file not found: {file_path}")
        return "Not found", 404
    
    try:
        # Set appropriate MIME type and caching
        if filename.endswith('.m3u8'):
            mimetype = 'application/vnd.apple.mpegurl'
            # No cache for playlist - it changes frequently
            cache_control = 'no-cache, no-store, must-revalidate'
        elif filename.endswith('.ts'):
            mimetype = 'video/MP2T'
            # Cache segments for a short time
            cache_control = 'public, max-age=2'
        else:
            mimetype = 'application/octet-stream'
            cache_control = 'no-cache'
        
        response = send_file(
            file_path, 
            mimetype=mimetype,
            as_attachment=False
        )
        response.headers['Cache-Control'] = cache_control
        response.headers['Access-Control-Allow-Origin'] = '*'
        
        return response
        
    except Exception as e:
        streaming_logger.error(f"Error serving HLS file {filename}: {e}")
        return "Server error", 500
