import subprocess
import platform
import psutil
import os

def run(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL).decode().strip()
    except:
        return "N/A"

print("=" * 50)
print("SYSTEM INFO")
print("=" * 50)

# OS
print(f"\n--- OS ---")
print(f"System:   {platform.system()}")
print(f"Release:  {platform.release()}")
print(f"Version:  {platform.version()}")
print(f"Arch:     {platform.machine()}")

# CPU
print(f"\n--- CPU ---")
print(run("lscpu | grep -E 'Model name|Core\(s\) per socket|Socket\(s\)|CPU max MHz|Thread'"))

# RAM
print(f"\n--- MEMORY ---")
print(run("free -h"))

# Disk
print(f"\n--- DISK ---")
print(run("df -h --total | grep -E 'Filesystem|total'"))

# GPU - nvidia
print(f"\n--- GPU (nvidia-smi) ---")
gpu_info = run("""nvidia-smi --query-gpu=name,driver_version,memory.total,memory.free,memory.used,temperature.gpu,utilization.gpu,compute_cap --format=csv,noheader""")
if gpu_info != "N/A":
    headers = ["Name", "Driver", "VRAM Total", "VRAM Free", "VRAM Used", "Temp (C)", "Util (%)", "Compute Cap"]
    for line in gpu_info.split("\n"):
        values = [v.strip() for v in line.split(",")]
        for h, v in zip(headers, values):
            print(f"  {h:<15}: {v}")
        print()
else:
    print("  nvidia-smi not found")

# GPU - CUDA via PyTorch (daca e instalat)
print(f"--- GPU (PyTorch CUDA) ---")
try:
    import torch
    if torch.cuda.is_available():
        for i in range(torch.cuda.device_count()):
            props = torch.cuda.get_device_properties(i)
            print(f"  Device {i}:       {props.name}")
            print(f"  VRAM:           {props.total_memory / 1024**3:.1f} GB")
            print(f"  CUDA Cores:     {props.multi_processor_count} SMs")
            print(f"  CUDA Version:   {torch.version.cuda}")
            print(f"  cuDNN Version:  {torch.backends.cudnn.version()}")
            print(f"  Compute Cap:    {props.major}.{props.minor}")
    else:
        print("  CUDA not available in PyTorch")
except ImportError:
    print("  PyTorch not installed")

# Python + pachete relevante
print(f"\n--- PYTHON & PACKAGES ---")
print(f"Python: {platform.python_version()}")
for pkg in ["torch", "transformers", "faster_whisper", "numpy", "accelerate", "peft"]:
    try:
        mod = __import__(pkg.replace("-", "_"))
        ver = getattr(mod, "__version__", "installed")
        print(f"  {pkg:<20}: {ver}")
    except ImportError:
        print(f"  {pkg:<20}: not installed")

print("\n" + "=" * 50)