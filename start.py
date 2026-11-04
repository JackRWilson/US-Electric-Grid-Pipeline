import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EIA_API_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
REQUIRED_SERVICES = {"db", "data-init", "airflow", "app"}
SERVICES = {
    "Airflow": "http://localhost:8080",
    "Dashboard": "http://localhost:8050",
}


def compose(*args, capture_output=False, check=True):
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=check,
        capture_output=capture_output,
        text=True,
    )


def env_value(env_path, name):
    for line in env_path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() == name:
            return value.strip().strip("\"'")
    return ""


def ensure_env_file():
    env_path = ROOT / ".env"
    if not env_path.exists():
        shutil.copyfile(ROOT / ".env.example", env_path)
        print("Created .env. Set EIA_API_KEY in that file, then run start.py again.")
        return False

    api_key = env_value(env_path, "EIA_API_KEY")
    if not api_key or api_key.lower() == "eia_api_key":
        print("Set your EIA_API_KEY in .env before starting the pipeline.")
        return False
    return True


def validate_eia_api_key(api_key, timeout=15):
    params = urllib.parse.urlencode(
        [
            ("api_key", api_key),
            ("frequency", "hourly"),
            ("data[0]", "value"),
            ("facets[type][]", "D"),
            ("start", "2026-01-01T00"),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "desc"),
            ("length", "1"),
            ("offset", "0"),
        ]
    )
    request = urllib.request.Request(
        f"{EIA_API_URL}?{params}",
        headers={"Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            body = json.loads(error.read())
        except (json.JSONDecodeError, OSError):
            body = {}
        message = str(body.get("error", "")).lower()
        if error.code in (401, 403) or any(
            phrase in message
            for phrase in ("api key", "api_key", "invalid key", "unauthorized")
        ):
            return False, "EIA rejected the API key."
        return None, f"EIA key check returned HTTP {error.code}."
    except (urllib.error.URLError, TimeoutError, OSError):
        return None, "Could not reach EIA to verify the API key."
    except json.JSONDecodeError:
        return None, "EIA returned an unreadable response while checking the key."

    error = payload.get("error")
    if error:
        if isinstance(error, dict):
            error = error.get("message", "")
        message = str(error).lower()
        if any(
            phrase in message
            for phrase in ("api key", "api_key", "invalid key", "unauthorized")
        ):
            return False, "EIA rejected the API key."
        return None, "EIA returned an error while checking the API key."
    if isinstance(payload.get("response"), dict):
        return True, "EIA API key is valid."
    return None, "EIA returned an unexpected response while checking the key."


def start_stack(force_build):
    existing_services = set(
        compose("ps", "--all", "--services", capture_output=True).stdout.split()
    )
    if not force_build and REQUIRED_SERVICES.issubset(existing_services):
        print("Starting the existing containers...")
        compose("start", "db", "airflow", "app")
    else:
        print("Building and starting the containers...")
        compose("up", "-d", "--build")


def check_service_until_ready(url, deadline):
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return time.monotonic()
        except urllib.error.HTTPError:
            return time.monotonic()
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.5)

    raise TimeoutError(f"Service at {url} did not become available.")


def wait_for_services(services, timeout=600):
    stream = sys.stdout
    interactive = stream.isatty()
    use_color = interactive and "NO_COLOR" not in os.environ
    use_checkmark = interactive
    if use_checkmark and stream.encoding:
        try:
            "✓".encode(stream.encoding)
        except UnicodeEncodeError:
            try:
                stream.reconfigure(encoding="utf-8")
            except (AttributeError, OSError, ValueError):
                use_checkmark = False

    start_time = time.monotonic()
    service_states = [
        {"name": name, "url": url, "ready_at": None} for name, url in services.items()
    ]
    status_column = max(
        len(f"{state['name']} at {state['url']}") for state in service_states
    )
    cursor_saved = False

    def color(value, code):
        return f"\033[{code}m{value}\033[0m" if use_color else value

    def render():
        nonlocal cursor_saved
        completed = sum(state["ready_at"] is not None for state in service_states)
        lines = [f"[+] start {completed}/{len(service_states)}"]
        now = time.monotonic()
        for state in service_states:
            ready_at = state["ready_at"]
            elapsed = (ready_at or now) - start_time
            elapsed_text = f"{elapsed:.1f}s"
            label = f"{state['name']} at {state['url']}"
            status_padding = " " * (status_column - len(label) + 2)
            if ready_at is None:
                plain_marker = "-"
                marker = color(plain_marker, "33")
                status = "Waiting"
                status_text = f"{label}{status_padding}{status}"
            else:
                plain_marker = "✓" if use_checkmark else "[OK]"
                marker = color(plain_marker, "32")
                status_text = f"{label}{status_padding}" f"{color('Started', '32')}"
            width = shutil.get_terminal_size((100, 20)).columns
            padding = max(
                2,
                width
                - len(f" {plain_marker} {label}{status_padding}")
                - len("Waiting" if ready_at is None else "Started")
                - len(elapsed_text),
            )
            line = (
                f" {marker} {status_text}{' ' * padding}" f"{color(elapsed_text, '34')}"
            )
            lines.append(line)

        if interactive:
            if cursor_saved:
                stream.write("\033[u")
            else:
                stream.write("\033[s")
                cursor_saved = True
            for line in lines:
                stream.write(f"\033[2K\r{line}\n")
            stream.flush()
        else:
            if not cursor_saved:
                print(lines[0])
                for state, line in zip(service_states, lines[1:]):
                    state["last_rendered"] = line
                    print(line)
                cursor_saved = True
            else:
                ready_states = [
                    state
                    for state in service_states
                    if state["ready_at"] is not None and not state.get("reported_ready")
                ]
                if ready_states:
                    print(lines[0])
                    for state in ready_states:
                        index = service_states.index(state)
                        print(lines[index + 1])
                        state["reported_ready"] = True

    deadline = start_time + timeout
    with ThreadPoolExecutor(max_workers=len(service_states)) as executor:
        future_states = {
            executor.submit(
                check_service_until_ready,
                state["url"],
                deadline,
            ): state
            for state in service_states
        }
        pending = set(future_states)
        render()

        while pending:
            completed, pending = wait(
                pending,
                timeout=0.1,
                return_when=FIRST_COMPLETED,
            )
            for future in completed:
                state = future_states[future]
                try:
                    state["ready_at"] = future.result()
                except TimeoutError:
                    waiting = [
                        item["name"]
                        for item in service_states
                        if item["ready_at"] is None
                    ]
                    raise TimeoutError(
                        f"Services did not become available within {timeout} seconds: "
                        f"{', '.join(waiting)}"
                    ) from None
            render()


def print_airflow_credentials():
    result = compose(
        "exec",
        "-T",
        "airflow",
        "sh",
        "-c",
        "cat /opt/airflow/simple_auth_manager_passwords.json.generated",
        capture_output=True,
        check=False,
    )
    print("\nAirflow credentials:")
    if result.returncode == 0 and result.stdout.strip():
        print(result.stdout.strip())
    else:
        print("Could not read the generated credentials file.")
        if result.stderr.strip():
            print(result.stderr.strip())


def stop_stack():
    print("\nStopping containers (persistent data is kept)...")
    compose("stop")
    print("Containers stopped. Run start.py again to resume them.")


def main():
    parser = argparse.ArgumentParser(description="Start the electricity pipeline.")
    parser.add_argument(
        "--build",
        action="store_true",
        help="Rebuild images instead of resuming existing containers.",
    )
    parser.add_argument(
        "--skip-eia-check",
        action="store_true",
        help="Start without verifying the EIA API key (for example, if EIA is unavailable).",
    )
    args = parser.parse_args()

    if not ensure_env_file():
        return 1

    if not args.skip_eia_check:
        api_key = env_value(ROOT / ".env", "EIA_API_KEY")
        valid, message = validate_eia_api_key(api_key)
        if valid is not True:
            print(message, file=sys.stderr)
            if valid is False:
                print("Correct EIA_API_KEY in .env before starting.", file=sys.stderr)
            else:
                print(
                    "If you still want to start, rerun with --skip-eia-check.",
                    file=sys.stderr,
                )
            return 1
        print(message)

    may_be_running = True
    try:
        start_stack(args.build)
        wait_for_services(SERVICES)

        print()
        for name, url in SERVICES.items():
            print(f"{name}: {url}")
        print_airflow_credentials()
        for url in SERVICES.values():
            webbrowser.open(url)

        if sys.stdin.isatty():
            input("\nPress Enter to stop the containers (or Ctrl+C)...")
            stop_stack()
            may_be_running = False
        else:
            print("\nContainers will keep running. Stop them with: docker compose stop")
    except KeyboardInterrupt:
        print()
        if may_be_running:
            stop_stack()
        return 130
    except (subprocess.CalledProcessError, TimeoutError, FileNotFoundError) as error:
        print(f"Could not start the pipeline: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr.strip(), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
