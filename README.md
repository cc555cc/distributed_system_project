# Distributed Transit System

A public-transit simulation: simulated buses send telemetry over MQTT to regional edge servers, which forward it to a central server with a web interface.

These steps are for Windows with PowerShell. Everything runs in containers, so you do not need Python unless you want to run the tests.

## 1. Install the tools

```powershell
winget install -e --id Git.Git
winget install -e --id Podman.CLI
winget install -e --id Docker.DockerCompose
```

`Docker.DockerCompose` is the program that `podman compose` uses to read the compose file. It does not require Docker Desktop.

Close PowerShell and open a new window so the new commands are found.

## 2. Set up Podman

Podman runs containers inside a small Linux virtual machine. Create it once:

```powershell
podman machine init
```

Then start it. Repeat this after every reboot:

```powershell
podman machine start
```

If `init` says the machine already exists, or `start` says it is already running, continue to the next step. If Podman reports that WSL is missing, run `wsl --install` as administrator and reboot.

## 3. Download the project

```powershell
git clone https://github.com/cc555cc/distributed_system_project.git
cd distributed_system_project
```

Run all remaining commands from this folder.

## 4. Start the servers

```powershell
podman compose -f infrastructure/compose.yaml up -d --build
```

This starts the MQTT broker, the database, the central server and four edge servers. The first run downloads and builds the images, which takes a few minutes. The command returns when the servers have started.

Check that they are running:

```powershell
podman compose -f infrastructure/compose.yaml ps
```

You should see seven services: `mosquitto`, `postgres`, `central`, `edge-west-1`, `edge-west-2`, `edge-east-1` and `edge-east-2`.

## 5. Start the buses

Open a second PowerShell window in the project folder:

```powershell
podman compose -f infrastructure/compose.yaml run --rm buses
```

Twenty simulated buses start printing their readings. Leave this window open. Press `Ctrl+C` to stop them.

## 6. Open the web pages

1. Go to <http://localhost:8000/login.html> and sign in with username `ali` and password `123`.
2. Open <http://localhost:8000/control.html>. Click a simulation from the options. Monitor using the [health html page](http://localhost:8000/health.html).
3. Open <http://localhost:8000/user.html> to see the buses on the passenger map.

| Page | Purpose | Login needed |
| --- | --- | --- |
| [user.html](http://localhost:8000/user.html) | Passenger map and arrivals | No |
| [control.html](http://localhost:8000/control.html) | Simulation controls | Yes |
| [health.html](http://localhost:8000/health.html) | Bus, sensor and server health | Yes |
| [operator.html](http://localhost:8000/operator.html) | Operator overview | Yes |

## 7. Stop everything

Press `Ctrl+C` in the bus window, then run:

```powershell
podman compose -f infrastructure/compose.yaml down
```

The database resets each time.

## Run the tests (optional)

The tests run directly on your computer and do not need the containers. Install Python, then open a new PowerShell window in the project folder:

```powershell
winget install -e --id Python.Python.3.12
```

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt pytest httpx
.venv\Scripts\python -m pytest services tests -q
```

The run takes about two minutes. Some tests are skipped because they need dedicated live services.

## Troubleshooting

| Problem | What to do |
| --- | --- |
| `podman` is not recognized | Open a new PowerShell window. |
| `Cannot connect to Podman` | Run `podman machine start`. |
| `podman compose` cannot find a compose provider | Install `Docker.DockerCompose` (step 1) and open a new window. |
| Port already in use, or the web page shows something else | The project needs ports 8000, 1883 and 5433. Find the program using one with `Get-Process -Id (Get-NetTCPConnection -LocalPort 8000 -State Listen).OwningProcess`, changing the port number as needed, and close it. |
| A server keeps restarting or a page does not load | Read the logs with `podman compose -f infrastructure/compose.yaml logs central`. |

Using Docker Desktop instead of Podman? Skip step 2 and type `docker compose` wherever these steps say `podman compose`.
