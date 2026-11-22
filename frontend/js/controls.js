const edgeTable = document.getElementById("edge-table");
const messageBox = document.getElementById("message");
const resetButton = document.getElementById("reset-button");

function showMessage(text, isError) {
  messageBox.textContent = text;
  messageBox.className = isError ? "message error" : "message";
}

async function loadWorkers() {
  try {
    const simulation = await getSimulation();
    edgeTable.innerHTML = simulationTable(simulation, true);
    document.getElementById("simulation-clock").textContent = simulationClockText(simulation);
  } catch (error) {
    console.error("Could not load simulation", error);
    edgeTable.textContent = "Could not load the edge servers: " + error.message;
  }
}

edgeTable.addEventListener("click", async function (event) {
  const button = event.target.closest("button");
  if (!button) {
    return;
  }
  const edgeId = button.dataset.id;

  try {
    if (button.dataset.action === "fail") {
      await failEdge(edgeId);
      showMessage(edgeId + " was failed.", false);
    } else {
      await recoverEdge(edgeId);
      showMessage(edgeId + " is recovering.", false);
    }
  } catch (error) {
    showMessage("Could not do that: " + error.message, true);
  }
  loadWorkers();
});

document.getElementById("clock-controls").addEventListener("click", async function (event) {
  const button = event.target.closest("button");
  if (!button) return;
  try {
    await setSimulationClock(button.dataset.running === "true", Number(document.getElementById("clock-speed").value));
    await loadWorkers();
  } catch (error) {
    showMessage(error.message, true);
  }
});

document.getElementById("bus-controls").addEventListener("click", async function (event) {
  const button = event.target.closest("button[data-command]");
  if (!button) return;
  const action = button.dataset.command;
  const params = {};
  if (action === "set_demand") params.demand = document.getElementById("bus-demand").value;
  if (action === "set_traffic") params.traffic_factor = Number(document.getElementById("bus-traffic").value);
  if (action === "set_sensor_failure_rate") params.sensor_failure_rate = Number(document.getElementById("bus-fault").value);
  button.disabled = true;
  try {
    const result = await sendBusCommand(document.getElementById("bus-region").value, action, params);
    showMessage("Command published: " + action + " (" + result.command_id + "). Check live telemetry for the result.", false);
  } catch (error) {
    showMessage("Command failed: " + error.message, true);
  } finally {
    button.disabled = false;
  }
});

resetButton.addEventListener("click", async function () {
  try {
    await resetSimulation();
    showMessage("Simulation was reset.", false);
  } catch (error) {
    showMessage("Could not reset: " + error.message, true);
  }
  loadWorkers();
});

loadWorkers();
setInterval(loadWorkers, REFRESH_SECONDS * 1000);
