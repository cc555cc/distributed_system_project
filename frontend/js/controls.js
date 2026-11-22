const edgeTable = document.getElementById("edge-table");
const messageBox = document.getElementById("message");
const resetButton = document.getElementById("reset-button");

function showMessage(text, isError) {
  messageBox.textContent = text;
  messageBox.className = isError ? "message error" : "message";
}

function drawWorkers(workers) {
  const rows = workers.map(function (worker) {
    const failed = worker.state === "failed";
    const buttons =
      '<button data-id="' + worker.edge_id + '" data-action="fail"' + (failed ? " disabled" : "") + ">Fail</button> " +
      '<button data-id="' + worker.edge_id + '" data-action="recover"' + (failed ? "" : " disabled") + ">Recover</button>";
    return [
      worker.edge_id,
      worker.region_id,
      tag(worker.state, failed ? "bad" : "good"),
      Math.round(worker.queue_utilization_percent) + "%",
      worker.assigned_buses.length,
      buttons
    ];
  });
  edgeTable.innerHTML = tableHtml(["Edge server", "Region", "State", "Queue", "Buses", "Action"], rows);
}

async function loadWorkers() {
  try {
    const simulation = await getSimulation();
    drawWorkers(simulation.workers);
  } catch (error) {
    edgeTable.innerHTML = "<p>Could not load the edge servers. Is the server running?</p>";
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
