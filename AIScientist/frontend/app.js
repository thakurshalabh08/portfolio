const form = document.getElementById("query-form");
const statusEl = document.getElementById("status");
const summaryEl = document.getElementById("summary");
const entriesEl = document.getElementById("entries");
const hypothesesEl = document.getElementById("hypotheses");
const tasksEl = document.getElementById("tasks");
const interpretationEl = document.getElementById("interpretation");

const renderList = (container, items, renderer) => {
  container.innerHTML = "";
  if (!items.length) {
    container.innerHTML = "<p class=\"empty\">No items yet.</p>";
    return;
  }
  items.forEach((item) => {
    const element = renderer(item);
    container.appendChild(element);
  });
};

const createCard = (title, details) => {
  const article = document.createElement("article");
  article.className = "mini-card";
  const heading = document.createElement("h3");
  heading.textContent = title;
  const body = document.createElement("p");
  body.textContent = details;
  article.appendChild(heading);
  article.appendChild(body);
  return article;
};

const createBullet = (text) => {
  const li = document.createElement("li");
  li.textContent = text;
  return li;
};

const setStatus = (message, state = "") => {
  statusEl.textContent = message;
  statusEl.className = `status ${state}`.trim();
};

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const query = document.getElementById("query").value.trim();
  const organism = document.getElementById("organism").value.trim();
  const focus = document.getElementById("focus").value.trim();

  if (!query) {
    setStatus("Please enter a query.", "error");
    return;
  }

  setStatus("Analyzing UniProt data...", "loading");

  try {
    const response = await fetch("/api/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, organism, focus }),
    });

    if (!response.ok) {
      const errorPayload = await response.json();
      throw new Error(errorPayload.detail || "Request failed");
    }

    const data = await response.json();
    summaryEl.textContent = data.summary;

    renderList(entriesEl, data.entries, (entry) =>
      createCard(
        `${entry.protein_name || entry.id} (${entry.accession})`,
        `${entry.organism || "Unknown organism"} · ${entry.gene || "No gene"}\n${
          entry.function || "No function available."
        }`
      )
    );

    renderList(hypothesesEl, data.hypotheses, (hypothesis) =>
      createCard(hypothesis.statement, hypothesis.rationale)
    );

    renderList(tasksEl, data.tasks, (task) =>
      createCard(task.task, task.data_needed)
    );

    interpretationEl.innerHTML = "";
    if (data.interpretation.length) {
      data.interpretation.forEach((line) => {
        interpretationEl.appendChild(createBullet(line));
      });
    } else {
      interpretationEl.appendChild(createBullet("No interpretation cues yet."));
    }

    setStatus("Analysis ready.", "success");
  } catch (error) {
    console.error(error);
    setStatus(`Error: ${error.message}`, "error");
  }
});
