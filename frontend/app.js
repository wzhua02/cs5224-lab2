const API = window.API_BASE.replace(/\/$/, "");
const $ = (id) => document.getElementById(id);

async function api(path, options) {
  const res = await fetch(API + path, options);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
  return body;
}

function setText(id, text, isError = false) {
  $(id).textContent = text;
  $(id).classList.toggle("error", isError);
}

async function checkBackend() {
  try {
    const h = await api("/api/health");
    setText("backend-status", h.loaded_run
      ? `Backend OK — results from run ${h.loaded_run} (${h.users} users)`
      : "Backend OK — no results yet, run the analysis first");
  } catch (e) {
    setText("backend-status", `Backend unreachable at ${API}: ${e.message}`, true);
  }
}

async function pollJob(jobId) {
  const job = await api(`/api/jobs/${jobId}`);
  setText("job-status", `Job ${jobId}: ${job.state} ${job.details || ""}`, job.state === "FAILED");
  if (["SUCCESS", "FAILED", "CANCELLED"].includes(job.state)) {
    $("run-job").disabled = false;
    checkBackend();
    return;
  }
  setTimeout(() => pollJob(jobId).catch((e) => setText("job-status", e.message, true)), 10000);
}

$("run-job").addEventListener("click", async () => {
  $("run-job").disabled = true;
  try {
    const { job_id } = await api("/api/jobs", { method: "POST" });
    await pollJob(job_id);
  } catch (e) {
    setText("job-status", e.message, true);
    $("run-job").disabled = false;
  }
});

$("lookup").addEventListener("click", async () => {
  const ids = $("user-ids").value.split(",").map((s) => s.trim()).filter(Boolean).join(",");
  try {
    const { users } = await api(`/api/users?ids=${encodeURIComponent(ids)}`);
    setText("results", users
      .map((u) => `${u.user_id} has ${u.followers} followers\n${u.user_id} has ${u.followees} followees`)
      .join("\n"));
  } catch (e) {
    setText("results", e.message, true);
  }
});

checkBackend();
