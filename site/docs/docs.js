for (const block of document.querySelectorAll(".code-block")) {
  const button = block.querySelector("button");
  if (!navigator.clipboard?.writeText) continue;

  button.hidden = false;
  let reset;
  button.addEventListener("click", async () => {
    clearTimeout(reset);
    try {
      await navigator.clipboard.writeText(block.querySelector("code").textContent);
      button.textContent = "Скопировано";
    } catch {
      button.textContent = "Выделить текст";
      const range = document.createRange();
      range.selectNodeContents(block.querySelector("code"));
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
    }
    reset = setTimeout(() => { button.textContent = "Копировать"; }, 1800);
  });
}

const links = [...document.querySelectorAll(".docs-sidebar nav a")];
const sections = links.map((link) => document.getElementById(link.hash.slice(1)));

function activeSection() {
  let index = 0;
  for (const [position, section] of sections.entries()) {
    if (section.getBoundingClientRect().top <= 75) index = position;
  }
  for (const [position, link] of links.entries()) {
    if (position === index) link.setAttribute("aria-current", "location");
    else link.removeAttribute("aria-current");
  }
}

window.addEventListener("scroll", activeSection, { passive: true });
window.addEventListener("hashchange", activeSection);
activeSection();

const balanced = document.querySelector('[data-strategy="Balanced"]');
const weight = document.getElementById("latency-weight");
const scores = [...balanced.querySelectorAll(".chart-row")];
const values = (strategy) => [...document.querySelectorAll(`[data-strategy="${strategy}"] [data-value]`)]
  .map((value) => Number(value.dataset.value));
const normalize = (items) => {
  const min = Math.min(...items);
  const max = Math.max(...items);
  return items.map((item) => max === min ? 0 : (item - min) / (max - min));
};
const latency = normalize(values("LowestLatency"));
const cost = normalize(values("LowestCost"));
const format = new Intl.NumberFormat("ru", { minimumFractionDigits: 3, maximumFractionDigits: 3 });

function updateScores() {
  const latencyWeight = weight.valueAsNumber / 100;
  const estimates = latency.map((delay, index) => latencyWeight * delay + (1 - latencyWeight) * cost[index]);
  const best = estimates.indexOf(Math.min(...estimates));
  for (const [index, row] of scores.entries()) {
    row.classList.toggle("preferred", index === best);
    row.querySelector(".chart-bar").style.setProperty("--fill", `${estimates[index] * 100}%`);
    row.querySelector("output").textContent = format.format(estimates[index]);
  }
  document.getElementById("latency-weight-value").value = `${weight.value}%`;
  balanced.querySelector(".chart-result").textContent = `${"ABC"[best]} — минимальная оценка`;
}

weight.closest(".chart-control").hidden = false;
weight.addEventListener("input", updateScores);
updateScores();
