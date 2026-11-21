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
