const $ = (id) => document.getElementById(id);
const labels = { chat: "Chat Completions", messages: "Anthropic Messages" };
const statusLabels = { "built-in": "Встроенный адаптер", config: "Есть пример TOML", researched: "Изучен" };
const page = document.body.dataset.page;
const results = $("results");
const status = $("status");

function node(tag, value, className) {
  const element = document.createElement(tag);
  if (value !== undefined) element.textContent = value;
  if (className) element.className = className;
  return element;
}

function link(label, url) {
  const element = node("a", label);
  const target = new URL(url);
  if (target.protocol !== "https:") throw new Error("Ожидалась HTTPS-ссылка");
  element.href = target.href;
  return element;
}

async function data(filename) {
  const url = new URL(filename, import.meta.url);
  const response = await fetch(url);
  if (!response.ok) throw new Error(filename);
  return response.json();
}

function matches(value, query) {
  const words = (text) => text.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
  return words(query).split(/\s+/).every((word) => words(value).includes(word));
}

function filters() {
  return { query: $("query").value, protocol: $("protocol").value, cards: $("cards").checked };
}

function providerCard(provider, config) {
  const card = node("article", undefined, "card");
  card.id = provider.id;
  const tags = node("div", undefined, "tags");
  tags.append(node("span", statusLabels[provider.status], "tag"));
  if (provider.card) tags.append(node("span", "Карты РФ", "tag good"));
  for (const protocol of provider.protocols) tags.append(node("span", labels[protocol], "tag"));
  card.append(node("h2", provider.name), tags, node("p", provider.payment));
  if (provider.api) card.append(node("code", provider.api));
  const links = node("div", undefined, "sources");
  const signup = link(provider.referral ? "Регистрация · партнёрская ссылка" : "Сайт провайдера", provider.referral || provider.website);
  if (provider.referral) signup.rel = "sponsored";
  links.append(signup, ...provider.sources.map(({ label, url }) => link(label, url)));
  const section = config?.get(provider.id);
  if (section) {
    const detail = node("details");
    const example = node("pre");
    example.append(node("code", section));
    detail.append(node("summary", "Пример TOML"), example);
    card.append(detail);
  }
  card.append(links);
  return card;
}

function renderProviders(providers, config) {
  const { query, protocol, cards } = filters();
  const filtered = providers.filter((p) => (!cards || p.card) && (!protocol || p.protocols.includes(protocol))
    && matches(`${p.name} ${p.payment} ${p.api ?? ""}`, query));
  status.textContent = filtered.length ? `Показано ${filtered.length} из ${providers.length}` : "Провайдеры по этим фильтрам не найдены.";
  results.replaceChildren(...filtered.map((provider) => providerCard(provider, config)));
}

function units(value, currency) {
  const maximumFractionDigits = value > 0 && value < 0.01 ? 9 : 4;
  if (value > 0 && value < 0.000000001) return `${value.toExponential(2)} ${currency}`;
  return new Intl.NumberFormat("ru", { style: "currency", currency, maximumFractionDigits }).format(value);
}

function estimate(price, input, output) {
  return (price.input * input + price.output * output) / 1e6 + price.request;
}

function rank(offers, input, output, rate) {
  const rows = offers.map((offer) => {
    const price = offer.price;
    const value = price && estimate(price, input, output);
    const comparable = price && value * (price.currency === "USD" && rate ? rate : 1);
    return { offer, value, comparable };
  });
  const known = rows.filter((r) => r.value !== null);
  const canCompare = Boolean(rate) || new Set(known.map((r) => r.offer.price.currency)).size <= 1;
  rows.sort((a, b) => {
    if (a.value === null || b.value === null) return (a.value === null) - (b.value === null);
    if (!canCompare && a.offer.price.currency !== b.offer.price.currency) {
      return a.offer.price.currency.localeCompare(b.offer.price.currency);
    }
    return a.comparable - b.comparable;
  });
  const minimum = canCompare && known.length >= 2 ? rows[0].comparable : null;
  return { rows, minimum };
}

function modelCard(group, providers, input, output, rate) {
  const card = node("article", undefined, "group");
  const { rows, minimum } = rank(group, input, output, rate);
  const table = node("table");
  const header = table.createTHead().insertRow();
  for (const title of ["Провайдер / API ID", "Вход / 1 млн", "Выход / 1 млн", "За вызов", "Оценка запроса"]) {
    const heading = node("th", title);
    heading.scope = "col";
    header.append(heading);
  }
  const body = table.createTBody();
  for (const { offer, value, comparable } of rows) {
    const row = body.insertRow();
    const info = row.insertCell();
    const provider = node("strong", providers.get(offer.provider).name);
    const identifier = link(offer.id, offer.source);
    info.append(provider, identifier);
    const price = offer.price;
    for (const key of ["input", "output", "request"]) row.insertCell().textContent = price ? units(price[key], price.currency) : "—";
    const total = row.insertCell();
    total.textContent = price ? units(value, price.currency) : "Цена неизвестна";
    if (price?.currency === "USD" && rate) total.append(node("small", ` ≈ ${units(comparable, "RUB")}`));
    if (minimum !== null && comparable !== null && Math.abs(comparable - minimum) <= Math.abs(minimum) * 1e-12) {
      total.append(node("strong", "Минимум", "good"));
    }
  }
  const scroll = node("div", undefined, "table-wrap");
  scroll.tabIndex = 0;
  scroll.setAttribute("aria-label", `Цены модели ${group[0].key}`);
  scroll.append(table);
  card.append(node("h2", group[0].key), scroll);
  return card;
}

function modelPage(providers, snapshot) {
  const sources = $("sources");
  const offers = [];
  for (const [id, feed] of Object.entries(snapshot.feeds)) {
    const source = node("li");
    source.append(link(providers.get(id).name, feed.url), document.createTextNode(` · ${feed.date}${feed.stale ? " · сохранённый снимок" : ""}`));
    sources.append(source);
    for (const offer of feed.offers) offers.push({ ...offer, source: offer.source || feed.url });
  }
  if (snapshot.fx) {
    $("rate").value = snapshot.fx.rate;
    const source = node("li");
    source.append(link("Курс Банка России", snapshot.fx.url), document.createTextNode(` · ${snapshot.fx.date}${snapshot.fx.stale ? " · сохранённый курс" : ""}`));
    sources.append(source);
  }
  const missing = [...providers.values()].filter((p) => !snapshot.feeds[p.id]);
  if (missing.length) sources.append(node("li", `Тарифы вне каталога: ${missing.map((p) => p.name).join(", ")}.`));
  if (snapshot.feeds.proxyapi) sources.append(node("li", "Для ProxyAPI включены отдельные тарифы, сверенные по страницам моделей."));

  let limit = 24;
  function render() {
    const { query, protocol, cards } = filters();
    const input = $("input-tokens").valueAsNumber;
    const output = $("output-tokens").valueAsNumber;
    const rate = $("rate").valueAsNumber;
    if (!Number.isInteger(input) || !Number.isInteger(output) || !$("input-tokens").validity.valid ||
        !$("output-tokens").validity.valid || !$("rate").validity.valid) {
      status.textContent = "Укажите допустимое число токенов и положительный курс либо оставьте курс пустым.";
      results.replaceChildren();
      $("more").hidden = true;
      return;
    }
    const groups = new Map();
    for (const offer of offers) {
      if ((cards && !providers.get(offer.provider).card) || (protocol && !offer.protocols.includes(protocol))) continue;
      if (!groups.has(offer.key)) groups.set(offer.key, []);
      groups.get(offer.key).push(offer);
    }
    const entries = [...groups.values()]
      .filter((group) => group.some((o) => matches(`${o.key} ${o.name} ${o.id}`, query)))
      .sort((a, b) => (new Set(b.map((o) => o.provider)).size - new Set(a.map((o) => o.provider)).size)
        || a[0].key.localeCompare(b[0].key));
    const count = entries.reduce((sum, group) => sum + group.length, 0);
    status.textContent = entries.length ? `Моделей: ${entries.length} · предложений: ${count}` : "Модели по этим фильтрам не найдены.";
    results.replaceChildren(...entries.slice(0, limit).map((group) => modelCard(group, providers, input, output, rate)));
    $("more").hidden = entries.length <= limit;
  }

  document.querySelectorAll("input, select").forEach((element) => element.addEventListener("input", () => {
    limit = 24;
    render();
  }));
  $("more").addEventListener("click", () => { limit += 24; render(); });
  render();
}

async function main() {
  const providers = await data("providers.json");
  if (page === "models") {
    modelPage(new Map(providers.map((p) => [p.id, p])), await data("catalog.json"));
    return;
  }
  const response = await fetch(new URL("sift-gate.ru.example.toml", import.meta.url));
  const config = new Map();
  if (response.ok) {
    const example = await response.text();
    for (const match of example.matchAll(/^\[providers\.(\w+)\]\r?\n[^\[]+/gm)) {
      config.set(match[1], match[0].trim());
    }
  }
  const render = () => renderProviders(providers, config);
  document.querySelectorAll("input, select").forEach((element) => element.addEventListener("input", render));
  render();
}

main().catch((error) => {
  status.textContent = "Не удалось загрузить каталог. Обновите страницу и попробуйте снова.";
  console.error(error);
});
