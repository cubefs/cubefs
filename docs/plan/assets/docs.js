// cubefs docs/plan: nav active / mobile nav / code copy / scroll reveal /
// count-up / 入口页客户端搜索过滤。
// 风格端口自 modelfs/docs/assets/docs.js。

document.addEventListener("DOMContentLoaded", () => {
  initNavActive();
  initMobileNav();
  initCodeCopy();
  initReveal();
  initCountUp();
  initDocSearch();
});

const prefersReducedMotion = () =>
  window.matchMedia("(prefers-reduced-motion: reduce)").matches;

// 根据 body[data-section] 给对应导航项加 .is-active
function initNavActive() {
  const section = document.body.dataset.section;
  if (!section) return;
  document.querySelectorAll("[data-nav]").forEach((link) => {
    if (link.dataset.nav === section) {
      link.classList.add("is-active");
    }
  });
}

// 移动端汉堡:点击 .nav-toggle 切换 .site-header.is-open
function initMobileNav() {
  const header = document.querySelector(".site-header");
  const toggle = document.querySelector(".nav-toggle");
  if (!header || !toggle) return;

  toggle.addEventListener("click", () => {
    const isOpen = header.classList.toggle("is-open");
    toggle.setAttribute("aria-expanded", String(isOpen));
  });

  header.querySelectorAll(".nav-link, .nav-cta").forEach((link) => {
    link.addEventListener("click", () => {
      header.classList.remove("is-open");
      toggle.setAttribute("aria-expanded", "false");
    });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && header.classList.contains("is-open")) {
      header.classList.remove("is-open");
      toggle.setAttribute("aria-expanded", "false");
    }
  });
}

// 给所有 <pre><code> 注入复制按钮
function initCodeCopy() {
  document.querySelectorAll("pre > code").forEach((code) => {
    const pre = code.parentElement;
    if (!pre || pre.querySelector(".code-copy")) return;

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "code-copy";
    btn.setAttribute("aria-label", "复制代码");
    btn.textContent = "复制";

    btn.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(code.innerText);
        btn.textContent = "已复制";
        btn.classList.add("is-copied");
      } catch {
        btn.textContent = "复制失败";
      }
      setTimeout(() => {
        btn.textContent = "复制";
        btn.classList.remove("is-copied");
      }, 1600);
    });

    pre.appendChild(btn);
  });
}

// 滚动渐显:IntersectionObserver 给 .reveal 加 .is-in
function initReveal() {
  const items = document.querySelectorAll(".reveal");
  if (!items.length) return;

  if (prefersReducedMotion() || !("IntersectionObserver" in window)) {
    items.forEach((el) => el.classList.add("is-in"));
    return;
  }

  const io = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("is-in");
          io.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.12, rootMargin: "0px 0px -8% 0px" }
  );

  items.forEach((el) => io.observe(el));
}

// stats 数字 count-up:见 [data-count-to] 触底就动画
function initCountUp() {
  const targets = document.querySelectorAll("[data-count-to]");
  if (!targets.length) return;

  if (prefersReducedMotion() || !("IntersectionObserver" in window)) {
    targets.forEach((el) => {
      const decimals = parseInt(el.dataset.countDecimals || "0", 10);
      const suffix = el.dataset.countSuffix || "";
      const to = parseFloat(el.dataset.countTo);
      el.textContent = to.toFixed(decimals) + suffix;
    });
    return;
  }

  const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3);

  const io = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        const el = entry.target;
        const to = parseFloat(el.dataset.countTo);
        const decimals = parseInt(el.dataset.countDecimals || "0", 10);
        const suffix = el.dataset.countSuffix || "";
        const duration = 1400;
        const start = performance.now();

        const tick = (now) => {
          const p = Math.min(1, (now - start) / duration);
          const val = to * easeOutCubic(p);
          el.textContent = val.toFixed(decimals) + suffix;
          if (p < 1) requestAnimationFrame(tick);
          else el.textContent = to.toFixed(decimals) + suffix;
        };

        requestAnimationFrame(tick);
        io.unobserve(el);
      });
    },
    { threshold: 0.5 }
  );

  targets.forEach((el) => io.observe(el));
}

// 入口页客户端搜索:[data-doc-search] 输入 → 过滤 [data-search-item]，
// 组容器 [data-group-panel] 若组内全部隐藏也整组隐藏。
function initDocSearch() {
  const inputs = Array.from(document.querySelectorAll("[data-doc-search]"));
  if (!inputs.length) return;

  const groups = Array.from(document.querySelectorAll("[data-group-panel]"));
  const items = Array.from(document.querySelectorAll("[data-search-item]"));
  const emptyHint = document.querySelector("[data-search-empty]");

  const applyFilter = (raw) => {
    const q = raw.trim().toLowerCase();

    items.forEach((item) => {
      const hay = (item.getAttribute("data-search") || "").toLowerCase();
      const visible = !q || hay.includes(q);
      item.classList.toggle("is-hidden", !visible);
    });

    let totalVisible = 0;
    groups.forEach((group) => {
      const shown = group.querySelectorAll("[data-search-item]:not(.is-hidden)");
      const hide = shown.length === 0;
      group.classList.toggle("is-hidden", hide);
      totalVisible += shown.length;
    });

    if (emptyHint) {
      emptyHint.classList.toggle("is-visible", q !== "" && totalVisible === 0);
    }
  };

  inputs.forEach((input) => {
    input.addEventListener("input", () => {
      const value = input.value;
      inputs.forEach((other) => {
        if (other !== input) other.value = value;
      });
      applyFilter(value);
    });
  });
}
