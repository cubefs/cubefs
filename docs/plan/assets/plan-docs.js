(() => {
  const body = document.body;
  const navToggle = document.querySelector("[data-nav-toggle]");
  const sidebar = document.querySelector("[data-site-sidebar]");

  if (navToggle && sidebar) {
    navToggle.addEventListener("click", () => {
      body.classList.toggle("nav-open");
    });

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      if (body.classList.contains("nav-open") && !sidebar.contains(target) && !navToggle.contains(target)) {
        body.classList.remove("nav-open");
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        body.classList.remove("nav-open");
      }
    });
  }

  const searchInputs = Array.from(document.querySelectorAll("[data-doc-search]"));
  if (!searchInputs.length) {
    return;
  }

  const groups = Array.from(document.querySelectorAll("[data-group-panel]"));
  const items = Array.from(document.querySelectorAll("[data-search-item]"));

  const applyFilter = (query) => {
    const normalized = query.trim().toLowerCase();

    items.forEach((item) => {
      const haystack = (item.getAttribute("data-search") || "").toLowerCase();
      const visible = !normalized || haystack.includes(normalized);
      item.classList.toggle("is-hidden", !visible);
    });

    groups.forEach((group) => {
      const visibleChildren = group.querySelectorAll("[data-search-item]:not(.is-hidden)");
      group.classList.toggle("is-hidden", visibleChildren.length === 0);
    });
  };

  searchInputs.forEach((input) => {
    input.addEventListener("input", () => {
      const value = input.value;
      searchInputs.forEach((other) => {
        if (other !== input) {
          other.value = value;
        }
      });
      applyFilter(value);
    });
  });
})();
