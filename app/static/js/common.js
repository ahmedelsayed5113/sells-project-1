/* ──────────────────────────────────────────────────────────────────
   Shared browser helpers — API, toast, formatting, modals.
   `api()` now auto-attaches a CSRF header and normalises errors so
   callers can pass the thrown error directly to `tError()` (which
   comes from i18n.js) to get a localised, code-driven message.
   ────────────────────────────────────────────────────────────────── */
const API = window.location.origin;

// ─── CSRF token (fetched lazily once; refreshed on 403) ─────────────
let _CSRF = null;
async function _fetchCsrf() {
  try {
    const r = await fetch(API + "/api/auth/me", { credentials: "same-origin" });
    if (!r.ok) return null;
    const d = await r.json();
    _CSRF = d.csrf || null;
    return _CSRF;
  } catch { return null; }
}

// ─── Toast ─────────────────────────────────────────────────────────
function toast(message, type = "") {
  let el = document.getElementById("__toast");
  if (!el) {
    el = document.createElement("div");
    el.id = "__toast";
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = message || t("common.error");
  el.className = "toast show " + type;
  clearTimeout(window.__toastTimer);
  window.__toastTimer = setTimeout(() => {
    el.classList.remove("show");
  }, 3500);
}

function toastError(err) { toast(tError(err), "error"); }

// ─── API wrapper ───────────────────────────────────────────────────
async function api(path, options = {}) {
  const method = (options.method || "GET").toUpperCase();
  const needsCsrf = method !== "GET" && method !== "HEAD";

  const headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    ...(options.headers || {}),
  };

  if (needsCsrf) {
    if (!_CSRF) await _fetchCsrf();
    if (_CSRF) headers["X-CSRF-Token"] = _CSRF;
  }

  const opts = {
    credentials: "same-origin",
    ...options,
    headers,
    method,
  };

  if (opts.body && typeof opts.body !== "string") {
    opts.body = JSON.stringify(opts.body);
  }

  let r;
  try {
    r = await fetch(API + path, opts);
  } catch (netErr) {
    const err = new Error(t("errors.server"));
    err.status = 0;
    err.netError = true;
    throw err;
  }

  let data = null;
  try { data = await r.json(); } catch { data = null; }

  if (!r.ok) {
    const code = data && data.error_code;
    const msg = (code && t("errors." + code)) || (data && data.error) || t("errors.server");
    const err = new Error(msg);
    err.status = r.status;
    err.data = data;
    err.error_code = code || null;
    if (r.status === 401 && path !== "/api/auth/me" && path !== "/api/auth/login") {
      // Session expired — bounce to login
      setTimeout(() => { window.location.href = "/login"; }, 50);
    }
    if (r.status === 403 && code === "forbidden" && needsCsrf) {
      _CSRF = null; // Force refetch on next call
    }
    throw err;
  }
  return data;
}

async function logout() {
  try { await api("/api/auth/logout", { method: "POST" }); } catch (_) {}
  window.location.href = "/login";
}

// ─── Dropdown / lang toggle ────────────────────────────────────────
function initUserDropdown() {
  const trigger = document.querySelector(".topnav-user");
  if (!trigger) return;
  const dropdown = trigger.querySelector(".dropdown");
  if (!dropdown) return;
  trigger.addEventListener("click", (e) => {
    e.stopPropagation();
    dropdown.classList.toggle("open");
  });
  document.addEventListener("click", () => dropdown.classList.remove("open"));
}

// Track the element that had focus before the drawer opened, so we can
// return focus on close (a11y requirement for modal dialogs).
let _sidebarReturnFocus = null;

function _isMobileViewport() { return window.innerWidth <= 1024; }

function toggleSidebar(force) {
  const sb = document.getElementById("sidebar");
  const backdrop = document.getElementById("sidebarBackdrop");
  const burger = document.getElementById("navBurger");
  if (!sb) return;
  const willOpen = force === undefined ? !sb.classList.contains("open") : !!force;
  sb.classList.toggle("open", willOpen);
  if (backdrop) backdrop.classList.toggle("open", willOpen);
  if (burger) {
    burger.classList.toggle("open", willOpen);
    burger.setAttribute("aria-expanded", willOpen ? "true" : "false");
  }
  document.body.style.overflow = willOpen ? "hidden" : "";

  // Modal dialog behavior only applies when the sidebar acts as a drawer (mobile).
  if (_isMobileViewport()) {
    if (willOpen) {
      _sidebarReturnFocus = document.activeElement;
      // Move focus to the first focusable element inside the drawer.
      const first = sb.querySelector("a, button, [tabindex]:not([tabindex='-1'])");
      if (first) first.focus();
    } else if (_sidebarReturnFocus && typeof _sidebarReturnFocus.focus === "function") {
      _sidebarReturnFocus.focus();
      _sidebarReturnFocus = null;
    }
  }
}
window.toggleSidebar = toggleSidebar;

// Trap Tab inside the drawer while it's open on mobile, and close on Escape.
function _onSidebarKeydown(e) {
  const sb = document.getElementById("sidebar");
  if (!sb || !sb.classList.contains("open") || !_isMobileViewport()) return;

  if (e.key === "Escape") {
    e.preventDefault();
    toggleSidebar(false);
    return;
  }

  if (e.key !== "Tab") return;
  const focusable = sb.querySelectorAll(
    "a[href], button:not([disabled]), [tabindex]:not([tabindex='-1'])"
  );
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (e.shiftKey && document.activeElement === first) {
    e.preventDefault();
    last.focus();
  } else if (!e.shiftKey && document.activeElement === last) {
    e.preventDefault();
    first.focus();
  }
}

function initBurger() {
  const btn = document.getElementById("navBurger");
  if (!btn) return;
  btn.addEventListener("click", (e) => {
    e.stopPropagation();
    toggleSidebar();
  });
  // Auto-close when any sidebar link is tapped
  const sb = document.getElementById("sidebar");
  if (sb) sb.querySelectorAll("a, button").forEach(el => el.addEventListener("click", () => {
    if (window.innerWidth <= 1024) toggleSidebar(false);
  }));
  // Reset state when bumping back up to desktop layout
  window.addEventListener("resize", () => {
    if (window.innerWidth > 1024) toggleSidebar(false);
  });
}

function _refreshLangToggleLabel() {
  document.querySelectorAll("#langToggleBtn, .lang-toggle").forEach(btn => {
    // Show the *target* language: in AR mode → "EN", in EN mode → "AR".
    btn.textContent = getLang() === "ar" ? "EN" : "AR";
  });
}

function initLangToggle() {
  _refreshLangToggleLabel();
  document.querySelectorAll("#langToggleBtn, .lang-toggle").forEach(btn => {
    btn.addEventListener("click", () => {
      btn.classList.remove("spin-once"); void btn.offsetWidth;
      btn.classList.add("spin-once");
      const current = getLang();
      setLang(current === "ar" ? "en" : "ar");
      _refreshLangToggleLabel();
      if (typeof onLangChange === "function") onLangChange(getLang());
    });
  });
}

// ─── Formatting ────────────────────────────────────────────────────
function ratingClass(rating) {
  return {
    "Excellent": "badge-excellent", "V.Good": "badge-vgood", "Good": "badge-good",
    "Medium": "badge-medium", "Weak": "badge-weak", "Bad": "badge-bad",
    "Pending": "badge-pending"
  }[rating] || "badge-pending";
}

function ratingLabel(rating) { return t("rating." + rating); }

function scoreColor(pct) {
  if (pct >= 75) return "success";
  if (pct >= 55) return "warn";
  return "danger";
}

function fmtMonth(monthStr) {
  if (!monthStr) return "—";
  const [y, m] = monthStr.split("-");
  const lang = getLang();
  const namesAr = ["يناير","فبراير","مارس","أبريل","مايو","يونيو","يوليو","أغسطس","سبتمبر","أكتوبر","نوفمبر","ديسمبر"];
  const namesEn = ["January","February","March","April","May","June","July","August","September","October","November","December"];
  const names = lang === "en" ? namesEn : namesAr;
  return names[parseInt(m) - 1] + " " + y;
}

function currentMonth() {
  const d = new Date();
  return d.getFullYear() + "-" + (d.getMonth() + 1).toString().padStart(2, "0");
}

function fmtNum(n, decimals = 0) {
  if (n == null || isNaN(n)) return "—";
  const locale = getLang() === "ar" ? "ar-EG" : "en-US";
  return Number(n).toLocaleString(locale, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function fmtMoney(n) {
  if (n == null || isNaN(n)) return "—";
  const locale = getLang() === "ar" ? "ar-EG" : "en-US";
  return Number(n).toLocaleString(locale, { maximumFractionDigits: 0 });
}

function openModal(id) {
  const m = document.getElementById(id);
  if (m) m.classList.add("open");
}
function closeModal(id) {
  const m = document.getElementById(id);
  if (m) m.classList.remove("open");
}

// ─── Password visibility toggles ───────────────────────────────────
function initPasswordToggles() {
  document.querySelectorAll(".pw-toggle").forEach(btn => {
    btn.addEventListener("click", () => {
      const id = btn.getAttribute("data-target");
      const input = id ? document.getElementById(id) : null;
      if (!input) return;
      const showing = input.getAttribute("type") === "text";
      input.setAttribute("type", showing ? "password" : "text");
      const icon = btn.querySelector(".material-symbols-outlined");
      if (icon) icon.textContent = showing ? "visibility" : "visibility_off";
      btn.setAttribute("aria-label", t(showing ? "common.show_password" : "common.hide_password"));
    });
  });
}

// ─── Reveal-on-scroll for .reveal elements ─────────────────────────
function initReveal() {
  if (!("IntersectionObserver" in window)) return;
  const io = new IntersectionObserver((entries) => {
    entries.forEach(en => {
      if (en.isIntersecting) {
        en.target.classList.add("revealed");
        io.unobserve(en.target);
      }
    });
  }, { threshold: 0.12 });
  document.querySelectorAll(".reveal").forEach(el => io.observe(el));
}

document.addEventListener("DOMContentLoaded", () => {
  initUserDropdown();
  initBurger();
  initLangToggle();
  initReveal();
  initPasswordToggles();
  document.addEventListener("keydown", _onSidebarKeydown);
  document.querySelectorAll(".modal-backdrop").forEach(m => {
    m.addEventListener("click", (e) => {
      if (e.target === m) m.classList.remove("open");
    });
  });
});
